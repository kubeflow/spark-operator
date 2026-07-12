# KEP: Fenced, Progress-Preserving Driver Restarts

- **Authors:** Ganga (gangavh@gmail.com)
- **Status:** Provisional (pre-review draft)
- **Creation date:** 2026-07-11

## Summary

Add an opt-in extension to `SparkApplication.spec.restartPolicy` that makes
operator-driven restarts **fenced** and **progress-preserving**:

1. **Fenced:** before each re-submission, the operator atomically advances a
   monotonic *epoch* in an external state store (Redis or any S3-compatible
   object store). Writes guarded by an older epoch are atomically rejected,
   so a zombie of the previous driver — alive on a partitioned node the
   kubelet can no longer report on — can never commit state again. Today,
   `restartPolicy: OnFailure/Always` restarts an application with no
   protection against the previous driver still running.
2. **Progress-preserving:** an operator-injected sidecar in the driver pod
   accepts small progress markers ("snapshots") from the application,
   commits them to the store under the current epoch, and restores the
   latest marker into the restarted driver — so a well-structured job
   resumes from its last commit point instead of from zero.

The feature reuses machinery this operator already has: the driver pod
mutating webhook (`internal/webhook/sparkpod_defaulter.go`) injects the
sidecar; the existing restart flow
(`FAILING → PENDING_RERUN → SUBMITTED`) gains one step (epoch advance);
re-submission stays the native `spark-submit` path. No new CRD, no second
controller, no external operator.

### What this is not (honest framing)

Spark has no driver checkpoint/restore. When a driver dies, executors,
shuffle data, cached blocks, and scheduler state are gone regardless of
what any operator does. This feature delivers **restart, minus redone work,
plus a guarantee that the old driver cannot double-commit** — never
"resume mid-stage." All user-facing documentation for this feature must
preserve that distinction; wording that implies live-state recovery is a
documentation bug.

## Motivation

The driver is the SPoF of Spark on Kubernetes. For long batch and
iterative-ML jobs — especially on spot/preemptible capacity — a driver
death costs hours of recomputation, and today's restart policy has two
gaps:

- **Correctness:** after a node partition, `OnFailure` can produce two
  live drivers racing on non-transactional sinks. Nothing in the ecosystem
  fences the old one at submission time. (Transactional sinks like
  Delta/Iceberg protect their own commits; plain-file, JDBC, and custom
  sinks are exposed.)
- **Economics:** restart-from-zero re-runs work the job had already
  durably completed, even when the application records its own progress —
  because the operator gives it no standard place to record it or read it
  back.

YARN precedent: AM re-attempts (`spark.yarn.maxAppAttempts`) put
restart-with-attempt-tracking in the resource-manager layer. On
Kubernetes, this operator *is* that layer.

This capability belongs inside the operator rather than in any external
add-on, for structural reasons: the operator owns the restart state
machine, so fencing composes with it instead of racing it; recovery rides
the native `spark-submit` path, so nothing ever clones or replays driver
pod specs; the `SparkApplication` status stays truthful throughout
recovery, so downstream systems (Airflow/Dagster sensors, dashboards,
`kubectl get sparkapplications`) never observe a false `FAILED`; and the
existing pod webhook and cert infrastructure are reused instead of
duplicated. An out-of-tree implementation would have to fight the operator
on all four fronts.

### Goals

- Fencing: at most one epoch's writes can ever commit; recovery is safe
  even when the old driver cannot be confirmed dead.
- Progress restore via the native re-submission path, with attempt
  tracking integrated into existing `OnFailureRetries` semantics.
- Opt-in via the `SparkApplication` spec; zero behavior change for
  applications that don't set it. Alpha behind a feature gate.
- Pluggable state store (`redis`, `s3`) behind one Go interface; store
  endpoints/credentials configured at operator level (profiles), selected
  per application.
- Truthful status: the application object reflects fenced recovery
  natively (`PENDING_RERUN` with recovery detail in status), never a
  false `FAILED`.

### Non-Goals

- Mid-stage resume, driver memory/process checkpointing (CRIU) — see
  Alternatives.
- Executor recovery (Spark handles it) or snapshotting Spark-internal
  state.
- Generic non-Spark workload recovery.
- Replacing sink-side exactly-once mechanisms; fencing complements them.

### When not to use it

To be included in user docs and maintained as a release checklist item:
short or cheap-to-rerun jobs (plain `OnFailure` is the right tool);
OOM- or bug-dominated crash profiles (the rerun fails the same way —
this feature adds retries, not fixes); Structured Streaming with
transactional sinks (checkpoint restart already covers it — though
fencing still adds value on non-transactional sinks); applications whose
authors will not integrate progress reporting (they get fenced
restarts-from-zero, which the status surfaces honestly). The intended
sweet spot, stated positively: long-running, expensive batch and
iterative-ML jobs with natural commit points, on unreliable capacity,
operated by platform teams that amortize the cost across a fleet.

## Proposal

### API changes (v1beta2, all optional — no breaking changes)

```go
// RestartPolicy gains one optional field.
type RestartPolicy struct {
	Type RestartPolicyType `json:"type,omitempty"`
	// ... existing retry fields unchanged ...

	// Recovery, if set, makes restarts fenced and progress-preserving.
	// Only valid with Type=OnFailure or Type=Always.
	// Requires the FencedRestart feature gate.
	// +optional
	Recovery *RecoveryPolicy `json:"recovery,omitempty"`
}

type RecoveryPolicy struct {
	// StoreProfile names a state-store profile from operator config
	// (endpoint + credentials secret, type redis|s3). Required.
	StoreProfile string `json:"storeProfile"`

	// HeartbeatInterval for the injected sidecar (default 10s) and
	// HeartbeatTTL: how long the heartbeat may remain unchanged, judged
	// on the controller's clock, before the driver is Suspect
	// (default 30s). Enables partition detection faster than, and
	// independent of, kubelet reporting.
	// +optional
	HeartbeatInterval *metav1.Duration `json:"heartbeatInterval,omitempty"`
	// +optional
	HeartbeatTTL *metav1.Duration `json:"heartbeatTTL,omitempty"`

	// MaxSnapshotAge, if set, surfaces a Stalled condition when no
	// snapshot has been committed for this long. Detection-only.
	// +optional
	MaxSnapshotAge *metav1.Duration `json:"maxSnapshotAge,omitempty"`

	// SnapshotMaxSizeBytes caps marker size (default 1 MiB).
	// +optional
	SnapshotMaxSizeBytes *int64 `json:"snapshotMaxSizeBytes,omitempty"`
}
```

Status additions:

```go
type SparkApplicationStatus struct {
	// ... existing fields unchanged ...

	// RecoveryStatus is populated only when restartPolicy.recovery is set.
	// +optional
	RecoveryStatus *RecoveryStatus `json:"recoveryStatus,omitempty"`
}

type RecoveryStatus struct {
	Epoch             int64        `json:"epoch"`
	SnapshotTier      string       `json:"snapshotTier,omitempty"` // none|observed|auto-streaming|integrated
	LastSnapshotTime  *metav1.Time `json:"lastSnapshotTime,omitempty"`
	LastHeartbeatTime *metav1.Time `json:"lastHeartbeatTime,omitempty"`
	FencedWrites      int64        `json:"fencedWrites"` // stale writes rejected; >0 proves fencing fired
}
```

Because opt-in is a **spec field on the object this operator owns**, no
side-channel activation contract (labels, annotations, companion CRDs) is
needed, and validation happens in the existing
`sparkapplication_validator.go` webhook (e.g., rejecting `recovery` with
`Type: Never`, or an unknown store profile).

### Controller changes

The existing state machine is touched in exactly two places:

1. **`FAILING → PENDING_RERUN` (the fence).** When
   `restartPolicy.recovery` is set, the transition performs
   `AdvanceEpoch()` against the store *before* the rerun is scheduled, and
   records the new epoch in `status.recoveryStatus.epoch`. Idempotent: if
   the controller crashes between the advance and the status write, the
   next reconcile observes `store epoch > status epoch` with a terminated
   driver and treats fencing as done (a doubled advance only invalidates
   epochs no live driver holds). Ordering is the correctness core:
   **fence → (native) delete → (native) resubmit**. Pod deletion remains a
   cleanup step, not a correctness step — safety never depends on the old
   driver actually being gone, only on it being fenced, which is what
   makes recovery safe on partitioned nodes.
2. **Suspect detection (new, cheap).** For recovery-enabled apps the
   reconciler additionally reads the store heartbeat on its existing
   resync: if the heartbeat `Seq` hasn't advanced for `heartbeatTTL`
   (controller-clock; no cross-machine time comparison) while the pod
   still reports Running, the app is Suspect — after a grace of two
   resyncs, treated as failed. This catches partitioned nodes faster than
   pod-status alone; fencing makes acting on it safe even if the driver
   is actually alive.

Re-submission is untouched `spark-submit`. The only additions are driver
environment variables injected through the existing submission config:
`SPARK_RECOVERY_EPOCH`, `SPARK_RECOVERY_RESTORE=<latest snapshot epoch>`,
store coordinates. The restarted driver is a first-class,
operator-managed driver in every respect — UI service, event-log wiring,
status tracking, and cleanup all behave exactly as for any other run.

`OnFailureRetries` / `OnFailureRetryInterval` keep their exact semantics
and now also bound fenced recoveries — one attempt-tracking system, not
two.

### Webhook changes

`sparkpod_defaulter.go` (already a mutating webhook on Spark pods) gains
one defaulting step: when the owning application has
`restartPolicy.recovery` set and the pod is the driver, inject (a) the
`spark-recovery-agent` sidecar (distroless Go image, heartbeat loop +
localhost:7691 snapshot API + restore-at-boot), (b) a shared `emptyDir`
for the restored marker, (c) optionally `JAVA_TOOL_OPTIONS` for the Tier-1
Java agent (below). Idempotent; no new webhook deployment, no new
cert path, no admission scope expansion beyond pods already intercepted.

### Snapshot tiers

Where marker content comes from, in increasing order of application
involvement. A constraint worth stating first: the sidecar cannot
usefully capture the driver's memory — containers share no address space,
and process-image capture is rejected in Alternatives. Restorable
progress exists only at the level of application semantics, so the design
captures those semantics with as little application effort as each
workload type permits:

- **Tier 0 — observed:** the sidecar scrapes the driver's REST API
  (`localhost:4040/api/v1`) for completed jobs/stages. Diagnostics only;
  never drives recovery decisions (a fresh application cannot skip
  stages whose shuffle data died with the executors).
- **Tier 1 — auto (zero code):** an injected `-javaagent` registers
  `SparkListener`/`StreamingQueryListener` in-JVM and auto-commits
  actionable markers where Spark semantics genuinely permit resumption:
  Structured Streaming checkpoint location + last batch ID, and
  declaratively described checkpoint artifacts (e.g., a path glob for ML
  model checkpoints). Explicitly refuses to fabricate resumability for
  arbitrary batch DAGs — listener events reveal what finished, not what
  is safe to skip.
- **Tier 2 — integrated:** the application POSTs its own markers to
  `localhost:7691` (a thin Scala/Java client published from this repo).
  The only tier that makes arbitrary batch pipelines resumable.

`status.recoveryStatus.snapshotTier` and a `SnapshotsObserved` condition
keep this honest: an application that opted in but never produced an
actionable marker shows `tier: none` with a recurring warning event.
A fenced restart-from-zero must never masquerade as progress
preservation — protection the user believes in but never receives is
worse than no feature at all.

### State store

`pkg/statestore` with a small interface (`AdvanceEpoch`, epoch-guarded
`PutSnapshot`/`Heartbeat`, reads, prune, ping) and two implementations:

- **Redis:** `INCR` for epochs; Lua scripts make check-epoch-then-write
  atomic (closing the read-epoch-then-write TOCTOU hole). Fast, but
  replication is async — a failover can lose the last epoch increment,
  which is precisely the write that must never be lost. Documented as
  the *latency* option with required topology (AOF + Sentinel +
  `min-replicas-to-write 1`).
- **S3-compatible:** all fenced mutations are ETag compare-and-swap
  (`If-Match`) on one control object per application; snapshot blobs go
  to per-epoch keys and are *committed* by CAS-ing the pointer — closing
  the late-write race where a stale driver's in-flight PUT lands after
  fencing. Recommended as the *correctness-first* option (eleven-nines
  durability; failure modes are latency/availability, which delay but
  never corrupt). Conditional-write support is probed at startup; absence
  is a hard failure, never a silent unfenced downgrade.

Heartbeats are monotonic sequence numbers judged by the reader — no store
TTLs, no cross-machine clocks; skew cannot affect correctness, only
detection latency. A conformance test suite runs both backends
(miniredis / MinIO) against the same contract, including concurrent
fencing races.

Store *profiles* (name → type + endpoint + credentials secret ref) live in
operator configuration, keeping infrastructure coordinates out of
application specs and out of job authors' hands.

## Design details

**Feature gate:** `FencedRestart`, alpha (default off) → beta (default on,
field still opt-in) → GA. Gate off ⇒ `recovery` field rejected by the
validating webhook with a clear message.

**Observability:** metrics
`spark_app_recovery_epoch`, `spark_app_recoveries_total{outcome}`,
`spark_app_recovery_duration_seconds`, `spark_app_fenced_writes_total`,
`spark_app_snapshot_age_seconds`, `statestore_up`; events
`EpochAdvanced`, `RecoverySucceeded`, `RecoveryExhausted`,
`NoSnapshotObserved`, `JobStalled`, `StateStoreUnreachable`. Store outage
freezes fencing decisions (never fence on stale data) and keeps healthy
drivers alive — the sidecar retries with jittered backoff and never kills
the driver over store unavailability.

**Security:** sidecar and agent run non-root/read-only/no-caps; store
credentials only via secret refs resolved by the operator, mounted to the
sidecar, never visible in the application spec; the sidecar's localhost
API is unauthenticated by design (any container in the driver pod can
write markers for its own epoch) — stated in SECURITY.md, mitigated by
the epoch guard which caps the blast radius at self-sabotage of one
epoch's marker.

**Scale:** per-app steady cost is one heartbeat read per resync; admission
cost rides the existing pod webhook. S3 heartbeat PUT economics (a 10s
interval ≈ 260k PUTs/month/app) are documented; recommended S3 intervals
are ≥ 30s, Redis for sub-minute detection.

**Test plan:** unit (state-machine deltas, Lua/CAS semantics, defaulter
injection); envtest (fence idempotency under crash injection between
fence/status-write/rerun); e2e on kind per PR (both stores: kill / OOM /
node-drain a driver, assert rerun resumes from marker with epoch+1, assert
`recovery`-less apps behave byte-identically); nightly chaos with a
toxiproxy partition asserting `fenced_writes_total > 0` and no
double-commit.

## Alternatives

- **An external companion operator.** Implementable without touching this
  project, but structurally worse on four fronts: it must clone and
  re-create driver pods outside `spark-submit` (a permanent compatibility
  treadmill against this operator's releases), the `SparkApplication`
  status reads `FAILED` while an out-of-band recovery pod runs (breaking
  every downstream sensor), it needs its own webhook + cert deployment,
  and it duplicates restart/attempt tracking the operator already owns.
- **Transparent driver checkpointing (CRIU / memory capture).** No shared
  address space between containers; restored JVMs hold dead sockets,
  stale IPs, and scheduler state for dead executors; JVM checkpointing
  (CRaC) reintroduces application cooperation anyway; and a memory image
  is saturated with the old epoch, defeating fencing. Semantic markers
  are the only restorable currency.
- **Kubernetes Lease-based fencing.** Leases provide liveness, not
  guarded *data* writes; snapshots need a store regardless, and tokens
  must be validated where data lands.
- **Fencing via sink-side transactions only.** Right answer where
  available (Delta/Iceberg/Kafka-txn); does nothing for plain-file, JDBC,
  and custom sinks, and cannot prevent two live drivers racing — which is
  a submission-layer problem this operator is uniquely placed to solve.

## Graduation criteria

Alpha: gate off, both stores, e2e green, docs including "when not to
use." Beta: gate on by default, conformance suite published for
third-party stores, scale test at 1,000 recovery-enabled apps. GA: two
releases at beta with no correctness bugs, `v1beta2` field semantics
frozen.

## Implementation history

- 2026-07-11: Provisional draft.
