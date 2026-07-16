# Fenced, Progress-Preserving Restarts (alpha)

Feature gate: `FencedRestart` (alpha, default off). Design:
[proposals/fenced-progress-preserving-restart.md](../proposals/fenced-progress-preserving-restart.md).

When a Spark driver dies, `restartPolicy: OnFailure` restarts the whole
application from zero, and nothing prevents a zombie of the old driver — on
a partitioned node the kubelet can no longer report on — from still
committing output. With `FencedRestart` enabled and
`spec.restartPolicy.recovery` set, every operator-driven rerun is:

- **Fenced:** before resubmission the operator atomically advances a
  monotonic *epoch* in an external state store (Redis). Writes guarded by
  an older epoch are rejected atomically, so the previous driver can never
  commit again — even if it is still running.
- **Progress-preserving:** an injected `spark-recovery-agent` sidecar lets
  the application commit small progress markers (last committed offset,
  completed stage, checkpoint path) and hands the latest marker to the
  restarted driver, which skips completed work.

**What this is not:** Spark has no driver checkpoint/restore. Executors,
shuffle data, and scheduler state are lost on driver death regardless. This
feature delivers *restart minus redone work plus a fencing guarantee* —
never mid-stage resume. If your application never commits a marker, you get
a fenced restart-from-zero (a recurring warning event tells you so).

## You don't need this if…

- your jobs are short or cheap to re-run — plain `OnFailure` is the right
  tool;
- driver deaths are dominated by OOM or application bugs — the rerun fails
  the same way;
- you run Structured Streaming with a transactional sink — checkpoint
  restart already covers you;
- you can't add progress reporting to the application.

The sweet spot: long, expensive batch and iterative-ML jobs with natural
commit points on unreliable (spot/preemptible) capacity.

## Setup

1. Run a Redis reachable from the operator and driver pods (AOF persistence
   recommended; the fencing epoch must survive restarts).
2. Start the controller and webhook with the gate and a store profile, and
   the webhook additionally with the agent image (typically the operator
   image itself — the agent is the `spark-operator agent` subcommand):

```
--feature-gates=FencedRestart=true
--recovery-store-profiles=default=redis:redis.spark-operator.svc:6379
--recovery-agent-image=<operator image>       # webhook only
```

Passwords are read from `RECOVERY_STORE_<NAME>_PASSWORD` environment
variables on both deployments; credentials never appear in application
manifests.

## Usage

```yaml
spec:
  restartPolicy:
    type: OnFailure
    onFailureRetries: 3
    recovery:
      storeProfile: default
      heartbeatInterval: 10s   # agent liveness beat
      heartbeatTTL: 30s        # unchanged-for-this-long => driver suspect
```

The webhook injects the agent sidecar into the driver pod. The driver
receives:

| Environment variable | Meaning |
|---|---|
| `SPARK_RECOVERY_ENABLED` | `"true"` when the feature applies |
| `SPARK_RECOVERY_EPOCH` | epoch this run is bound to |
| `SPARK_RECOVERY_RESTORE_EPOCH` | epoch of the marker to resume from (reruns only) |
| `SPARK_RECOVERY_AGENT_URL` | `http://localhost:7691` |

Application contract (any language, plain HTTP):

- **Commit progress:** `PUT $SPARK_RECOVERY_AGENT_URL/v1/snapshot` with an
  opaque body (≤ 1 MiB by default) at every commit point. `409 Conflict`
  means this run was fenced — stop committing.
- **Resume:** `GET $SPARK_RECOVERY_AGENT_URL/v1/snapshot` at startup (or
  read `/var/run/spark-recovery/snapshot`); `404` means a fresh start.

Markers are *markers*, not data: store references (paths, offsets, stage
numbers), never datasets.

See [examples/spark-fenced-restart.yaml](../examples/spark-fenced-restart.yaml)
for a complete resumable job, and for an end-to-end demo with evidence
collection:
[hack/fenced-restart-demo/run-demo-kind.sh](../hack/fenced-restart-demo/run-demo-kind.sh)
(kind) or
[hack/fenced-restart-demo/run-demo.sh](../hack/fenced-restart-demo/run-demo.sh)
(minikube).

## Status and events

`status.recoveryStatus` reports `epoch` and `restoredFromEpoch`. Events:
`SparkApplicationEpochAdvanced`, `SparkApplicationMarkerRestored`, and the
warning `SparkApplicationNoMarkerObserved` when a recovery-enabled
application restarts having never committed a marker.
