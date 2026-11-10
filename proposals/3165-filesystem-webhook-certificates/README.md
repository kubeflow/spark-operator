# KEP-3165: Filesystem-sourced TLS certificates for the webhook

**Authors:**
- Shubham Mishra - [@shubhM13](https://github.com/shubhM13)

**Tracking Issue:** [kubeflow/spark-operator#3165](https://github.com/kubeflow/spark-operator/issues/3165)

**Status:** Provisional

---

## Table of Contents

- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [User Stories](#user-stories)
  - [Ownership Model](#ownership-model)
  - [Risks and Mitigations](#risks-and-mitigations)
  - [Open Questions](#open-questions)
- [Design Details](#design-details)
  - [Iteration Plan](#iteration-plan)
  - [Configuration](#configuration)
  - [Helm and Kustomize](#helm-and-kustomize)
  - [Startup and Serving-Certificate Reload](#startup-and-serving-certificate-reload)
  - [CA Bundle Reconciliation](#ca-bundle-reconciliation)
  - [Rotation and Migration](#rotation-and-migration)
  - [Readiness](#readiness)
  - [RBAC](#rbac)
  - [Observability](#observability)
  - [Test Plan](#test-plan)
  - [Graduation Criteria](#graduation-criteria)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)

## Summary

The Spark Operator webhook currently obtains its TLS material in one of two
ways: it generates a self-signed CA and serving certificate, or it reads a
Secret populated by cert-manager. Both paths assume that the serving private key
is stored in a Kubernetes Secret and copied into the webhook's serving
directory.

Many production clusters instead use an external PKI agent, CSI driver,
projected Secret, or sidecar to deliver and rotate short-lived certificates as
files. This KEP adds an opt-in `filesystem` certificate provider that consumes
those files without creating, reading, updating, or copying a certificate
Secret.

The provider validates and hot-reloads the serving identity. CA publication is
a separate responsibility: the Spark Operator can synchronize the mounted CA
bundle into the mutating and validating webhook configurations, or an external
injector or declarative deployment system can be the sole `caBundle` owner.

Existing self-signed and cert-manager installations remain unchanged by
default.

This work is delivered in two iterations. Iteration 1 (v1) ships the functional,
opt-in provider with a minimal CA-validation floor; Iteration 2 (v2) adds semantic
certificate hardening and a coordinated trust-overlap protocol for fully safe
issuer and root rotation. The [Design Details](#design-details) describe the
complete target; the [Iteration Plan](#iteration-plan) states which behavior each
iteration delivers, and each design subsection is annotated accordingly.

![Proposed certificate architecture](webhook-certificate-architecture.svg)

## Motivation

An external PKI already owns certificate issuance, private-key rotation, and
renewal. Requiring that material to pass through the current self-signed or
cert-manager paths creates one or more of the following problems:

- A second component becomes responsible for a private key.
- The webhook retains Secret permissions it does not need.
- Certificate files are copied only at startup and do not follow renewal.
- A pod restart is required to serve a renewed certificate.
- Each external-PKI installation maintains a security-sensitive downstream
  patch.

Admission webhooks are fail-closed by default. A serving-certificate or CA
rotation mistake can therefore prevent unrelated Kubernetes API operations.
The upstream contract needs to define ownership, validation, overlap, and
rollback rather than merely adding a flag that skips certificate generation.

Related work:

- [#1178](https://github.com/kubeflow/spark-operator/issues/1178) requested
  cert-manager support.
- [#2373](https://github.com/kubeflow/spark-operator/pull/2373) implemented the
  current cert-manager path.
- [#2502](https://github.com/kubeflow/spark-operator/issues/2502) tracks future
  webhook deprecation. Existing supported releases still require safe TLS
  operation until that work is complete.

### Goals

Goals are tagged by the iteration that delivers them (see
[Iteration Plan](#iteration-plan)):

- Add an explicit, opt-in filesystem certificate provider. *(v1)*
- Preserve the existing self-signed default and cert-manager compatibility. *(v1)*
- Never access a certificate Secret in filesystem mode. *(v1)*
- Validate the initial and replacement serving identity before accepting it.
  *(v1 accepts a parseable, key-matched pair and applies a CA↔served-leaf
  interlock; full semantic preflight — SAN, usage, issuer, chain, bounds — is v2.)*
- Reload valid certificate renewals without restarting the pod. *(v1)*
- Support one or more CA certificates for issuer overlap. *(v1 accepts and
  publishes multi-certificate bundles; the overlap-safe rotation protocol is v2.)*
- Give exactly one component ownership of admission `caBundle` fields. *(v1)*
- Repair operator-owned CA drift after file or Kubernetes object changes. *(v1)*
- Render provider-specific probes and least-privilege RBAC. *(RBAC gating v1; the
  filesystem startup probe is v2.)*
- Define safe upgrade, root rotation, and rollback procedures. *(v2)*

### Non-Goals

- Issuing certificates or generating private keys for external PKI.
- Integrating directly with Vault, SPIRE, a particular CSI driver, or a service
  mesh.
- Building a second Spark-Operator-specific filesystem notification library.
- Fixing all cert-manager renewal behavior in this change.
- Managing CRD conversion-webhook CA bundles.
- Changing admission failure policies, selectors, ports, or handlers.
- Changing the default certificate provider.
- Guaranteeing atomicity across filesystems, Kubernetes objects, API servers,
  and webhook replicas.

## Proposal

Add `filesystem` alongside the current self-signed and cert-manager certificate
sources. The filesystem provider reads a serving certificate chain, private key,
and minimal CA bundle from configurable paths. It does not persist or copy the
external private key.

Certificate source and CA publication are selected independently:

- **Operator-owned CA publication:** the existing admission-configuration
  controllers publish the validated CA bundle.
- **External CA publication:** the Spark Operator does not read or write the
  admission configurations for certificate management. An injector, Helm,
  GitOps controller, or administrator owns the fields.

Exactly one CA writer is allowed. The binary and chart reject combinations that
would create two writers.

### User Stories

#### Story 1: platform PKI with operator-owned CA publication

A platform team mounts `tls.crt`, `tls.key`, and `ca.crt` into the webhook pod.
The webhook waits for a valid initial generation, serves it, follows renewals,
and publishes `ca.crt` into both admission configurations.

#### Story 2: existing CA injector

A cluster already uses a CA injector or GitOps controller. The platform team
mounts the same serving and CA files but disables Spark Operator CA
synchronization. The external system remains the only admission `caBundle`
writer.

#### Story 3: existing installation

An operator upgrades without selecting the new provider. Self-signed and
cert-manager behavior, resource names, command-line arguments, and rendered
manifests remain unchanged.

### Ownership Model

The following matrix is normative:

| Provider | CA sync | Serving identity owner | Certificate Secret access | `caBundle` owner |
|---|---|---|---|---|
| `self-signed` | `auto` or `enabled` | Spark Operator | Create, get, update | Spark Operator |
| `self-signed` | `disabled` | Spark Operator | Create, get, update | External system |
| `cert-manager` | `auto` | cert-manager | Get | cert-manager cainjector |
| `cert-manager` | `enabled` | Invalid initial combination | - | - |
| `cert-manager` | `disabled` | cert-manager | Get | External system |
| `filesystem` | `auto` or `enabled` | External PKI | None | Spark Operator |
| `filesystem` | `disabled` | External PKI | None | External system |

`auto` is deterministic. It never infers ownership from installed CRDs,
annotations, file presence, or field managers:

- self-signed: Spark Operator owns `caBundle`;
- cert-manager: cert-manager cainjector owns `caBundle`;
- filesystem: Spark Operator owns `caBundle`.

`self-signed + disabled` is retained to support staged ownership transfer.
`cert-manager + enabled` is initially invalid because the current provider does
not expose a dynamic CA source independent of cainjector.

### Risks and Mitigations

*Iteration: v1 delivers the last-known-good retention, the single-owner resolution,
the bounded periodic poll, the every-replica-refresh / leader-only-write split, and
compare-before-patch. The overlap-bundle mitigation for serving a new issuer before
the API server trusts it, and the published-trust stabilization it depends on, are
v2 — so in v1 the corresponding rows below are addressed only for leaf renewal under
an unchanged, already-trusted issuer (see
[Known v1 limitations](#known-v1-limitations)).*

| Risk | Mitigation |
|---|---|
| A partial or invalid serving generation causes an outage | Validate before cache replacement and retain the last accepted pair |
| A new issuer is served before the API server trusts it | Publish an old-plus-new overlap bundle before accepting the new leaf |
| Two reconcilers fight over `caBundle` | Resolve one explicit owner and reject conflicting chart values or annotations |
| Filesystem notifications are unavailable or miss projected-volume changes | Keep bounded periodic polling independent of notification watch success |
| Followers compare against stale CA state | Refresh desired trust on every replica; only the leader writes Kubernetes objects |
| CA polling creates API churn | Compare before patching and perform zero writes after convergence |
| The webhook identity is accidentally reused by secure metrics | Use separate TLS option slices and install the certificate callback only on the webhook server |
| Broad enterprise trust permits webhook impersonation | Require a dedicated minimal CA bundle and report certificate count and digest |
| Upgrade or rollback reintroduces an old CA writer | Use a compatibility release and retain overlap until the rollback window closes |

### Open Questions

#### Question 1: controller-runtime watcher dependency

The controller-runtime watcher used by the current webhook server parses a
matching key pair but does not validate SAN, validity, usage, or issuer before
replacing its cache. It also compares only the leaf and key when detecting
changes, and its polling fallback does not start if filesystem watch
registration fails.

Options:

| Option | Trade-off |
|---|---|
| Enhance controller-runtime and consume the released version | Keeps certificate watching in the shared library, but introduces an upstream dependency and release wait |
| Add a bounded poll-based loader in Spark Operator | Unblocks implementation, but creates local TLS reload code to maintain |
| Accept controller-runtime's current validation | Smaller change, but a parseable wrong-SAN, expired, or wrong-issuer renewal can replace the working identity |

**Recommendation:** enhance controller-runtime. If that cannot be completed in
the implementation window, use a small poll-based loader only with explicit
maintainer approval and the same validation contract.

#### Question 2: initial chart surface for external CA ownership

Options:

| Option | Trade-off |
|---|---|
| Generic admission-configuration annotations | Supports existing injectors without depending on one product |
| Static CA PEM in Helm values | Simple for stable roots, but every rotation requires a chart release |
| Require out-of-band ownership | Smallest chart API, but initial installation is harder to make safe |

**Recommendation:** support generic annotations and optional static PEM. They are
mutually exclusive, and both remain disabled by default.

## Design Details

The design below is the complete target. It is delivered in two iterations so a
functional, opt-in path lands first and the semantic and multi-object-safety
hardening follows. Each subsection is annotated with the iteration that delivers
it.

### Iteration Plan

#### Iteration 1 (v1) — implemented, opt-in

- **`filesystem` serving identity.** The webhook reads the serving chain and key
  and hot-reloads renewals in place, without creating, reading, or copying a
  Secret. Parsing and key-match use the Kubernetes apiserver
  `dynamiccertificates` content/controller; a renewed pair is published
  atomically.
- **Operator-owned CA publication.** In `filesystem + auto/enabled` mode the
  operator reads `ca.crt`, applies the v1 validation floor, and reconciles both
  named admission configurations with compare-before-write, a targeted
  strategic-merge patch keyed by webhook name, an optimistic lock, and a jittered
  periodic safety reconcile. Publication is leader-elected; every replica refreshes
  its own local trust.
- **v1 CA validation floor (Option A).** A candidate bundle is accepted only when
  it (1) parses to one or more PEM `CERTIFICATE` blocks and canonicalizes —
  preserving input order and dropping exact-duplicate DER — and (2) the currently
  served leaf verifies against a certificate pool built from the candidate
  (CA↔served-leaf interlock). Any failure retains the last-known-good bundle and
  retries on the next poll. This never publishes unparseable input and never
  publishes a bundle that would reject the operator's own serving leaf.
- **Readiness gate.** In `filesystem + operator` mode `readyz` stays not-ready
  until both admission configurations carry the committed bundle.
- **RBAC gating and multi-replica guard.** Admission `list`/`watch`/`get`/`patch`
  permissions render only under operator ownership; the chart fails to render when
  operator ownership is combined with more than one replica and leader election
  disabled.

#### Iteration 2 (v2) — designed here, deferred

- **Semantic serving preflight.** SAN match against the webhook Service DNS name,
  server-authentication usage, validity window, issuer, full chain validation
  against the mounted CA, 1 MiB / 100-certificate read bounds, and mixed-pair
  (projected-symlink-switch) rejection. v1 relies on the library's parse-and-match
  only.
- **Semantic CA hardening.** Reject private keys, unknown block types, trailing
  data, malformed DER, and non-CA certificates (`IsCA`/BasicConstraints), and
  verify intermediate chains. The v1 interlock is leaf-only against a roots pool
  and therefore assumes the served leaf is issued directly by a CA present in the
  bundle (for example, Madkub's roots-only `cacerts.pem`).
- **Coordinated trust state machine.** A per-replica desired/published/served
  tracker with `--webhook-ca-bundle-stabilization-interval`, so a replacement leaf
  is accepted only against *published* trust and endpoints stay ready during
  overlap.
- **Overlap-safe rotation and migration.** The issuer/root rotation overlap
  procedure, the compatibility-release migration and rollback sequence, the
  matching webhook startup probe, the two Kustomize overlays, and the observability
  metrics.

#### Known v1 limitations

Until v2 lands, v1 is safe only under the stated constraints. These are called out
again in [Risks and Mitigations](#risks-and-mitigations) and [Drawbacks](#drawbacks):

- Serving identity and CA publication advance independently, so a new-issuer leaf
  can be served while the source and admission objects still carry the old CA. v1
  is therefore safe for **leaf renewal under an unchanged issuer already trusted by
  the published bundle**, not for issuer or root rotation.
- During a safe trust expansion (old → old+new), every replica can become unready
  before the leader publishes the combined bundle.
- The interlock verifies only the served leaf against a roots pool; it cannot
  bootstrap a leaf/intermediate/root chain when `ca.crt` contains roots only and
  an intermediate is required.
- Per-replica desired trust can diverge; after leader failover a stale replica can
  roll the published bundle backward, and readiness byte-equality does not prevent
  it.
- Filesystem bootstrap can wait up to `--webhook-cert-wait-timeout` before the
  health server starts, and v1 ships no matching webhook startup probe.

### Configuration

The binary adds these flags:

| Flag | Default | Description |
|---|---|---|
| `--webhook-cert-provider` | `self-signed` | `self-signed`, `cert-manager`, or `filesystem` |
| `--webhook-cert-dir` | Existing default | Serving-certificate directory |
| `--webhook-cert-name` | Existing `tls.crt` | Certificate-chain filename |
| `--webhook-key-name` | Existing `tls.key` | Private-key filename |
| `--webhook-ca-bundle-file` | Empty | Filesystem CA path; defaults to `<cert-dir>/ca.crt` in filesystem mode |
| `--webhook-ca-bundle-sync` | `auto` | `auto`, `enabled`, or `disabled` |
| `--webhook-ca-bundle-sync-interval` | `10s` | Successful filesystem refresh interval |
| `--webhook-ca-bundle-stabilization-interval` | `20s` | *(v2)* Time both admission objects must match before new trust authorizes a replacement leaf |
| `--webhook-cert-wait-timeout` | `2m` | Maximum initial wait for filesystem material |
| `--enable-cert-manager` | Existing `false` | Compatibility alias |

*Iteration: v1 implements `--webhook-cert-provider`, `--webhook-cert-dir`,
`--webhook-cert-name`, `--webhook-key-name`, `--webhook-ca-bundle-file`,
`--webhook-ca-bundle-sync`, `--webhook-ca-bundle-sync-interval`, and
`--webhook-cert-wait-timeout`. `--webhook-ca-bundle-stabilization-interval` and the
published-trust tracker it feeds are deferred to v2, so the
stabilization-vs-sync-interval validation applies only once v2 lands.*

The command uses Cobra's `Flag.Changed` state to distinguish an omitted provider
from an explicitly selected value:

| Provider flag | Legacy cert-manager flag | Result |
|---|---|---|
| Omitted | false or omitted | `self-signed` |
| Omitted | true | `cert-manager` |
| `self-signed` | false or omitted | `self-signed` |
| `cert-manager` | either value | `cert-manager`; warn when both select it |
| `filesystem` | false or omitted | `filesystem` |
| `self-signed` or `filesystem` | true | Reject the conflict |

Unknown providers, unknown sync modes, non-positive intervals, and a
stabilization interval shorter than the sync interval fail before API mutation.

### Helm and Kustomize

The chart adds the following values under `webhook`:

```yaml
certificate:
  # Empty preserves certManager.enable compatibility.
  provider: ""
  certDir: ""
  certName: ""
  keyName: ""
  waitTimeout: 2m
  caBundle:
    file: ""
    sync: auto
    syncInterval: 10s
    stabilizationInterval: 20s
    # Used only when sync is disabled and Helm owns the field.
    pem: ""

# Used only when sync is disabled and an injector owns the field.
admissionConfigurationAnnotations: {}
```

A shared template helper resolves the provider once. Certificate resources,
Deployment arguments, cainjector annotations, and RBAC all use the resolved
value.

Filesystem mode uses the existing generic `webhook.volumes` and
`webhook.volumeMounts` extension points. It rejects the chart's default writable
`emptyDir` and `subPath` mount because those would either contain no external
material or prevent projected Secret updates. The documentation provides a
read-only whole-directory example.

The Deployment omits `--webhook-secret-name` and
`--webhook-secret-namespace` in filesystem mode. It also adds a startup probe
whose budget, and the Deployment progress deadline, exceed the configured file
wait timeout.

The proposal adds two Kustomize overlays so ownership-specific RBAC and
readiness are explicit:

- `webhook-filesystem-operator-ca`
- `webhook-filesystem-external-ca`

*Iteration: v1 renders `--webhook-cert-provider` and the `--webhook-ca-bundle-*`
arguments, gates admission RBAC behind operator ownership, and enforces the
multi-replica leader-election guard. The filesystem startup probe, the two
Kustomize overlays, and the `caBundle.stabilizationInterval` / `caBundle.pem` /
`admissionConfigurationAnnotations` values are v2.*

### Startup and Serving-Certificate Reload

*Iteration: v1 performs the bounded startup wait and the parse/key-match/atomic-
reload using the apiserver `dynamiccertificates` content and controller. The full
preflight checklist below (SAN, validity, server-authentication usage, chain
validation, and the 1 MiB / 100-certificate bounds), the mixed-pair rejection, and
the published-trust replacement rule are v2; v1 accepts any parseable, key-matched
pair and relies on the CA↔served-leaf interlock in
[CA Bundle Reconciliation](#ca-bundle-reconciliation).*

Filesystem delivery may be asynchronous. Startup retries transient file absence,
permission errors, partial writes, and key mismatch until the bounded timeout.
It exits immediately on context cancellation.

One complete generation must pass all of these checks:

- Certificate and key parse through `tls.X509KeyPair`.
- The first certificate matches the private key.
- The leaf is currently valid.
- The leaf includes the exact webhook Service DNS SAN.
- Server authentication is allowed when extended usages are present.
- The leaf chain validates against the mounted CA bundle.

Certificate and CA files are limited to 1 MiB and 100 certificates. The key file
is limited to 1 MiB. Errors identify the path and validation category but never
include PEM or private-key contents.

The serving chain is leaf first, followed by intermediates. A projected-volume
symlink switch may occur between two file reads; a mixed pair is rejected and
retried while the previous accepted pair remains served.

The reload component validates a candidate before replacing its cache. A
replacement in operator-owned mode validates against **published trust**: the
last desired CA bundle observed in both admission configurations for the
stabilization interval. This prevents accepting a new issuer before trust has
converged. In external-owned mode, where admission read permissions are absent,
the candidate validates against the last valid mounted CA and the external owner
is responsible for publishing the same trust safely.

The command constructs independently owned webhook and metrics TLS option
slices from their common protocol policy. The reload component's
`GetCertificate` callback is installed only on the webhook server. The secure
metrics listener retains its existing certificate-selection behavior.

### CA Bundle Reconciliation

*Iteration: v1 implements the candidate/commit contract with the Option-A floor —
steps 1, 3, 4, and 5, plus the parse-≥1-`CERTIFICATE` half of step 2. The full
rejection set in step 2 (private keys, unknown block types, trailing data,
malformed DER, non-CA certificates) and the published-trust advance in the final
paragraph are v2. The v1 interlock in step 4 verifies the served leaf against a
roots pool built from the candidate, so it assumes a directly-issued leaf.*

CA parsing and publication use a candidate/commit contract:

1. Read and parse a candidate without changing the last-known-good value.
2. Require one or more PEM `CERTIFICATE` blocks and reject private keys, unknown
   block types, trailing data, malformed DER, and non-CA certificates.
3. Canonically encode certificates while preserving input order and removing
   exact duplicates.
4. Verify that the candidate still validates the currently served leaf.
5. Commit the candidate as desired trust only after validation succeeds.

Every webhook replica refreshes desired trust for local serving validation and
readiness. A successful change immediately enqueues both fixed admission object
names. Only the leader-elected reconcilers publish it.

Each reconciler:

1. Reads committed desired trust.
2. Gets its named mutating or validating webhook configuration.
3. Compares every webhook entry's current `caBundle` with desired trust.
4. Returns without writing when all entries match.
5. Applies a targeted strategic merge patch keyed by webhook name when they do
   not match.
6. Schedules a jittered periodic safety reconciliation.

Kubernetes object watches remain for prompt drift repair. File refresh is
periodic because file changes do not emit Kubernetes events. Invalid file input
retains the last-known-good desired and API state and retries after the bounded
interval rather than entering unbounded workqueue backoff.

After both managed admission configurations continuously match desired trust for
the stabilization interval, the per-replica tracker advances published trust.
Readiness and replacement-leaf validation continue using the previous published
trust while a newer desired bundle converges.

### Rotation and Migration

*Iteration: leaf renewal under an unchanged, already-trusted issuer is supported
in v1. The issuer-rotation overlap procedure, the compatibility-release migration
and rollback sequence, and the `update`→`patch` two-release RBAC transition all
depend on the published-trust tracker and stabilization interval and are therefore
v2. In v1, issuer or root rotation is not safe (see
[Known v1 limitations](#known-v1-limitations)).*

Leaf renewal under an unchanged issuer is safe when the external owner publishes
the new certificate and key as one generation. Replicas may briefly serve old
and new leaves because both chain to already published trust.

Issuer rotation requires overlap:

1. Publish a CA file containing old and new trust anchors.
2. Wait until both admission configurations contain that bundle.
3. Wait for the stabilization interval and verify admission TLS.
4. Publish the new serving certificate and key.
5. Wait until all replicas serve the new identity.
6. Preserve overlap through the rollback window.
7. Remove the old CA and verify admission again.

![Safe certificate rotation](webhook-certificate-rotation.svg)

There is no portable transaction across filesystem generations, two Kubernetes
objects, all API servers, and all replicas. Simultaneously replacing the old CA
and leaf is unsupported.

Migration from an older release uses a compatibility release that understands
the new provider and `sync=disabled`:

1. Deploy the compatibility release with the existing provider and owner.
2. Wait until no older replica remains.
3. Disable in-process CA sync while retaining the old leaf.
4. Start one persistent external CA owner and publish old-plus-new overlap.
5. Roll to filesystem serving while retaining overlap.
6. If operator ownership is desired, stop the external writer while retaining
   its last-applied field, then enable the operator reconciler with the same
   overlap.
7. Remove old trust only after the rollback window closes.

Rollback targets the compatibility release, not a legacy binary that would
restart self-minting and overwrite prepared trust.

### Readiness

*Iteration: v1 implements the bootstrap-convergence gate (first paragraph below).
The published-trust refinement (second paragraph) that keeps endpoints ready
during overlap is v2; in v1 readiness compares against the committed desired
bundle directly.*

The current server-started check does not prove that the API server trusts the
webhook. For explicitly selected filesystem mode with operator-owned CA sync,
readiness remains false until both admission configurations contain the
validated bootstrap CA.

After initial convergence, readiness uses published trust rather than newly read
desired trust. This keeps endpoints ready while overlap is being published or a
projected replacement is temporarily partial.

This is a local object-convergence signal, not proof that every API server has
consumed the latest object. The rotation procedure still requires a
stabilization interval and a real admission/TLS check before leaf cutover.

External-owned mode does not inspect admission objects because doing so would
require permissions solely to monitor another owner's responsibility.

### RBAC

Permissions are rendered from resolved responsibilities:

| Responsibility | Permission |
|---|---|
| Self-signed material | Create Secrets; get and update the named Secret |
| Current cert-manager provider | Get the named Secret |
| Filesystem provider | No certificate Secret permission |
| Operator CA publication | List/watch with exact-name field selectors; get and patch the named admission objects |
| External CA publication | No admission read or write permission for certificate management |

The existing reconcilers use full `update`. v1 adds `patch` to the operator role
(the reconcilers apply a strategic-merge patch) while retaining `update`. Dropping
`update` via the two-release RBAC sequence — a compatibility release granting both,
then a following release removing `update` — is a v2 migration step.

Operator-owned CA publication requires leader election when the chart renders
more than one webhook replica. The chart rejects multiple replicas with resolved
operator ownership and leader election disabled.

### Observability

*Iteration: v1 logs the resolved provider and CA owner and logs CA commits without
PEM contents. The fixed-cardinality metrics below are v2.*

Startup logs the resolved provider and CA owner. Rotation logs include only a
short certificate or CA digest, certificate count, validity metadata, and target
resource. PEM and private-key contents are never logged.

Proposed fixed-cardinality metrics:

- serving-certificate expiry time, labeled by provider;
- CA refresh success timestamp;
- CA synchronization errors, labeled by resource kind and error category;
- CA update count, labeled by resource kind;
- CA in-sync state, labeled by resource kind.

Serial numbers, paths, full digests, Secret names, and raw error strings are not
metric labels.

### Test Plan

[x] I understand the owners of the involved components may require updates to
existing tests before implementation.

*Iteration: the coverage delivered in v1 is listed under
[Iteration 1 (v1) coverage](#iteration-1-v1-coverage). The prerequisite, unit,
integration, and E2E sections that follow are the complete v2 target; items not
listed as v1 coverage are deferred to v2.*

#### Iteration 1 (v1) coverage

Implemented and green in v1:

- **Unit.** Options/ownership resolution (owner matrix, invalid combinations,
  duration and sync-mode validation); filesystem CA source floor A (parse ≥1
  certificate, order-preserving dedupe, interlock accept when the served leaf
  chains to the candidate and reject when it does not, last-known-good retention
  on bad input, broadcast only on change, `NeedLeaderElection()==false`); dynamic
  serving acquire/reload; both reconcilers via a fake `CABundleSource` (drift,
  no-op, targeted patch); readiness bootstrap-not-ready→ready.
- **Integration (envtest).** A real API server with both named admission
  configurations and both reconcilers wired through a manager in
  `filesystem + operator` mode: both configurations converge to the committed
  bundle; `readyz` flips false→true on convergence; rotating the CA file (adding a
  second root) converges both configurations; a corrupt file retains the
  last-known-good bundle with no re-patch (stable resource versions); a follower
  source refreshes its local trust without publishing.
- **Chart/Kustomize.** helm-unittest over the rendered provider and
  `--webhook-ca-bundle-*` arguments, the RBAC ownership gating, and the
  multi-replica leader-election guard; `drift-check`; the Kustomize build test.

The v1 envtest proves **admission-object caBundle convergence and readiness**. It
does not start a webhook server, perform a verified TLS or admission request,
rotate the serving issuer, or exercise real leader-election failover; those are
part of the v2 integration and E2E target below.

#### Prerequisite testing updates

- Add focused tests for both admission reconcilers.
- Fix and test validating-reconciler error propagation.
- Prove converged reconciliation performs no API write.
- Record default Helm and Kustomize output as the compatibility baseline.

#### Unit tests

The implementation touches currently tested packages, but coverage must be
measured and recorded in this section when implementation starts.

| Area | Required cases |
|---|---|
| Configuration | Defaults, legacy alias, every provider and owner, conflict and duration validation |
| File preflight | Delayed and partial files, key mismatch, validity, SAN, usage, issuer, chain ordering, size limits, cancellation |
| Reload | Last-known-good retention, projected symlink switch, full-chain-only change, watch failure with polling fallback |
| TLS isolation | Secure metrics never serves or follows the filesystem webhook identity |
| CA source | Candidate/commit ordering, canonicalization, overlap, invalid candidate retention, bounded retry |
| Reconcilers | Initial enqueue, drift, no-op, targeted patch, API errors, deletion and recreation |
| Readiness | Bootstrap convergence, follower readiness, overlap publication, external ownership |

#### Integration tests

- Start managers against envtest with pre-created admission configurations and
  delayed filesystem material.
- Rotate and corrupt CA input, proving update, retention, and automatic recovery.
- Run two managers with leader election, proving one writer and two ready serving
  replicas.
- Verify resource versions stop changing after convergence.
- Render all Helm provider/owner combinations and both Kustomize overlays,
  checking arguments, probes, annotations, CA encoding, mounts, and RBAC.

#### E2E tests

- Install with externally mounted test certificates through Helm and Kustomize.
- Exercise a real mutating and validating admission request.
- Renew the leaf without pod restart or admission failure.
- Publish old-plus-new CA overlap, change issuer, then remove old trust without
  admission failure.
- Skew rotation across two replicas.
- Inject missing, partial, malformed, expired, wrong-SAN, and wrong-issuer
  replacements and prove last-known-good behavior.
- Test upgrade and rollback through the compatibility release.

The required CI modes are `default`, `filesystem-operator-ca`, and
`filesystem-external-ca`. Duplicate Kubernetes-version combinations may be
reduced to keep CI cost bounded.

### Graduation Criteria

*Iteration: v1 targets the first three Alpha criteria. The E2E leaf-renewal and
issuer-overlap transition require the v2 semantic preflight and overlap protocol
and complete Alpha in v2.*

#### Alpha

- Provider and ownership APIs are accepted. *(v1)*
- Unit, integration, Helm, and Kustomize tests pass. *(v1, at the coverage above)*
- Default behavior and rendered manifests are unchanged. *(v1)*
- At least one E2E leaf renewal and issuer-overlap transition pass. *(v2)*

#### Beta

- Multi-replica skew and rollback tests run in CI.
- At least two external certificate delivery mechanisms have experience reports.
- Observability names and labels are stable.
- No unresolved high-severity security or availability defects remain.

#### Stable

- The feature has shipped for two minor releases without API changes.
- Default and opt-in E2E paths remain supported in CI.
- Maintainers confirm the feature remains useful relative to the webhook
  deprecation plan.

## Implementation History

- **2026-09-13** - Tracking issue
  [#3165](https://github.com/kubeflow/spark-operator/issues/3165) opened and KEP
  drafted.
- **2026-09-23** - Split delivery into Iteration 1 (v1) and Iteration 2 (v2);
  annotated the design with per-iteration scope. v1 (functional filesystem serving
  + operator-owned CA publication with the Option-A validation floor and
  convergence readiness) implemented and proposed as stacked PRs; v2 semantic
  hardening and the coordinated trust-overlap protocol remain designed here and
  deferred.

## Drawbacks

This adds flags, chart values, reconciliation, tests, and operational guidance to
a webhook that may eventually be deprecated. It also adds bounded file polling
and provider-specific RBAC branches.

The design cannot make root rotation globally atomic. It depends on an overlap
procedure and on external file writers publishing complete generations. External
CA ownership also means the Spark Operator cannot prove that mounted trust and
admission trust are identical without violating least privilege.

Finally, strict validation can reject certificate generations that an existing
downstream patch might have attempted to serve. That is intentional for a
fail-closed admission endpoint, but it requires clear diagnostics.

Delivering in two iterations means v1 ships with the
[Known v1 limitations](#known-v1-limitations): serving identity and CA publication
advance independently, trust expansion can leave replicas transiently unready, the
interlock is leaf-only, a stale replica can roll the bundle backward after
failover, and there is no filesystem startup probe yet. v1 is opt-in and safe for
leaf renewal under an unchanged, already-trusted issuer; issuer and root rotation
require the v2 protocol. These are accepted deliberately to land a reviewable,
functional path before the larger hardening change.

## Alternatives

### Require a pre-created Kubernetes Secret

This reuses part of the existing path but excludes PKI systems that write only
files, adds API persistence for private keys, and still requires correct live
reload behavior. Users remain free to project a Secret as filesystem input, but
it is not required by the provider.

### Reuse `--enable-cert-manager`

Filesystem PKI is not cert-manager. Reusing the boolean would render
cert-manager-specific resources and annotations, retain Secret access, and make
source and field ownership ambiguous.

### Add an `externally-managed` boolean

A boolean can skip generation but cannot describe whether files or a Secret are
the source or who owns `caBundle`. An enum plus a separate owner selection is
more explicit and extensible.

### Always use an external CA injector

This gives the Spark Operator less RBAC but makes a generic filesystem provider
depend on a separate privileged component that is not installed in every
cluster. External ownership remains supported rather than required.

### Always let the Spark Operator publish CA trust

This is turnkey but creates dual ownership in clusters with an existing
injector or GitOps manager. Explicit disablement is required for safe adoption.

### Copy files with an init container and restart for renewal

This snapshots the initial generation, duplicates private-key material, expands
the image trust boundary, and turns routine renewal into a disruptive rollout.

### Maintain a downstream-only patch

That avoids upstream API surface but makes each external-PKI operator maintain
the same security-sensitive behavior and migration logic. The requirement is
generic enough to support upstream.

### Wait for webhook deprecation

The completion and release timeline for removing webhook functionality is not
defined. Existing supported deployments still need safe certificate ownership
and rotation during that period.
