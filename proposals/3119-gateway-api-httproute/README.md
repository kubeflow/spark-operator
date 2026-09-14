# KEP-3119: Gateway API HTTPRoute for the Spark UI

**Authors:**
- Shivansh Pandey - [@sxivansx](https://github.com/sxivansx)

**Tracking Issue:** [kubeflow/spark-operator#3119](https://github.com/kubeflow/spark-operator/issues/3119)

**Status:** Provisional

---

## Table of Contents

- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
  - [Prior art](#prior-art)
- [Proposal](#proposal)
  - [User Stories](#user-stories)
  - [Open Questions](#open-questions)
  - [Risks and Mitigations](#risks-and-mitigations)
- [Design Details](#design-details)
  - [Capability detection](#capability-detection)
  - [Route generation](#route-generation)
  - [Path handling](#path-handling)
  - [Ownership and cleanup](#ownership-and-cleanup)
  - [RBAC and chart surface](#rbac-and-chart-surface)
  - [Test Plan](#test-plan)
  - [Graduation Criteria](#graduation-criteria)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)

## Summary

The operator can expose the Spark web UI through a Kubernetes `Ingress`. This KEP adds
Gateway API `HTTPRoute` as an opt-in alternative, selected by configuration, with the
Ingress path unchanged and still the default.

The operator creates one routing object per `SparkApplication`, named and owned by that
application. Nothing about that lifecycle changes. Only the kind of object created
changes, and only when a cluster administrator or user asks for it.

Two design questions are deliberately left open for maintainers to settle in review, since
they determine the API surface and cannot be walked back once released. They are listed
under [Open Questions](#open-questions).

## Motivation

`ingress-nginx` reached end of life in March 2026. The repository is archived and
read-only, with no further releases, bug fixes, or CVE patches. Clusters that have
standardised on Gateway API implementations such as Envoy Gateway currently cannot drop
`ingress-nginx` if they also run the Spark Operator, because the operator has no other way
to expose the UI.

The generated objects are also specific to that controller by design. The getting-started
guide states that "the operator generates ingress resources intended for use with the
Ingress NGINX Controller" and gives `kubernetes.io/ingress.class: nginx` as the worked
example. `SparkUIConfiguration` exposes only `ingressAnnotations` and `ingressTLS`
alongside the chart's `controller.uiIngress`, so a cluster running a different data plane
has no supported option.

The scale makes manual conversion impractical. The operator creates one `Ingress` per
`SparkApplication`, each carrying controller-specific annotations, and the backend service
name is derived from the application name, so a single static route cannot replace them.

### Goals

- Expose the Spark web UI through a Gateway API `HTTPRoute` as an opt-in alternative to
  `Ingress`.
- Leave existing behaviour unchanged when the feature is not enabled.
- Reuse the existing per-application object lifecycle, including ownership and garbage
  collection.
- Cover both `SparkUIConfiguration` and `DriverIngressConfiguration`, which today follow
  the same Ingress pattern.
- Degrade safely on clusters that do not have the Gateway API CRDs installed.

### Non-Goals

- Removing, deprecating, or changing the `Ingress` code path. It stays the default.
- Migrating existing `Ingress` objects to `HTTPRoute` automatically.
- Supporting Gateway API kinds beyond `HTTPRoute` (`GRPCRoute`, `TLSRoute`, `TCPRoute`).
- Managing `Gateway` or `GatewayClass` objects. Those are cluster infrastructure and
  remain the administrator's responsibility.
- Istio `VirtualService` support, or any other mesh-specific routing object.

### Prior art

This has been asked for twice before and neither request was answered on the merits:

| Issue | Author | Outcome |
|---|---|---|
| [#2781](https://github.com/kubeflow/spark-operator/issues/2781) "Decouple Spark Operator from ingress-nginx" | `jonerer` | opened 2025-12-18, closed `not_planned` by the stale bot 2026-07-31 |
| [#2214](https://github.com/kubeflow/spark-operator/issues/2214) "[QUESTION] httpRoute suported" | `stephbat` | opened 2024-10-02, closed `not_planned` by the stale bot 2025-01-26 |

The wider project has already committed to this direction:

- The **GSoC 2025** project "Istio CNI and Ambient Mesh" (contributor
  [@madmecodes](https://github.com/madmecodes), mentors Julius von Kohout and Kimonas
  Sotirchos) delivered 25+ PRs described as "pioneering the migration to Gateway API
  (HTTPRoute)", including HTTPRoute-based path routing for KServe.
- **GSoC 2026 Project 5** "Platform Scalability and Security" (mentor
  [@juliusvonkohout](https://github.com/juliusvonkohout)) lists "migration from Istio
  Gateway to Kubernetes Gateway API" among its deliverables.
- [kubeflow/notebooks#1301](https://github.com/kubeflow/notebooks/pull/1301)
  ([@aojea](https://github.com/aojea)) implements a Gateway API routing provider for
  workspaces, generating `HTTPRoute` instead of Istio `VirtualService`, selected by a
  `ROUTING_PROVIDER` setting.

This KEP covers the Spark Operator's part of that migration. It does not attempt to
coordinate the other repositories.

## Proposal

Mirror the existing Ingress support rather than replace it.

When the feature is enabled and the cluster supports it, the operator creates an
`HTTPRoute` in place of the `Ingress` it would otherwise have created, attached to
administrator-supplied `parentRefs`, targeting the same Spark UI `Service`, with the same
name, namespace, labels and owner reference.

When the feature is not enabled, no new object kind is created and no existing behaviour
changes.

### User Stories

#### Story 1: cluster with no Ingress controller

A platform team has moved every workload to Envoy Gateway and wants to delete
`ingress-nginx`. They set `controller.uiHTTPRoute.enable=true` and supply the `parentRefs`
for their shared `Gateway`. Every subsequent `SparkApplication` gets an `HTTPRoute` bound
to that `Gateway`, and `ingress-nginx` can be removed.

#### Story 2: gradual migration

A team runs both data planes during a migration. They need per-application control so a
subset of applications can move to `HTTPRoute` while the rest continue to use `Ingress`.
Whether this is supported depends on [Open Question 1](#open-questions).

#### Story 3: shared chart across mixed clusters

A team installs one chart across clusters, some with the Gateway API CRDs installed and
some without. They need the operator to start and reconcile normally on both. Whether it
no-ops or refuses to start depends on [Open Question 2](#open-questions).

### Open Questions

These two decisions determine the released API surface. They are the reason this KEP
exists and are the specific points on which maintainer direction is requested.

#### Question 1: where does the switch live?

| Option | Shape | Trade-off |
|---|---|---|
| **A. Chart and operator flag only** | `controller.uiHTTPRoute.enable`, applies to every application the operator manages | No permanent API surface. Cannot mix modes in one cluster, which blocks Story 2. |
| **B. Per-application field only** | A new optional field on `SparkUIConfiguration` and `DriverIngressConfiguration` | Maximum flexibility. Adds permanent API surface to `v1beta2` and every user must opt in individually. |
| **C. Both, field overrides flag** | Operator-level default, per-application override | Matches how `ingressTLS` already works (chart default, spec overrides). Largest surface. |

**Recommendation: start with A, and treat B or C as a follow-up** once there is a concrete
request for mixed-mode. A is the only option that commits to no `v1beta2` surface, and it
can grow into C later without a breaking change. Adding a CRD field is a one-way door;
omitting one is not.

#### Question 2: require the CRDs, or detect and no-op?

| Option | Behaviour when Gateway API CRDs are absent | Trade-off |
|---|---|---|
| **A. Detect and no-op** | Operator starts, logs that HTTPRoute support is unavailable, continues without it | Matches the established pattern in this repository. A misconfiguration is silent. |
| **B. Require the CRDs** | Operator fails to start when the feature is enabled but the CRDs are missing | Misconfiguration is loud and immediate. A missing CRD takes down an otherwise healthy operator. |
| **C. Detect, but fail fast if explicitly enabled** | No-op when not requested; refuse to start when explicitly enabled and unsupported | Silent when the user did not ask, loud when they did. |

`pkg/util/capabilities.go` already establishes detection as the house pattern for
`Ingress`, via `getPreferredAvailableAPIs`, which is generic over kind rather than
hardcoded to `Ingress`.

**Recommendation: C.** Detection is already the repository's pattern, but a user who
explicitly set `uiHTTPRoute.enable=true` and gets silence has a configuration that appears
to work and does not. Failing only in that case keeps the default path quiet.

### Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Regression in the existing Ingress path | The feature is off by default and the Ingress branch is untouched. Existing unit and e2e coverage runs unchanged. |
| A cluster lacks the Gateway API CRDs | Capability detection, with behaviour settled by Question 2. |
| The operator gains RBAC it does not need | As implemented in #3125 the `gateway.networking.k8s.io/httproutes` rule is granted unconditionally, matching how the existing `ingresses` rule is handled. Gating it on `uiHTTPRoute.enable` would be tighter but splits the role template. Flagging for reviewer preference. |
| `parentRefs` point at a `Gateway` that does not exist or does not permit the namespace | The operator does not validate `Gateway` existence. Route status reports the failure, which matches how `Ingress` behaves with a missing controller. Document this. |
| New `sigs.k8s.io/gateway-api` dependency | Types-only dependency, already widely vendored across the ecosystem. No new controller or runtime component. |
| Divergence from `kubeflow/notebooks` | Question 1 option A mirrors that repository's `ROUTING_PROVIDER` operator-level selector. Worth aligning naming if maintainers prefer. |

## Design Details

### Capability detection

`pkg/util/capabilities.go` already exposes `getPreferredAvailableAPIs(client, kind)`, which
is generic over the kind. `IngressCapabilities` and `InitializeIngressCapabilities` are
callers of it, not special cases.

HTTPRoute detection therefore reuses the same helper with
`gateway.networking.k8s.io/v1`, initialised alongside `InitializeIngressCapabilities` in
`cmd/operator/controller/start.go`. No new discovery mechanism is introduced.

### Route generation

`web_ui.go:86` and `driveringress.go:105` already branch on
`util.IngressCapabilities.Has("networking.k8s.io/v1")`, choosing between
`createDriverIngressV1` and `createDriverIngressLegacy` (`extensions/v1beta1`). The
HTTPRoute path is a third branch in that existing switch, not a new subsystem.

The generated `HTTPRoute`:

- is named and namespaced identically to the `Ingress` it replaces;
- carries the same labels;
- sets `parentRefs` from configuration, defaulting `namespace` to the SparkApplication's
  own namespace when omitted;
- has one rule with a `backendRef` to the Spark UI `Service` and the configured port.

### Path handling

The operator already sets `spark.ui.proxyBase`, so a route with a path prefix match plus a
`URLRewrite` filter reproduces what the current `nginx.ingress.kubernetes.io/rewrite-target`
annotation does, without controller-specific annotations.

The hostname and path come from the same URL format value used for the Ingress, so the two
modes produce the same externally visible URL for a given application.

### Ownership and cleanup

The `HTTPRoute` carries an owner reference to the `SparkApplication`, exactly as the
`Ingress` does today. Deleting the application garbage-collects the route. No new cleanup
code path is added.

Switching an existing application between modes must not orphan the object created by the
previous mode.

### RBAC and chart surface

- `config/rbac/role.yaml` and the chart's controller role gain
  `gateway.networking.k8s.io` / `httproutes`.
- Chart values mirror the existing `controller.uiIngress` block.

### Test Plan

[x] I/we understand the owners of the involved components may require updates to existing
tests to make this code solid enough prior to committing the changes necessary to
implement this enhancement.

#### Prerequisite testing updates

None. The existing Ingress unit and e2e coverage is the regression baseline and must keep
passing unchanged with the feature off.

#### Unit Tests

Current coverage of the packages this touches, measured 2026-09-12 at `cfc9cba`:

- `pkg/util`: `2026-09-12` - `47.0%`
- `internal/controller/sparkapplication`: `2026-09-12` - `61.0%`

New unit tests cover capability detection when the CRDs are present and absent, route
generation including `parentRefs` namespace defaulting, owner references, and that the
Ingress path is unchanged when the feature is disabled.

#### E2E tests

An e2e case that installs the Gateway API CRDs and a Gateway implementation, enables the
feature, and asserts an `HTTPRoute` is created and cleaned up with the application.
`kubeflow/notebooks#1301` uses Envoy Gateway with `cloud-provider-kind` for the equivalent
test and is worth following rather than inventing a second approach.

Gating this behind a separate make target, as that PR does, keeps the default e2e matrix
unchanged.

#### Integration tests

Helm unit tests asserting the rendered controller args and RBAC rules with the feature on
and off.

### Graduation Criteria

Ships disabled by default. Considered stable once the e2e case above runs in CI and at
least one release has gone out with the feature available.

## Implementation History

- **2026-08-25** - Issue [#3119](https://github.com/kubeflow/spark-operator/issues/3119)
  filed by [@xman1980](https://github.com/xman1980).
- **2026-08-29** - [#3125](https://github.com/kubeflow/spark-operator/pull/3125) opened,
  covering capability detection and the operator-level flag, deliberately adding no CRD
  surface. All CI green.
- **2026-09-07** - Maintainer review on #3119 requested a design document before
  implementation continues.
- **2026-09-12** - KEP drafted.

## Drawbacks

It is a second routing code path to maintain, in a controller that already carries two
(`networking.k8s.io/v1` and `extensions/v1beta1`). If the Ingress path were removed first
this would be simpler, but that is not an option while the Ingress path is the default and
in use.

The Gateway API surface is also still evolving. Committing to a CRD field now, as in
Question 1 options B and C, means supporting that shape in `v1beta2` indefinitely.

## Alternatives

**Replace the Ingress path outright.** Rejected. Every current user would have to install
Gateway API CRDs and define a `Gateway` before upgrading.

**Leave it to users.** Users could disable `uiIngress` and template their own routes.
Rejected because the backend service name is derived from the application name, so routes
cannot be created ahead of time without a second controller.

**A generic routing-provider abstraction.** `kubeflow/notebooks#1301` uses
`ROUTING_PROVIDER` to select between Istio and Gateway API. A similar abstraction here
could later cover other kinds. Rejected for the first iteration as more surface than the
problem needs, but Question 1 option A is deliberately compatible with growing into it.

**Wait for Ingress to gain the needed features.** The Ingress API is feature-frozen. Not a
viable path.
