# Kubernetes-native Workload scheduler integration

:::{admonition} Alpha feature
:class: warning

The `workload` scheduler backend requires a cluster serving `scheduling.k8s.io/v1alpha2`. This API
is available in Kubernetes 1.36. The cluster must enable `GenericWorkload` on kube-apiserver and kube-scheduler, and enable `GangScheduling` on kube-scheduler. These
Kubernetes features are alpha and disabled by default.
:::

The Kubernetes-native `workload` backend uses the upstream Kubernetes
[`Workload`](https://kubernetes.io/docs/concepts/workloads/workload-api/) and
[`PodGroup`](https://kubernetes.io/docs/concepts/workloads/podgroup-api/) APIs. It groups the
executor pods of a `SparkApplication` so the Kubernetes scheduler can place the configured minimum
number atomically.

## Cluster prerequisites

Enable the following Kubernetes features before installing the operator:

| Component | Required configuration |
|---|---|
| kube-apiserver | `GenericWorkload=true` and `scheduling.k8s.io/v1alpha2` served |
| kube-scheduler | `GenericWorkload=true` and `GangScheduling=true` |

If you set `batchSchedulerOptions.priorityClassName`, also enable the alpha
`WorkloadAwarePreemption` feature gate on the API server and scheduler. See the upstream
[gang scheduling](https://kubernetes.io/docs/concepts/scheduling-eviction/gang-scheduling/) and
[workload-aware preemption](https://kubernetes.io/docs/concepts/scheduling-eviction/workload-aware-preemption/)
documentation for details.

## Enable the backend

Enable batch scheduling and the `workload` backend when installing the Helm chart:

```yaml
controller:
  batchScheduler:
    enable: true
    workload:
      enable: true
```

You can optionally make it the default for applications that do not set `.spec.batchScheduler`:

```yaml
controller:
  batchScheduler:
    enable: true
    default: workload
    workload:
      enable: true
```

`workload` selects the Spark operator's batch-scheduler backend. It is not the name of a separate
scheduler process and is not copied to `pod.spec.schedulerName`. Unless a role-specific
`schedulerName` is explicitly configured, the pods use the cluster's default kube-scheduler with the
GangScheduling plugin.

When the backend is enabled, the backend factory is registered at controller startup. API discovery
occurs when the backend is invoked during application reconciliation, not at startup. Discovery
failures return an error for that application's reconciliation attempt without aborting the
controller process.

## Submit an application

Set `.spec.batchScheduler` to `workload`. A complete example is available at
[`examples/spark-pi-workload.yaml`](https://github.com/kubeflow/spark-operator/blob/master/examples/spark-pi-workload.yaml).

```yaml
spec:
  batchScheduler: workload
  batchSchedulerOptions:
    minMember: 2
  executor:
    instances: 2
```

Apply the example and inspect the generated scheduling resources:

```bash
kubectl apply -f examples/spark-pi-workload.yaml
kubectl get workloads.scheduling.k8s.io spark-pi-workload -n default
kubectl get podgroups.scheduling.k8s.io -n default
kubectl get pods -n default \
  -l sparkoperator.k8s.io/app-name=spark-pi-workload \
  -o custom-columns=NAME:.metadata.name,ROLE:.metadata.labels.spark-role,PODGROUP:.spec.schedulingGroup.podGroupName,NODE:.spec.nodeName
```

## Scheduling behavior

For each `SparkApplication`, the operator creates:

- one application-owned `Workload` containing the reusable scheduling policy; and
- one application-owned `PodGroup` for each submission attempt.

Only executor pods join the `PodGroup`. In cluster mode, the driver must schedule independently so
it can create those executor pods; including the driver in the same gang would cause a bootstrap
deadlock.

The executor pod template's native membership (`spec.executor.template.spec.schedulingGroup.podGroupName`)
is populated by the operator during submission for this backend. This operator-generated state
belongs to a temporary submission copy and is never persisted to the reconciled `SparkApplication`.

The operator deletes a submission's `PodGroup` during scheduler cleanup. It retains the `Workload`
for later submissions, and Kubernetes garbage collection removes it when its owning
`SparkApplication` is deleted.

If a retained `Workload` has a scheduling policy (minimum count or priority) that no longer matches
the application's current configuration, the backend will refuse to reuse it and fail scheduling.
Deleting the old `PodGroup` alone does not resolve this conflict; delete the `Workload` itself to
allow the backend to create a new one with the updated policy.

## Gang size

By default, the gang's `minCount` is the initial executor count, clamped to at least one. Set
`batchSchedulerOptions.minMember` to request a smaller quorum:

```yaml
spec:
  batchScheduler: workload
  batchSchedulerOptions:
    minMember: 2
  executor:
    instances: 3
```

The admission webhook validates that `minMember` is at least one (lower-bound check). The workload
backend validates that `minMember` does not exceed the initial executor count (upper-bound check).
The backend rejects applications that fail this validation.

The initial executor count is resolved from the effective Spark configuration, including values
specified via `sparkConf`. For example, an application with no typed `spec.executor.instances` but
with `spark.executor.instances: "4"` in `sparkConf` will be correctly sized as having 4 initial
executors.

### Dynamic allocation

With dynamic allocation enabled, the initial executor count is the greatest of
`.spec.executor.instances` (or `spark.executor.instances` from `sparkConf`),
`.spec.dynamicAllocation.initialExecutors` (or `spark.dynamicAllocation.initialExecutors`), and
`.spec.dynamicAllocation.minExecutors` (or `spark.dynamicAllocation.minExecutors`),
clamped to at least one. Typed field values are preferred when both typed fields and `sparkConf`
specify the same setting. This value is evaluated when the submission is created; the `PodGroup` is
not resized as Spark adds or removes executors later.

## Current limitations

- `batchSchedulerOptions.queue` has no effect because the Kubernetes Workload API has no native
  queue or quota field. The admission webhook emits a warning when `batchScheduler: workload` is
  explicitly selected and a queue is supplied.
- The backend depends on alpha Kubernetes APIs and feature gates; review Kubernetes release notes
  before upgrading the cluster.
