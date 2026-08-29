# Working with SparkConnect

A `SparkConnect` is a `v1alpha1` custom resource managed by the Spark Operator. It represents a long-running [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) server (the Spark driver) plus its dynamically-managed executor pods. The Spark Connect server accepts Spark client requests (for example from `spark-shell` or `pyspark`) and runs them against the cluster.

## Creating a SparkConnect

A `SparkConnect` can be created from a YAML file using `kubectl apply -f <file>`. The operator receives the object, creates the server pod, the supporting `Service`, and a `ConfigMap` containing the executor pod template. The executor pods themselves are created by Spark based on the executor configuration embedded in the server pod's `spark-submit` arguments.

```yaml
apiVersion: sparkoperator.k8s.io/v1alpha1
kind: SparkConnect
metadata:
  name: spark-connect
  namespace: default
spec:
  sparkVersion: 4.0.4
  image: "docker.io/apache/spark:4.0.4"
  server:
    cores: 1
    coreRequest: "500m"
    coreLimit: "1"
    memory: 1g
  executor:
    instances: 2
    cores: 1
    coreRequest: "500m"
    coreLimit: "1500m"
    memory: 512m
```

## Deleting a SparkConnect

A `SparkConnect` can be deleted using `kubectl delete sparkconnect <name>`. Deleting a `SparkConnect` causes the operator to garbage-collect the server pod, the `Service`, and the `ConfigMap`. Executor pods are owned by Spark and are removed when the server shuts down.

## Updating a SparkConnect

A `SparkConnect` can be updated using `kubectl apply -f <updated file>`. The operator reconciles the change. The admission webhook re-validates the spec on every update.

## Specifying CPU Resources

The `SparkPodSpec` type used by both `spec.server` and `spec.executor` exposes four related fields that control CPU allocation. They are deliberately split between Spark's task-slot count and the underlying Kubernetes CPU resource quantity.

### `cores` vs `coreRequest` / `coreLimit`

| Field | Meaning | Maps to |
|---|---|---|
| `cores` | Spark task-slot count / JVM concurrency | `spark.driver.cores` (server) or `spark.executor.cores` (executor) |
| `coreRequest` | Physical Kubernetes CPU request | container `resources.requests.cpu` |
| `coreLimit` | Physical Kubernetes CPU limit | container `resources.limits.cpu` |

`cores` and `coreRequest`/`coreLimit` are **independent**. A common configuration is `cores: 2` for task parallelism, with `coreRequest: 500m` and `coreLimit: 1500m` for the physical CPU budget that the scheduler reserves and the cgroup is allowed to consume.

### Server vs executor: how CPU resources are applied

The two sides are not symmetric. The server pod is created directly by the operator (the Spark Connect server runs in client mode), while the executor pods are created by Spark based on the configuration passed to `spark-submit`:

- `spec.server.coreRequest` and `spec.server.coreLimit` are applied **directly** by the operator to the operator-created server pod's container `resources.requests.cpu` and `resources.limits.cpu`. They are **not** mapped to `spark.kubernetes.driver.request.cores` / `spark.kubernetes.driver.limit.cores`.
- `spec.executor.coreRequest` and `spec.executor.coreLimit` are mapped to the Spark configuration keys `spark.kubernetes.executor.request.cores` and `spark.kubernetes.executor.limit.cores`, which Spark uses to set the executor pods' `resources.requests.cpu` / `resources.limits.cpu` when it creates them.

### Precedence with pod-template CPU resources (server only)

The `spec.server.template` field lets you supply an arbitrary pod template that the operator uses as a base when building the server pod. The operator applies the precedence rules below, matching the `addMemoryLimit` resource-merge convention used elsewhere in the operator:

- If `spec.server.coreRequest` is set, it **overrides** `spec.server.template.spec.containers[name=spark-kubernetes-driver].resources.requests.cpu`. Other resource keys on the template (`memory`, `ephemeral-storage`, etc.) are preserved.
- If `spec.server.coreLimit` is set, it **overrides** `spec.server.template.spec.containers[name=spark-kubernetes-driver].resources.limits.cpu`. Other resource keys on the template are preserved.
- If neither is set, the template's CPU values (if any) are preserved as-is.

This rule is deterministic and only applies to the server (since the operator creates that pod). Executor pods never see the template; their CPU comes from `spark.kubernetes.executor.*` and the executor pod template is applied by Spark.

### Validation

The admission webhook validates that every `coreRequest` and `coreLimit` is a non-empty, non-zero, non-negative Kubernetes CPU resource quantity that `resource.ParseQuantity` accepts. Examples of accepted values: `500m`, `1`, `1.5`, `0.5`, `8.5`. Examples of rejected values: `""`, `"0"`, `"0m"`, `"-500m"`, `"500x"`, `"1a0"`.

When both `coreRequest` and `coreLimit` are set on the same side (server or executor), the webhook additionally enforces `coreRequest <= coreLimit`.

### Example: server CPU resources

```yaml
spec:
  server:
    cores: 1
    coreRequest: "500m"
    coreLimit: "1"
    memory: 1g
    template:
      spec:
        containers:
        - name: spark-kubernetes-driver
          resources:
            requests:
              cpu: 100m       # overridden by spec.server.coreRequest
              memory: 1Gi     # preserved
            limits:
              cpu: 100m       # overridden by spec.server.coreLimit
              memory: 1Gi     # preserved
```

In this example, the resulting server pod has `resources.requests.cpu = 500m`, `resources.limits.cpu = 1`, and `resources.requests.memory = 1Gi` / `resources.limits.memory = 1Gi`.

### Example: executor CPU resources

```yaml
spec:
  executor:
    instances: 2
    cores: 1
    coreRequest: "500m"
    coreLimit: "1500m"
    memory: 512m
```

These are mapped to `spark.kubernetes.executor.request.cores=500m` and `spark.kubernetes.executor.limit.cores=1500m` and are applied by Spark when it creates the executor pods.

## Connecting a Spark Client

Once the server pod is ready, the operator exposes it via a `<name>-server` `Service` on port `15002`. A Spark client can connect to it with:

```bash
${SPARK_HOME}/bin/spark-shell --remote sc://spark-connect-server.default.svc:15002
```

## Dynamic Allocation

Dynamic allocation is configured via `spec.dynamicAllocation`. It is supported in Spark 3.0+ and lets Spark scale the executor pool up and down based on workload. See the [Apache Spark documentation](https://spark.apache.org/docs/latest/configuration.html#dynamic-allocation) for semantics.

```yaml
spec:
  dynamicAllocation:
    enabled: true
    initialExecutors: 2
    minExecutors: 1
    maxExecutors: 10
    shuffleTrackingEnabled: true
    shuffleTrackingTimeout: 60
```

## Field Reference

For the full API definition, including all fields, see the [`SparkConnect` API reference](../reference/api-docs.md).
