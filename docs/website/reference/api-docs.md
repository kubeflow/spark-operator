# API Reference

The Kubeflow Spark Operator defines three custom resources in the
`sparkoperator.k8s.io` API group, across two API versions:

- **`SparkApplication`** (`v1beta2`) — a single Spark application submitted to the cluster.
- **`ScheduledSparkApplication`** (`v1beta2`) — a Spark application run on a cron schedule.
- **`SparkConnect`** (`v1alpha1`) — a long-running Spark Connect server that clients
  connect to instead of submitting a self-contained application.

The complete, auto-generated API definition (every field, type, and description for
the CRDs) is maintained alongside the source code and regenerated whenever the API
types change.

:::{admonition} Full API definition
:class: note

The full `v1beta2` API reference is generated from the Go types and published in the
repository:

- [**`SparkApplication` / `ScheduledSparkApplication` API definition**](https://github.com/kubeflow/spark-operator/blob/master/docs/api-docs.md)

The generated reference covers the `v1beta2` types only. For `SparkConnect`, see the Go
types in
[`api/v1alpha1/sparkconnect_types.go`](https://github.com/kubeflow/spark-operator/blob/master/api/v1alpha1/sparkconnect_types.go).

:::

## SparkConnect (v1alpha1)

`SparkConnect` manages a Spark Connect server on Kubernetes. For each resource the
operator creates a long-running server pod and a service for client connections, and
the server then asks Kubernetes for the executor pods it needs.

| | |
|---|---|
| API version | `sparkoperator.k8s.io/v1alpha1` |
| Kind | `SparkConnect` |
| Resource | `sparkconnects` (short name `sparkconn`) |
| Scope | Namespaced |

:::{warning}
`SparkConnect` uses the `sparkoperator.k8s.io/v1alpha1` API. An alpha API can change in
a future release.
:::

See the [Spark Connect user guide](../user-guide/spark-connect.md) for a worked example.

## Regenerating the API docs

The reference is produced from the API types under `api/v1beta2/` using the
project's `Makefile`:

```bash
make build-api-docs
```

This writes the generated reference to `docs/api-docs.md`. See the
[Contributor Guide](../contributor-guide/index.md) for the full development workflow.
