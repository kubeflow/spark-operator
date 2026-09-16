# API Reference

The Kubeflow Spark Operator defines three custom resources in the
`sparkoperator.k8s.io` API group:

- **`SparkApplication`** (`v1beta2`) — a single Spark application submitted to the cluster.
- **`ScheduledSparkApplication`** (`v1beta2`) — a Spark application run on a cron schedule.
- **`SparkConnect`** (`v1alpha1`) — a long-running Spark Connect server that clients
  connect to instead of submitting a self-contained application.

:::{warning}
`SparkConnect` uses the `sparkoperator.k8s.io/v1alpha1` API. An alpha API can change in
a future release.
:::

The complete, auto-generated API definition (every field, type, and description for
the CRDs) is maintained alongside the source code and regenerated whenever the API
types change.

:::{admonition} Full API definition
:class: note

The full API reference is generated from the Go types and published in the repository:

- [**Spark Operator API definition**](https://github.com/kubeflow/spark-operator/blob/master/docs/api-docs.md)

:::

For a worked `SparkConnect` example, see the
[Spark Connect user guide](../user-guide/spark-connect.md).

## Regenerating the API docs

The reference is produced from the API types under `api/` using the project's
`Makefile`:

```bash
make build-api-docs
```

This writes the generated reference to `docs/api-docs.md`. See the
[Contributor Guide](../contributor-guide/index.md) for the full development workflow.
