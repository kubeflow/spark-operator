# SparkApplication API (v1beta2)

The Kubeflow Spark Operator defines two custom resources in the
`sparkoperator.k8s.io/v1beta2` API group:

- **`SparkApplication`** — a single Spark application submitted to the cluster.
- **`ScheduledSparkApplication`** — a Spark application run on a cron schedule.

The complete, auto-generated API definition (every field, type, and description for
the CRDs) is maintained alongside the source code and regenerated whenever the API
types change.

:::{admonition} Full API definition
:class: note

The full `v1beta2` API reference is generated from the Go types and published in the
repository:

- [**`SparkApplication` / `ScheduledSparkApplication` API definition**](https://github.com/kubeflow/spark-operator/blob/master/docs/api-docs.md)

:::

## Regenerating the API docs

The reference is produced from the API types under `api/v1beta2/` using the
project's `Makefile`:

```bash
make build-api-docs
```

This writes the generated reference to `docs/api-docs.md`. See the
[Contributor Guide](../contributor-guide/index.md) for the full development workflow.
