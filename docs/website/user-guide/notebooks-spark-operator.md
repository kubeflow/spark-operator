# Integration with Kubeflow Notebooks

If you work in Kubeflow Notebooks and want to run distributed PySpark, the
[Kubeflow SDK](https://sdk.kubeflow.org/en/latest/spark/index.html) talks to the Spark
Operator directly. You write a few lines of Python in your notebook and get back an
ordinary `SparkSession` — no `SparkApplication` manifests, and no gateway component in
between.

```{image} notebooks-spark-connect.svg
:alt: Two users each running an isolated Spark cluster in their own namespace, with the Spark Operator creating driver pods and the notebook connecting directly to the driver over gRPC
:width: 100%
```

## How it works

1. Your notebook calls `SparkClient().connect()`.
2. The SDK creates a `SparkConnect` custom resource in your namespace.
3. The Spark Operator provisions the driver and executor pods.
4. The SDK returns a connected `SparkSession` pointed at the Spark Connect server.

The session is a normal Spark Connect client, so everything on the
[Using Spark Connect](spark-connect.md) page applies.

## Prerequisites

- Spark Operator installed, with the `SparkConnect` CRD and the operator watching your
  user namespaces
- A notebook image with the Kubeflow SDK installed (`kubeflow[spark]`)
- Permission to create `SparkConnect` resources in your namespace

## Getting started

Install the SDK, then pin the client to the Spark version it provisions. The `spark` extra
installs a `pyspark-connect` release that does not currently match
`DEFAULT_SPARK_VERSION`, so the second step is required:

```bash
pip install "kubeflow[spark]"
SPARK_VERSION="$(python -c \
  'from kubeflow.spark.backends.kubernetes import constants; print(constants.DEFAULT_SPARK_VERSION)')"
pip install "pyspark-connect==${SPARK_VERSION}"
```

Then, from the notebook:

```python
from kubeflow.common.types import KubernetesBackendConfig
from kubeflow.spark import Name, SparkClient

client = SparkClient(backend_config=KubernetesBackendConfig(namespace="my-namespace"))
spark =