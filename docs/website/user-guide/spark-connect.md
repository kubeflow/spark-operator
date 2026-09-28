# Using Spark Connect

[Apache Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html)
uses a client-server architecture that lets applications submit DataFrame
operations to a remote Spark server. The Spark Operator manages that server on
Kubernetes through the `SparkConnect` custom resource. For each resource, the
operator creates a long-running server pod and a service for client
connections. The server then asks Kubernetes to create the requested executor
pods.

:::{warning}
`SparkConnect` uses the `sparkoperator.k8s.io/v1alpha1` API. An alpha API can
change in a future release.
:::

## Prerequisites

Before creating a Spark Connect server, make sure that:

- the Spark Operator and the `SparkConnect` CRD are installed;
- `kubectl` is configured for the target cluster;
- the target namespace contains a service account that can create and manage
  executor pods and services; and
- the server image contains Spark Connect and matches the Spark version used by
  the client.

The canonical example runs in the `default` namespace and uses the
`spark-operator-spark` service account. The Helm installation can create this
service account in configured Spark job namespaces. See
[Getting Started](../getting-started/index.md) for installation and namespace
configuration.

## Create a Spark Connect server

Download or check out the Spark Operator source that matches your installed
operator version. From the root of that checkout, apply the
[`SparkConnect` example](https://github.com/kubeflow/spark-operator/blob/master/examples/sparkconnect/spark-connect.yaml):

```shell
kubectl apply -f examples/sparkconnect/spark-connect.yaml
```

The example creates a resource named `spark-connect` with:

- Spark version `4.0.4` and matching server and executor images;
- a server pod that uses the `spark-operator-spark` service account; and
- two executors with one core and `512m` of memory each.

The server and executor pod templates also set a restricted security context.
Adapt the image, service account, resources, and security context to your
cluster before using the manifest in production.

## Verify the deployment

Inspect the resource and the objects associated with it:

```shell
kubectl get sparkconnect spark-connect
kubectl get pods -l sparkoperator.k8s.io/connect-name=spark-connect
kubectl get service spark-connect-server
```

The `STATUS` column reports `Ready` when the server pod is ready and `NotReady`
while it is unavailable. The operator creates a server pod named
`spark-connect-server` and a service with the same name. The service exposes
the Spark Connect gRPC endpoint on port `15002`. The alpha API also defines
`Provisioning` and `Failed` states for lifecycle reporting.

Use the resource status and server logs for more detail:

```shell
kubectl get sparkconnect spark-connect -o yaml
kubectl describe pod spark-connect-server
kubectl logs pod/spark-connect-server -c spark-kubernetes-driver
```

## Connect a client

Clients inside the cluster can use the service DNS name. For the canonical
example in the `default` namespace, the endpoint is:

```text
sc://spark-connect-server.default.svc.cluster.local:15002
```

For local development, forward the service port to your workstation:

```shell
kubectl port-forward service/spark-connect-server 15002:15002
```

Keep the port-forward process running, then connect from a PySpark environment
whose Spark version matches the server:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
spark.range(10).show()
spark.stop()
```

The default service type is `ClusterIP`. If you customize the service for
access outside the cluster, protect it with network controls, transport
security, and an authenticating proxy. Spark Connect does not provide built-in
authentication.

## Configure the server and executors

The main `SparkConnect` fields are:

| Field | Purpose |
| --- | --- |
| `.spec.sparkVersion` | Declares the Spark version used by the server. |
| `.spec.image` | Sets a common server and executor image when templates do not provide component-specific images. |
| `.spec.server.cores` and `.spec.server.memory` | Set the server pod's Spark CPU and memory configuration. |
| `.spec.server.template` | Customizes the server pod, including its image, service account, volumes, and security context. If no service account is set here, the operator falls back to the `--default-service-account` controller flag (`controller.defaultServiceAccount` in the Helm chart). |
| `.spec.executor.instances` | Sets the number of static executors. |
| `.spec.executor.cores` and `.spec.executor.memory` | Set each executor's Spark CPU and memory configuration. |
| `.spec.server.gpu` and `.spec.executor.gpu` | Request GPUs for the server or each executor and configure Spark's GPU resource amount and vendor. |
| `.spec.executor.template` | Customizes executor pods, including their image, volumes, and security context. |
| `.spec.dynamicAllocation` | Enables dynamic allocation and configures the initial, minimum, and maximum executor counts. |
| `.spec.sparkConf` | Passes Spark configuration properties to the server. |
| `.spec.hadoopConf` | Passes Hadoop properties; the operator adds the `spark.hadoop.` prefix when it is omitted. |
| `.spec.server.service` | Customizes service metadata and supported service settings. The operator controls its namespace, selectors, and required ports. |

An image set on the named container in a server or executor pod template takes
precedence over `.spec.image` for that component. Server and executor pod
templates require Spark 3.0 or later.

For all available fields and validation rules, inspect the installed CRD:

```shell
kubectl explain sparkconnect.spec
kubectl explain sparkconnect.spec.server
kubectl explain sparkconnect.spec.executor
```

## Request GPUs

Set `.spec.executor.gpu` to run GPU workloads on executors. Set
`.spec.server.gpu` only when the Connect server itself also needs a GPU.
Each GPU specification requires a `name` in the form `<vendor-domain>/gpu`
(for example, `nvidia.com/gpu` or `amd.com/gpu`) and a positive integer
`quantity`. This typed field maps the vendor portion to
`spark.{driver,executor}.resource.gpu.vendor`, so the name must contain a
`/gpu` suffix and a valid DNS subdomain as the vendor.

The operator passes `spark.executor.resource.gpu.amount` and
`spark.executor.resource.gpu.vendor` to Spark, which translates them into GPU
limits on the executor pods. Server GPUs use the corresponding
`spark.driver.resource.gpu.*` properties, and the operator also sets matching
GPU requests and limits on the server container. These typed settings take
precedence over the amount/vendor settings in `sparkConf`. Other container
resources and sidecars in the pod templates are preserved.

GPU resource names whose suffix is not `gpu` (for example, MIG profiles) are
not supported by this field. Configure them through the pod template and
`sparkConf` instead.

:::{note}
GPU settings for the server pod are applied only when the server pod is first
created. If you edit `.spec.server.gpu` on an already-running `SparkConnect`,
the change does not take effect until the server pod is recreated. To apply
updated GPU settings, delete the `SparkConnect` and create a new one, or
delete the server pod directly and let the operator recreate it. Recreating
the server pod interrupts all active client sessions; clients must reconnect
after the new pod becomes ready.
:::

Your cluster must have GPU nodes and the appropriate Kubernetes device plugin.
Use a Spark image with the GPU libraries needed by your workload and an
executable discovery script that reports the GPUs allocated to its container.
Spark cannot start a GPU-enabled server or executor without a way to discover
its GPUs, so the operator rejects a `SparkConnect` that sets `.spec.server.gpu`
or `.spec.executor.gpu` without the matching discovery configuration in
`sparkConf`: `spark.driver.resource.gpu.discoveryScript` for the server,
`spark.executor.resource.gpu.discoveryScript` for executors, or
`spark.resources.discoveryPlugin` for both. Configure discovery and per-task
GPU use in `sparkConf`, for example:

```yaml
apiVersion: sparkoperator.k8s.io/v1alpha1
kind: SparkConnect
metadata:
  name: spark-connect-gpu
spec:
  sparkVersion: "4.0.4"
  image: your-registry/spark:4.0.4-gpu
  server:
    template:
      spec:
        serviceAccountName: spark-operator-spark
  executor:
    instances: 2
    cores: 4
    memory: "4g"
    gpu:
      name: nvidia.com/gpu
      quantity: 1
  sparkConf:
    spark.executor.resource.gpu.discoveryScript: /opt/spark/scripts/getGpusResources.sh
    spark.task.resource.gpu.amount: "0.25"
```

Replace the image and script path with those provided by your environment.
This example requests one GPU per executor and lets each task use a quarter
of a GPU; the server requests no GPU. If you also configure `server.gpu`,
provide driver discovery through
`spark.driver.resource.gpu.discoveryScript` or a resource discovery plugin.
GPU allocation alone does not enable SQL/DataFrame acceleration: configure
the RAPIDS plugin separately if your workload needs it. See Spark's
[resource allocation documentation](https://spark.apache.org/docs/latest/running-on-kubernetes.html#resource-allocation-and-configuration-overview)
for discovery scripts and scheduling requirements.

## Troubleshoot

If the resource does not reach `Ready`:

1. Inspect `.status.conditions` with `kubectl get sparkconnect spark-connect -o yaml`.
2. Describe the server pod and review its container logs for image, startup, or
   Spark configuration errors.
3. Confirm that the server pod's service account can create executor pods and
   the Kubernetes resources required by Spark.
4. Verify that the image contains `${SPARK_HOME}/sbin/start-connect-server.sh`.
5. Confirm that the service has an endpoint and that port `15002` is reachable
   from the client.
6. Use compatible Spark versions on the client and server.

If you deploy the resource outside `default`, add `--namespace <namespace>` to
the commands in this guide and use that namespace in the service DNS name.

## Delete the server

Delete the custom resource when it is no longer needed:

```shell
kubectl delete sparkconnect spark-connect
```

Kubernetes garbage collection removes the server pod, service, and other
resources owned by the `SparkConnect` object.
