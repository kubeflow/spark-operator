# Integrate the REST Submitter

By default, the Spark Operator invokes the `spark-submit` CLI for each Spark application
submission. Each invocation starts two JVM processes in the controller pod. Those processes use
the pod's limited CPU and memory, so a burst of applications can increase submission latency.

With REST submission, the operator sends a request for each application to a separate service.
The service creates the driver pod through the Kubernetes API without starting `spark-submit` in
the controller pod. This avoids CLI startup overhead and can start applications faster. The service
can also scale horizontally to handle bursts, leaving more controller resources for
`SparkApplication` lifecycle management. For more details about the service, see the
[submitter documentation](https://github.com/venkomirisetti/k8s-spark-submitter/blob/main/README.md).

This guide walks you through deploying the REST submitter service and integrating it with the Spark
Operator.

```{note}
REST submission is an alpha feature. Enable the `RestSubmitter` feature gate to use it.
```

## Prerequisites

Complete the [Getting Started prerequisites](../getting-started/index.md#prerequisites). If the
operator is already installed, have its Helm release name and current values file ready. You can
also enable REST submission when installing the operator for the first time.

## Installation

In these examples, each Helm release has the same name as its namespace: `spark-operator` or
`spark-submitter`. Adjust the names to match your installation.

### Deploy the submitter service

The chart installs a submitter Deployment, Service, ServiceAccount, and RBAC. It starts one pod by
default. Increase `replicas` and enable a PodDisruptionBudget if you need multiple pods.

Clone the [submitter repository](https://github.com/venkomirisetti/k8s-spark-submitter) to get its
Helm chart. Run the remaining commands from the clone's parent directory:

```shell
git clone https://github.com/venkomirisetti/k8s-spark-submitter.git
```

Save the following as `submitter-values.yaml`:

```yaml
image:
  tag: "<submitter-image-tag>"

volumeMounts:
  - name: submitter-tmp
    mountPath: /tmp
volumes:
  - name: submitter-tmp
    emptyDir: {}
```

Set the values for your installation:

| Value | What to set |
| --- | --- |
| `image.tag` | Choose a tag from the [submitter images](https://hub.docker.com/r/venkomirisetti/k8s-spark-submitter/tags) that matches the Spark version in your operator image and supports your cluster's node architecture. Set `image.registry` and `image.repository` if you use your own image. |
| `volumeMounts` and `volumes` | Keep the writable `/tmp` mount shown above. The submitter needs temporary storage, while the chart makes its root filesystem read only. |
| `jobNamespaces` (optional) | List the namespaces where the submitter may create driver resources. If omitted, the chart grants cluster-wide RBAC. See the [submitter RBAC settings](https://github.com/venkomirisetti/k8s-spark-submitter/blob/main/charts/spark-submitter/README.md#rbac). |

Install the chart:

```shell
helm install spark-submitter ./k8s-spark-submitter/charts/spark-submitter \
    --namespace spark-submitter \
    --create-namespace \
    --values submitter-values.yaml
```

For an existing submitter release, use `helm upgrade` instead. Check that the deployment is ready
before configuring the operator:

```shell
kubectl -n spark-submitter rollout status deployment/spark-submitter
```

### Enable REST submission in the operator

The operator needs the submitter's full Service URL. Add these settings to your operator values
file, called `operator-values.yaml` here. Keep any other enabled feature gates in
`controller.featureGates`:

```yaml
controller:
  featureGates:
    - name: RestSubmitter
      enabled: true

submitter:
  serviceUrl: http://spark-submitter-svc.spark-submitter.svc.cluster.local:8080/api/v1/spark-submit
```

Set `submitter.serviceUrl` to the full submit endpoint URL for your submitter Service. The
[operator chart values](https://github.com/kubeflow/spark-operator/blob/master/charts/spark-operator-chart/README.md)
list optional startup timeout, request timeout, and retry settings.

For an existing operator release, upgrade it with its complete values file:

```shell
helm upgrade spark-operator spark-operator/spark-operator \
    --namespace spark-operator \
    --values operator-values.yaml
```

For a new operator installation, follow [Getting Started](../getting-started/index.md#installation)
and pass `--values operator-values.yaml` to `helm install`. The controller must be able to reach the
submitter Service when it starts. Check the release and controller deployment:

```shell
helm status --namespace spark-operator spark-operator
kubectl -n spark-operator rollout status deployment/spark-operator-controller
kubectl -n spark-operator logs deployment/spark-operator-controller | \
    grep 'Using REST submitter service'
```

The log line confirms that the controller selected REST submission. To verify a submission, run a
`SparkApplication` as described in [Getting Started](../getting-started/index.md#running-the-examples)
and check its status. Application manifests and lifecycle management stay the same.

## Enable mTLS

By default, the operator and submitter communicate over HTTP. Both support mTLS, but the operator
chart does not expose the controller's TLS flags. To enable mTLS, mount certificates in both pods,
pass the TLS flags to the controller, and change `submitter.serviceUrl` to HTTPS.

### Required certificates

| Component | Certificate and key | Trusted CA |
| --- | --- | --- |
| REST submitter | Server certificate and private key | CA that signed the operator's client certificate |
| Operator | Client certificate and private key | CA that signed the submitter's server certificate |

The server certificate must include the DNS name in `submitter.serviceUrl`, such as
`spark-submitter-svc.spark-submitter.svc.cluster.local`. The server key must be in PKCS#8 PEM
format.

### Create and mount Kubernetes Secrets

Create `spark-submitter-tls` in the submitter namespace for the server certificate and
`spark-submitter-client-tls` in the operator namespace for the client certificate. If you use
cert-manager, add two `Certificate` resources with these Secret names, using the operator chart's
existing [webhook certificate configuration](https://github.com/kubeflow/spark-operator/tree/master/charts/spark-operator-chart/templates/certmanager)
as a reference. Set `spec.privateKey.encoding: PKCS8` for the server certificate. Otherwise, create
the Secrets from your certificate files using the commands below. Replace the `/path/to/` paths
with your certificate file paths:

```shell
kubectl -n spark-submitter create secret generic spark-submitter-tls \
    --from-file=tls.crt=/path/to/server.crt \
    --from-file=tls.key=/path/to/server.key \
    --from-file=ca.crt=/path/to/client-ca.crt
kubectl -n spark-operator create secret generic spark-submitter-client-tls \
    --from-file=tls.crt=/path/to/client.crt \
    --from-file=tls.key=/path/to/client.key \
    --from-file=ca.crt=/path/to/server-ca.crt
```

Mount the server Secret in the submitter pod. In `submitter-values.yaml`, keep the existing `/tmp`
mount:

```yaml
volumeMounts:
  - name: submitter-tmp
    mountPath: /tmp
  - name: submitter-tls
    mountPath: /etc/submitter-tls
    readOnly: true
volumes:
  - name: submitter-tmp
    emptyDir: {}
  - name: submitter-tls
    secret:
      secretName: spark-submitter-tls
```

Mount the client Secret in the controller pod. In `operator-values.yaml`, keep the existing `/tmp`
mount:

```yaml
controller:
  volumeMounts:
    - name: tmp
      mountPath: /tmp
      readOnly: false
    - name: submitter-client-tls
      mountPath: /etc/submitter-client-tls
      readOnly: true
  volumes:
    - name: tmp
      emptyDir:
        sizeLimit: 1Gi
    - name: submitter-client-tls
      secret:
        secretName: spark-submitter-client-tls
```

If the CA files are stored separately, mount them too and update the CA paths in the next step.

### Configure the submitter and operator

Set the submitter's certificate paths in `submitter-values.yaml`:

```yaml
tls:
  enabled: true
  certPath: /etc/submitter-tls/tls.crt
  keyPath: /etc/submitter-tls/tls.key
  caCertPath: /etc/submitter-tls/ca.crt
```

Configure the controller deployment to pass these arguments through your deployment tooling:

```text
--submitter-tls-enabled=true
--submitter-tls-cert-file=/etc/submitter-client-tls/tls.crt
--submitter-tls-key-file=/etc/submitter-client-tls/tls.key
--submitter-tls-ca-file=/etc/submitter-client-tls/ca.crt
```

Once the controller has these arguments, set `submitter.serviceUrl` in `operator-values.yaml` to
the HTTPS endpoint:

```yaml
submitter:
  serviceUrl: https://spark-submitter-svc.spark-submitter.svc.cluster.local:8080/api/v1/spark-submit
```

```{note}
If your environment already provides the certificates, skip Secret creation. If the files are
already mounted in the pods, skip the mount steps too. Set the paths above to the files in your
pods.
```

## Submitter metrics

The submitter service emits these metrics for Spark application submission requests:

| Metric | Description |
| --- | --- |
| `spark_submit_request_success_count` | Successful submission requests |
| `spark_submit_request_failure_count` | Failed submission requests |
| `spark_submit_requests_in_flight` | Submission requests currently being processed |
| `spark_submit_request_latency_seconds` | Submission request latency in seconds |

For Prometheus discovery through pod annotations, use these values in `submitter-values.yaml`:

```yaml
probePort: 8081

prometheus:
  metrics:
    enable: true
```

Set the Prometheus scrape target for each submitter pod to
`http://<submitter-pod-ip>:<probePort>/metrics`.
