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
REST submission is an alpha feature. Enable the `RestSubmitter` feature gate to use it. Review
[Limitations](#limitations) before you enable it.
```

## Limitations

The submitter builds the driver pod with the Spark libraries in its own image, not the Spark in the
operator image or the application image. It also uses its own service account. Several differences
from `spark-submit` follow from this.

### The submitter's Spark version decides the driver pod

Spark settings that apply when the driver pod is created only work if the submitter's Spark
version supports them. Settings that the driver reads at runtime are not affected. For example, a
submitter built on Spark 4.0 ignores these settings for an application that runs Spark 4.3:

| Behavior | Added in | With a Spark 4.0 submitter |
| --- | --- | --- |
| `spark.kubernetes.driver.pod.excludedFeatureSteps` | 4.1 | Ignored |
| Per-application `NetworkPolicy` for executor pods | 4.2 | Not created |
| `spark.kubernetes.driver.securityContext.allowPrivilegeEscalation` (defaults to `false`) | 4.3 | Not set on the driver container |
| `spark.kubernetes.driver.service.publishNotReadyAddresses` | 4.3 | Ignored |

The driver pod's `spark-version` label also shows the submitter's Spark version.

Choose a submitter image whose Spark version matches the version your applications run. If your
applications run several Spark versions, the operator cannot route them to different submitters.
One operator sends every application to the single `submitter.serviceUrl`.

The published submitter images are built on Spark 4.0.1. The submitter source builds against Spark
4.1 and earlier only. Spark 4.2 moved to Jetty 12, so a submitter for Spark 4.2 or later needs
source changes in the submitter project before it can be built.

The published submitter images support `linux/amd64` only. On clusters with other node
architectures, set `nodeSelector` with `kubernetes.io/arch: amd64` in `submitter-values.yaml`.

### Extra RBAC for the submitter

The submitter chart grants its service account access to `pods`, `configmaps`, and `services` only.
Spark creates other resources with the submitter's service account:

| When | Resource |
| --- | --- |
| Every application, if the submitter is built on Spark 4.2 or later | `networkpolicies` (API group `networking.k8s.io`) |
| `OnDemand` persistent volume claims (`spark.kubernetes.driver.volumes.persistentVolumeClaim.*.options.claimName=OnDemand`) | `persistentvolumeclaims` |
| Kerberos delegation tokens | `secrets` |

Spark 4.2 and later create a `NetworkPolicy` for the executors of every application. Without the
`networkpolicies` rule, every submission fails with a submitter built on these versions.

Without access, the submission fails with a `forbidden` error after the driver pod is created. The
submitter then deletes the pod. It reports the failure as a retryable `INTERNAL_SERVER_ERROR`, so the
operator makes up to `--submitter-max-retries` attempts (3 by default) before it marks the
application failed. The `forbidden` message appears in the application's status. Grant the access
in each job namespace:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: spark-submitter-extra
  namespace: <job-namespace>
rules:
  - apiGroups: [""]
    resources: ["persistentvolumeclaims", "secrets"]
    verbs: ["get", "create", "patch", "delete"]
  - apiGroups: ["networking.k8s.io"]
    resources: ["networkpolicies"]
    verbs: ["get", "create", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: spark-submitter-extra
  namespace: <job-namespace>
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: spark-submitter-extra
subjects:
  - kind: ServiceAccount
    name: spark-submitter
    namespace: spark-submitter
```

Grant only the resources your applications use.

### Work that moves into the submitter pod

`spark-submit` work that does not create Kubernetes resources now runs in the shared submitter
process instead of a separate process per application:

- `--packages` (`.spec.deps.packages`) are resolved in the submitter pod. The pod needs access to the
  Maven repositories, and resolution time counts against `--submitter-request-timeout`.
- `file://` dependencies are read from the submitter pod's file system and uploaded to
  `spark.kubernetes.file.upload.path` with the submitter's credentials.
- Kerberos delegation tokens are obtained in the submitter process.
- `spark-defaults.conf` comes from the submitter image.

Prefer dependencies that are built into the application image or referenced with remote URIs.

### Other differences

- Only `cluster` deploy mode is supported.
- The controller does not start until the submitter is reachable, up to
  `--submitter-startup-timeout` (5 minutes by default).
- The submitter serves plain HTTP by default. If `jobNamespaces` is empty, the chart grants it
  cluster-wide access to create pods. Set `jobNamespaces`, [enable mTLS](#enable-mtls), and restrict
  ingress to the submitter Service to the operator.

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
