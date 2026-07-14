# Getting Started

This guide walks you through installing the Spark Operator with Helm and running your first
`SparkApplication`. The operator runs on **any conformant Kubernetes cluster** — there is no
dependency on a specific cloud or distribution. Once you have a cluster, the steps below are the
same whether you run on a managed service (EKS, GKE, AKS, OpenShift), a self-managed cluster
(kubeadm, k3s, RKE2), or a local cluster for development (kind, minikube, Docker Desktop).

For a deeper look at how to write, compose, and operate `SparkApplication`s, see the
[User Guide](../user-guide/index.md). Throughout this guide, the Kubernetes Operator for Apache
Spark is referred to simply as *the operator*.

## Prerequisites

- **A running Kubernetes cluster (v1.16 or later).** Bring your own cluster on any distribution.
  If you don't have one yet, create it first with the tooling for your environment — for example,
  `kind create cluster` or `minikube start` for local testing, or `eksctl` / `gcloud` / `az` for a
  managed cluster.
- **`kubectl`**, configured to point at that cluster. Verify access with `kubectl cluster-info`.
- **Helm 3 or later.** See the [Helm installation guide](https://helm.sh/docs/intro/install/).

## Installation

### Add Helm Repo

```shell
helm repo add spark-operator https://kubeflow.github.io/spark-operator

helm repo update
```

See [helm repo](https://helm.sh/docs/helm/helm_repo) for command documentation.

### Install the chart

```shell
helm install [RELEASE_NAME] spark-operator/spark-operator
```

For example, if you want to create a release with name `spark-operator` in the `spark-operator` namespace:

```shell
helm install spark-operator spark-operator/spark-operator \
    --namespace spark-operator \
    --create-namespace
```

See [helm install](https://helm.sh/docs/helm/helm_install) for command documentation.

The chart installs the CRDs and creates separate controller and webhook Deployments in the release namespace. It also creates operator RBAC and, by default, a Spark driver service account and RBAC in the `default` namespace. The validating and [mutating admission webhooks](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/) are enabled by default. The webhook manages an internal TLS certificate and stores it in a release-scoped Secret; set `certManager.enable=true` to use cert-manager instead.

To install without the webhook, set `webhook.enable=false`. Disabling it also disables admission defaulting, validation, and Spark pod customization, so the default installation is recommended.

If you want to deploy the chart to GKE cluster, you will first need to [grant yourself cluster-admin privileges](https://cloud.google.com/kubernetes-engine/docs/how-to/role-based-access-control#defining_permissions_in_a_role) before you can create custom roles and role bindings on a GKE cluster versioned 1.6 and up. Run the following command before installing the chart on GKE:

```shell
kubectl create clusterrolebinding <user>-cluster-admin-binding --clusterrole=cluster-admin --user=<user>@<domain>
```

Now you should see the operator running in the cluster by checking the status of the Helm release.

```shell
helm status --namespace spark-operator my-release
```

### Upgrade the Chart

```shell
helm upgrade [RELEASE_NAME] spark-operator/spark-operator [flags]
```

See [helm upgrade](https://helm.sh/docs/helm/helm_upgrade) for command documentation.

### Uninstall the Chart

```shell
helm uninstall [RELEASE_NAME]
```

This removes all the Kubernetes resources associated with the chart and deletes the release, except for the `crds`, those will have to be removed manually.

See [helm uninstall](https://helm.sh/docs/helm/helm_uninstall) for command documentation.

### Additional Steps to Integrate Jupyter Notebooks

Integrating Jupyter Notebooks with the Spark Operator to run big data or distributed machine learning jobs with PySpark.

See [Integration with Notebooks](../user-guide/notebooks-spark-operator.md) for further details.

## Running the Examples

To run the Spark PI example, run the following command:

```shell
kubectl apply -f examples/spark-pi.yaml
```

Note that `spark-pi.yaml` configures the driver pod to use the `spark` service account to communicate with the Kubernetes API server. You might need to replace it with the appropriate service account before submitting the job. If you installed the operator using the Helm chart and overrode `spark.jobNamespaces`, the service account name ends with `-spark` and starts with the Helm release name. For example, if you would like to run your Spark jobs to run in a namespace called `test-ns`, first make sure it already exists, and then install the chart with the command:

```shell
helm install my-release spark-operator/spark-operator --namespace spark-operator --set "spark.jobNamespaces={test-ns}"
```

Then the chart will set up a service account for your Spark jobs to use in that namespace.

See the section on the [Spark Job Namespace](#about-spark-job-namespaces) for details on the behavior of the default Spark Job Namespace.

Running the above command will create a `SparkApplication` object named `spark-pi`. Check the object by running the following command:

```shell
kubectl get sparkapplication spark-pi -o=yaml
```

This will show something similar to the following:

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  ...
spec:
  deps: {}
  driver:
    coreLimit: 1200m
    cores: 1
    labels:
      version: 2.3.0
    memory: 512m
    serviceAccount: spark
  executor:
    cores: 1
    instances: 1
    labels:
      version: 2.3.0
    memory: 512m
  image: gcr.io/ynli-k8s/spark:v3.1.1
  mainApplicationFile: local:///opt/spark/examples/jars/spark-examples_2.12-3.1.1.jar
  mainClass: org.apache.spark.examples.SparkPi
  mode: cluster
  restartPolicy:
      type: OnFailure
      onFailureRetries: 3
      onFailureRetryInterval: 10
      onSubmissionFailureRetries: 5
      onSubmissionFailureRetryInterval: 20
  type: Scala
status:
  sparkApplicationId: spark-5f4ba921c85ff3f1cb04bef324f9154c9
  applicationState:
    state: COMPLETED
  completionTime: 2018-02-20T23:33:55Z
  driverInfo:
    podName: spark-pi-83ba921c85ff3f1cb04bef324f9154c9-driver
    webUIAddress: 35.192.234.248:31064
    webUIPort: 31064
    webUIServiceName: spark-pi-2402118027-ui-svc
    webUIIngressName: spark-pi-ui-ingress
    webUIIngressAddress: spark-pi.ingress.cluster.com
  executorState:
    spark-pi-83ba921c85ff3f1cb04bef324f9154c9-exec-1: COMPLETED
  LastSubmissionAttemptTime: 2018-02-20T23:32:27Z
```

To check events for the `SparkApplication` object, run the following command:

```shell
kubectl describe sparkapplication spark-pi
```

This will show the events similarly to the following:

```text
Events:
  Type    Reason                      Age   From            Message
  ----    ------                      ----  ----            -------
  Normal  SparkApplicationAdded       5m    spark-operator  SparkApplication spark-pi was added, enqueued it for submission
  Normal  SparkApplicationTerminated  4m    spark-operator  SparkApplication spark-pi terminated with state: COMPLETED
```

The operator submits the Spark Pi example to run once it receives an event indicating the `SparkApplication` object was added.

## Configuration

The Helm chart runs the operator as two components:

- The **controller** reconciles `SparkApplication`, `ScheduledSparkApplication`, and `SparkConnect` resources and submits Spark applications.
- The **webhook** performs API defaulting and validation and mutates Spark driver and executor pods.

Configure these components with:

| Helm value | Purpose | Default |
| --- | --- | --- |
| `controller.workers` | Concurrent controller workers | `10` |
| `controller.uiService.enable` | Create Spark UI Services | `true` |
| `spark.jobNamespaces` | Explicit namespaces containing Spark workloads | `[default]` |
| `spark.jobNamespaceSelector` | Select additional workload namespaces by label | `""` |
| `webhook.enable` | Deploy admission webhooks | `true` |
| `webhook.port` | Webhook server port | `9443` |
| `prometheus.metrics.enable` | Serve operator metrics | `true` |
| `prometheus.metrics.port` | Operator metrics port | `8080` |
| `prometheus.podMonitor.create` | Create a Prometheus Operator `PodMonitor` | `false` |

For example, the following limits the operator to the `analytics` namespace and creates a `PodMonitor`:

```shell
helm upgrade --install spark-operator spark-operator/spark-operator \
    --namespace spark-operator \
    --create-namespace \
    --set 'spark.jobNamespaces={analytics}' \
    --set prometheus.podMonitor.create=true
```

The standalone binary exposes separate command trees for the two components. Run `spark-operator controller start --help` or `spark-operator webhook start --help` for their current flags.

### CRD upgrades

Helm does not automatically upgrade those CRDs. For upgrades that include CRD changes, enable the chart's pre-upgrade hook:

```shell
helm upgrade spark-operator spark-operator/spark-operator \
    --namespace spark-operator \
    --set hook.upgradeCrd=true
```

Review release notes before upgrading and keep CRDs when uninstalling unless you intend to delete all custom resources.

## About Spark Job Namespaces

The Spark Job Namespaces value defines the namespaces where `SparkApplications` can be deployed. The Helm chart value for the Spark Job Namespaces is `spark.jobNamespaces`, and its default value is `[]`. When the list of namespaces is empty the Helm chart will create a service account in the namespace where the spark-operator is deployed.

If you installed the operator using the Helm chart and overrode the `spark.jobNamespaces` to some other, pre-existing namespace, the Helm chart will create the necessary service account and RBAC in the specified namespace.

The Spark Operator uses the Spark Job Namespace to identify and filter relevant events for the `SparkApplication` CRD. If you specify a namespace for Spark Jobs, and then submit a SparkApplication resource to another namespace, the Spark Operator will filter out the event, and the resource will not get deployed. If you don't specify a namespace, the Spark Operator will see only `SparkApplication` events for the Spark Operator namespace.

## About the Service Account for Driver Pods

A Spark driver pod need a Kubernetes service account in the pod's namespace that has permissions to create, get, list, and delete executor pods, and create a Kubernetes headless service for the driver. The driver will fail and exit without the service account, unless the default service account in the pod's namespace has the needed permissions. To submit and run a `SparkApplication` in a namespace, please make sure there is a service account with the permissions in the namespace and set `.spec.driver.serviceAccount` to the name of the service account. Please refer to [spark-rbac.yaml](https://github.com/kubeflow/spark-operator/blob/master/config/spark-rbac/spark-application-rbac.yaml) for an example RBAC setup that creates a driver service account named `spark-operator-spark` in the `default` namespace, with a RBAC role binding giving the service account the needed permissions.

## About the Service Account for Executor Pods

A Spark executor pod may be configured with a Kubernetes service account in the pod namespace. To submit and run a `SparkApplication` in a namespace, please make sure there is a service account with the permissions required in the namespace and set `.spec.executor.serviceAccount` to the name of the service account.

## Enable Metric Exporting to Prometheus

The controller and webhook expose metrics for Prometheus by default. Configure the endpoint with `prometheus.metrics.*`. If the Prometheus Operator is installed, set `prometheus.podMonitor.create=true` to create a `PodMonitor`. To disable operator metrics, set the following value during installation or upgrade:

```shell
helm upgrade --install spark-operator spark-operator/spark-operator \
    --namespace spark-operator \
    --create-namespace \
    --set prometheus.metrics.enable=false
```

If enabled, the operator generates the following metrics:

### Spark Application Metrics

| Metric | Description |
| ------------- | ------------- |
| `spark_application_count`  | Total number of SparkApplication handled by the Operator.|
| `spark_application_submit_count`  | Total number of SparkApplication spark-submitted by the Operator.|
| `spark_application_success_count` | Total number of SparkApplication which completed successfully.|
| `spark_application_failure_count` | Total number of SparkApplication which failed to complete. |
| `spark_application_running_count` | Total number of SparkApplication which are currently running.|
| `spark_application_success_execution_time_seconds` | Execution time for applications which succeeded.|
| `spark_application_failure_execution_time_seconds` | Execution time for applications which failed. |
| `spark_application_start_latency_seconds` | Start latency of SparkApplication as type of [Prometheus Summary](https://prometheus.io/docs/concepts/metric_types/#summary). |
| `spark_application_start_latency_seconds` | Start latency of SparkApplication as type of [Prometheus Histogram](https://prometheus.io/docs/concepts/metric_types/#histogram). |
| `spark_executor_success_count` | Total number of Spark Executors which completed successfully. |
| `spark_executor_failure_count` | Total number of Spark Executors which failed. |
| `spark_executor_running_count` | Total number of Spark Executors which are currently running. |

#### Work Queue Metrics

| Metric | Description |
| ------------- | ------------- |
| `workqueue_depth` | Current depth of workqueue |
| `workqueue_adds_total` | Total number of adds handled by workqueue |
| `workqueue_queue_duration_seconds_bucket` | How long in seconds an item stays in workqueue before being requested |
| `workqueue_work_duration_seconds_bucket` | How long in seconds processing an item from workqueue takes |
| `workqueue_retries_total` | Total number of retries handled by workqueue |
| `workqueue_unfinished_work_seconds` | Unfinished work in seconds |
| `workqueue_longest_running_processor_seconds` | Longest running processor in seconds |

The corresponding Helm values are `prometheus.metrics.enable`, `port`, `endpoint`, `prefix`, and `labels`. When running either component directly, use `--enable-metrics=true`, `--metrics-bind-address=:8080`, `--metrics-endpoint=/metrics`, `--metrics-prefix=<prefix>`, and `--metrics-labels=<comma-separated-labels>`.

A note about `metrics-labels`: In `Prometheus`, every unique combination of key-value label pairs represents a new time series, which can dramatically increase the amount of data stored. Hence, labels should not be used to store dimensions with high cardinality with potentially a large or unbounded value range.

Additionally, these metrics are best-effort for the current operator run and will be reset on an operator restart. Also, some of these metrics are generated by listening to pod state updates for the driver/executors and deleting the pods outside the operator might lead to incorrect metric values for some of these metrics.

## Driver UI Access and Ingress

The operator, by default, makes the Spark UI accessible by creating a service of type `ClusterIP` which exposes the UI. This is only accessible from within the cluster.

The operator also supports creating an optional Ingress for the UI. This can be turned on by setting the `ingress-url-format` command-line flag. The `ingress-url-format` should be a template like `{{$appName}}.{ingress_suffix}/{{$appNamespace}}/{{$appName}}`. The `{ingress_suffix}` should be replaced by the user to indicate the cluster's ingress url and the operator will replace the `{{$appName}}` & `{{$appNamespace}}` with the appropriate value. Please note that Ingress support requires that cluster's ingress url routing is correctly set-up. For e.g. if the `ingress-url-format` is `{{$appName}}.ingress.cluster.com`, it requires that anything `*ingress.cluster.com` should be routed to the ingress-controller on the K8s cluster.

The operator also sets both `WebUIAddress` which is accessible from within the cluster as well as `WebUIIngressAddress` as part of the `DriverInfo` field of the `SparkApplication`.

The operator generates ingress resources intended for use with the [Ingress NGINX Controller](https://kubernetes.github.io/ingress-nginx/). Include this in your application spec for the controller to ensure it recognizes the ingress and provides appropriate routes to your Spark UI.

```yaml
spec:
  sparkUIOptions:
    ingressAnnotations:
        kubernetes.io/ingress.class: nginx
```

## About the Admission Webhooks

The Helm chart deploys a separate webhook server by default. It provides:

- Defaulting and validation for Spark Operator custom resources.
- Spark driver and executor pod customization, including volumes, ConfigMaps, affinity, tolerations, sidecars, and init containers.

The webhook listens on port `9443` by default. Without cert-manager, it generates and rotates an internal CA and serving certificate and synchronizes the release-scoped TLS Secret and webhook configurations. To use cert-manager instead, install cert-manager first and set:

```shell
helm upgrade --install spark-operator spark-operator/spark-operator \
    --namespace spark-operator \
    --create-namespace \
    --set certManager.enable=true
```

### Private clusters

The Kubernetes API server must be able to reach the webhook workload on `webhook.port` (default `9443`). Private GKE, EKS, and other restricted clusters may require a firewall or security-group rule permitting control-plane traffic to that port. Prefer allowing port `9443`; changing the container to a privileged port such as `443` also requires an appropriate pod security context and is not necessary when the network permits `9443`.

After installation, verify both Deployments before creating applications:

```shell
kubectl --namespace spark-operator get deployments
kubectl --namespace spark-operator rollout status deployment/spark-operator-controller
kubectl --namespace spark-operator rollout status deployment/spark-operator-webhook
```

Deployment names include the Helm release name and may differ if `nameOverride` or `fullnameOverride` is set.
