# Quick Start Guide

For a more detailed guide on how to use, compose, and work with `SparkApplication`s, please refer to the
[User Guide](user-guide.md). If you are running the Kubernetes Operator for Apache Spark on Google Kubernetes Engine and want to use Google Cloud Storage (GCS) and/or BigQuery for reading/writing data, also refer to the [GCP guide](gcp.md). The Kubernetes Operator for Apache Spark will simply be referred to as the operator for the rest of this guide.

## Table of Contents
- [Quick Start Guide](#quick-start-guide)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
  - [Running the Examples](#running-the-examples)
  - [Configuration](#configuration)
  - [Upgrade](#upgrade)
  - [About the Spark Job Namespace](#about-the-spark-job-namespace)
  - [About the Service Account for Driver Pods](#about-the-service-account-for-driver-pods)
  - [About the Service Account for Executor Pods](#about-the-service-account-for-executor-pods)
  - [Enable Metric Exporting to Prometheus](#enable-metric-exporting-to-prometheus)
      - [Spark Application Metrics](#spark-application-metrics)
      - [Work Queue Metrics](#work-queue-metrics)
  - [Driver UI Access and Ingress](#driver-ui-access-and-ingress)
  - [About the Mutating Admission Webhook](#about-the-mutating-admission-webhook)
    - [Mutating Admission Webhooks on a private GKE cluster](#mutating-admission-webhooks-on-a-private-gke-cluster)

## Installation

To install the operator, use the Helm [chart](../charts/spark-operator-chart).

```bash
$ helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator

$ helm install my-release spark-operator/spark-operator --namespace spark-operator --create-namespace
```

Installing the chart will create a namespace `spark-operator` if it doesn't exist, and helm will set up RBAC for the operator to run in the namespace. It will also set up RBAC in the `default` namespace for driver pods of your Spark applications to be able to manipulate executor pods. In addition, the chart will create a Deployment in the namespace `spark-operator`. The chart's [Spark Job Namespace](#about-the-spark-job-namespace) is set to `release namespace` by default. The chart by default does not enable [Mutating Admission Webhook](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/) for Spark pod customization. When enabled, a webhook service and a secret storing the x509 certificate called `spark-webhook-certs` are created for that purpose. To install the operator **with** the mutating admission webhook on a Kubernetes cluster, install the chart with the flag `webhook.enable=true`:

```bash
$ helm install my-release spark-operator/spark-operator --namespace spark-operator --set webhook.enable=true
```

Due to a [known issue](https://cloud.google.com/kubernetes-engine/docs/how-to/role-based-access-control#defining_permissions_in_a_role) in GKE, you will need to first grant yourself cluster-admin privileges before you can create custom roles and role bindings on a GKE cluster versioned 1.6 and up. Run the following command before installing the chart on GKE:

```bash
$ kubectl create clusterrolebinding <user>-cluster-admin-binding --clusterrole=cluster-admin --user=<user>@<domain>
```

Now you should see the operator running in the cluster by checking the status of the Helm release.

```bash
$ helm status --namespace spark-operator my-release
```

## Running the Examples

To run the Spark Pi example, run the following command:

```bash
$ kubectl apply -f examples/spark-pi.yaml
```

Note that `spark-pi.yaml` configures the driver pod to use the `spark` service account to communicate with the Kubernetes API server. You might need to replace it with the appropriate service account before submitting the job. If you installed the operator using the Helm chart and overrode `sparkJobNamespace`, the service account name ends with `-spark` and starts with the Helm release name. For example, if you would like to run your Spark jobs to run in a namespace called `test-ns`, first make sure it already exists, and then install the chart with the command:

```bash
$ helm install my-release spark-operator/spark-operator --namespace spark-operator --set sparkJobNamespace=test-ns
```

Then the chart will set up a service account for your Spark jobs to use in that namespace.

See the section on the [Spark Job Namespace](#about-the-spark-job-namespace) for details on the behavior of the default Spark Job Namespace.

Running the above command will create a `SparkApplication` object named `spark-pi`. Check the object by running the following command:

```bash
$ kubectl get sparkapplications spark-pi -o=yaml
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

```bash
$ kubectl describe sparkapplication spark-pi
```

This will show the events similarly to the following:

```
Events:
  Type    Reason                      Age   From            Message
  ----    ------                      ----  ----            -------
  Normal  SparkApplicationAdded       5m    spark-operator  SparkApplication spark-pi was added, enqueued it for submission
  Normal  SparkApplicationTerminated  4m    spark-operator  SparkApplication spark-pi terminated with state: COMPLETED
```

The operator submits the Spark Pi example to run once it receives an event indicating the `SparkApplication` object was added.

## Configuration

The operator is typically deployed and run using the Helm chart. However, users can still run it outside a Kubernetes cluster and make it talk to the Kubernetes API server of a cluster by specifying path to `kubeconfig`, which can be done using the `-kubeconfig` flag.

The operator uses multiple workers in the `SparkApplication` controller. The number of worker threads are controlled using command-line flag `-controller-threads` which has a default value of 10.

The operator enables cache resynchronization so periodically the informers used by the operator will re-list existing objects it manages and re-trigger resource events. The resynchronization interval in seconds can be configured using the flag `-resync-interval`, with a default value of 30 seconds.

By default, the operator will install the [CustomResourceDefinitions](https://kubernetes.io/docs/tasks/access-kubernetes-api/extend-api-custom-resource-definitions/) for the custom resources it manages. This can be disabled by setting the flag `-install-crds=false`, in which case the CustomResourceDefinitions can be installed manually using `kubectl apply -f manifest/spark-operator-crds.yaml`.

The mutating admission webhook is an **optional** component and can be enabled or disabled using the `-enable-webhook` flag, which defaults to `false`.

By default, the operator will manage custom resource objects of the managed CRD types for the whole cluster. It can be configured to manage only the custom resource objects in a specific namespace with the flag `-namespace=<namespace>`

## Upgrade

To upgrade the the operator, e.g., to use a newer version container image with a new tag, run the following command with updated parameters for the Helm release:

```bash
$ helm upgrade <YOUR-HELM-RELEASE-NAME> --set image.repository=org/image --set image.tag=newTag
```

Refer to the Helm [documentation](https://docs.helm.sh/helm/#helm-upgrade) for more details on `helm upgrade`.

## About the Spark Job Namespace

The Spark Job Namespace value defines the namespace(s) where `SparkApplications` can be deployed. The Helm chart value for the Spark Job Namespace is `sparkJobNamespace`, and its default value is `""`, as defined in the Helm chart's [README](../charts/spark-operator-chart/README.md). Note that in the [Kubernetes apimachinery](https://github.com/kubernetes/kubernetes/tree/master/staging/src/k8s.io/apimachinery) project, the constants `NamespaceAll` and `NamespaceNone` are both defined as the empty string. In this case, the empty string represents `NamespaceAll`. When set to `""`, the Spark Operator supports deploying `SparkApplications` to all namespaces. The Helm chart will create a service account in the namespace where the spark-operator is deployed. In order to successfully deploy `SparkApplications`, you will need to ensure the driver pod's service account meets the criteria described in the [service accounts for driver pods](#about-the-service-account-for-driver-pods) section.

if you installed the operator using the Helm chart and overrode the `sparkJobNamespace` to some other, pre-existing namespace, the Helm chart will create the necessary service account and RBAC in the specified namespace.

The Spark Operator uses the Spark Job Namespace to identify and filter relevant events for the `SparkApplication` CRD. If you specify a namespace for Spark Jobs, and then submit a SparkApplication resource to another namespace, the Spark Operator will filter out the event, and the resource will not get deployed. If you don't specify a namespace, the Spark Operator will see `SparkApplication` events for all namespaces, and will deploy them to the namespace requested in the create call.

## About the Service Account for Driver Pods

A Spark driver pod need a Kubernetes service account in the pod's namespace that has permissions to create, get, list, and delete executor pods, and create a Kubernetes headless service for the driver. The driver will fail and exit without the service account, unless the default service account in the pod's namespace has the needed permissions. To submit and run a `SparkApplication` in a namespace, please make sure there is a service account with the permissions in the namespace and set `.spec.driver.serviceAccount` to the name of the service account. Please refer to [spark-rbac.yaml](../manifest/spark-rbac.yaml) for an example RBAC setup that creates a driver service account named `spark` in the `default` namespace, with a RBAC role binding giving the service account the needed permissions.

## About the Service Account for Executor Pods

A Spark executor pod may be configured with a Kubernetes service account in the pod namespace. To submit and run a `SparkApplication` in a namespace, please make sure there is a service account with the permissions required in the namespace and set `.spec.executor.serviceAccount` to the name of the service account.

## Enable Metric Exporting to Prometheus

The operator exposes a set of metrics via the metric endpoint to be scraped by `Prometheus`. The Helm chart by default installs the operator with the additional flag to enable metrics (`-enable-metrics=true`) as well as other annotations used by Prometheus to scrape the metric endpoint. If `podMonitor.enable` is enabled, the helm chart will submit a pod monitor for the operator's pod. To install the operator  **without** metrics enabled, pass the appropriate flag during `helm install`:

```bash
$ helm install my-release spark-operator/spark-operator --namespace spark-operator --set metrics.enable=false
```

If enabled, the operator generates the following metrics:

#### Spark Application Metrics
| Metric | Description |
| ------------- | ------------- |
| `spark_app_count`  | Total number of SparkApplication handled by the Operator.|
| `spark_app_submit_count`  | Total number of SparkApplication spark-submitted by the Operator.|
| `spark_app_success_count` | Total number of SparkApplication which completed successfully.|
| `spark_app_failure_count` | Total number of SparkApplication which failed to complete. |
| `spark_app_running_count` | Total number of SparkApplication which are currently running.|
| `spark_app_success_execution_time_microseconds` | Execution time for applications which succeeded.|
| `spark_app_failure_execution_time_microseconds` | Execution time for applications which failed. |
| `spark_app_start_latency_microseconds` | Start latency of SparkApplication as type of [Prometheus Summary](https://prometheus.io/docs/concepts/metric_types/#summary). |
| `spark_app_start_latency_seconds` | Start latency of SparkApplication as type of [Prometheus Histogram](https://prometheus.io/docs/concepts/metric_types/#histogram). |
| `spark_app_executor_success_count` | Total number of Spark Executors which completed successfully. |
| `spark_app_executor_failure_count` | Total number of Spark Executors which failed. |
| `spark_app_executor_running_count` | Total number of Spark Executors which are currently running. |

#### Work Queue Metrics
| Metric | Description |
| ------------- | ------------- |
| `spark_application_controller_depth` | Current depth of workqueue |
| `spark_application_controller_adds` | Total number of adds handled by workqueue |
| `spark_application_controller_latency` | Latency for workqueue |
| `spark_application_controller_work_duration` | How long processing an item from workqueue takes |
| `spark_application_controller_retries` | Total number of retries handled by workqueue |
| `spark_application_controller_unfinished_work_seconds` | Unfinished work in seconds |
| `spark_application_controller_longest_running_processor_microseconds` | Longest running processor in microseconds |


The following is a list of all the configurations the operators supports for metrics:

```bash
-enable-metrics=true
-metrics-port=10254
-metrics-endpoint=/metrics
-metrics-prefix=myServiceName
-metrics-label=label1Key
-metrics-label=label2Key
```
All configs except `-enable-metrics` are optional. If port and/or endpoint are specified, please ensure that the annotations `prometheus.io/port`,  `prometheus.io/path` and `containerPort` in `spark-operator-with-metrics.yaml` are updated as well.

A note about `metrics-labels`: In `Prometheus`, every unique combination of key-value label pair represents a new time series, which can dramatically increase the amount of data stored.  Hence labels should not be used to store dimensions with high cardinality with potentially a large or unbounded value range.

Additionally, these metrics are best-effort for the current operator run and will be reset on an operator restart. Also some of these metrics are generated by listening to pod state updates for the driver/executors
and deleting the pods outside the operator might lead to incorrect metric values for some of these metrics.

## Driver UI Access and Ingress

The operator, by default, makes the Spark UI accessible by creating a service of type `ClusterIP` which exposes the UI. This is only accessible from within the cluster.
The operator also supports creating an optional Ingress for the UI. This can be turned on by setting the `ingress-url-format` command-line flag. The `ingress-url-format` should be a template like `{{$appName}}.{ingress_suffix}/{{$appNamespace}}/{{$appName}}`. The `{ingress_suffix}` should be replaced by the user to indicate the cluster's ingress url and the operator will replace the `{{$appName}}` & `{{$appNamespace}}` with the appropriate value. Please note that Ingress support requires that cluster's ingress url routing is correctly set-up. For e.g. if the `ingress-url-format` is `{{$appName}}.ingress.cluster.com`, it requires that anything `*ingress.cluster.com` should be routed to the ingress-controller on the K8s cluster.

The operator also sets both `WebUIAddress` which is accessible from within the cluster as well as `WebUIIngressAddress` as part of the `DriverInfo` field of the `SparkApplication`.

## About the Mutating Admission Webhook

The Kubernetes Operator for Apache Spark comes with an optional mutating admission webhook for customizing Spark driver and executor pods based on the specification in `SparkApplication` objects, e.g., mounting user-specified ConfigMaps and volumes, and setting pod affinity/anti-affinity, and adding tolerations.

The webhook requires a X509 certificate for TLS for pod admission requests and responses between the Kubernetes API server and the webhook server running inside the operator. For that, the certificate and key files must be accessible by the webhook server. The location of these certs is configurable and they will be reloaded on a configurable period.
The Kubernetes Operator for Spark ships with a tool at `hack/gencerts.sh` for generating the CA and server certificate and putting the certificate and key files into a secret named `spark-webhook-certs` in the namespace `spark-operator`. This secret will be mounted into the operator pod.

Run the following command to create the secret with a certificate and key files using a batch Job, and install the operator Deployment with the mutating admission webhook:

```bash
$ kubectl apply -f manifest/spark-operator-with-webhook.yaml
```

This will create a Deployment named `sparkoperator` and a Service named `spark-webhook` for the webhook in namespace `spark-operator`.

### Mutating Admission Webhooks on a private GKE cluster

If you are deploying the operator on a GKE cluster with the [Private cluster](https://cloud.google.com/kubernetes-engine/docs/how-to/private-clusters) setting enabled, and you wish to deploy the cluster with the [Mutating Admission Webhook](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/), then make sure to change the `webhookPort` to `443`. Alternatively you can choose to allow connections to the default port (8080).

> By default, firewall rules restrict your cluster master to only initiate TCP connections to your nodes on ports 443 (HTTPS) and 10250 (kubelet). For some Kubernetes features, you might need to add firewall rules to allow access on additional ports. For example, in Kubernetes 1.9 and older, kubectl top accesses heapster, which needs a firewall rule to allow TCP connections on port 8080. To grant such access, you can add firewall rules.
[From the docs](https://cloud.google.com/kubernetes-engine/docs/how-to/private-clusters#add_firewall_rules)

To install the operator with a custom port, pass the appropriate flag during `helm install`:

```bash
$ helm install my-release spark-operator/spark-operator --namespace spark-operator  --set sparkJobNamespace=spark --set webhook.enable=true --set webhook.port=443
```
