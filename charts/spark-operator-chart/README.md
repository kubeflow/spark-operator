# spark-operator

![Version: 2.0.0](https://img.shields.io/badge/Version-2.0.0-informational?style=flat-square) ![AppVersion: v1beta2-2.0.0-3.5.0](https://img.shields.io/badge/AppVersion-v1beta2--2.0.0--3.5.0-informational?style=flat-square)

A Helm chart for Spark on Kubernetes operator

**Homepage:** <https://github.com/kubeflow/spark-operator>

## Introduction

This chart bootstraps a [Kubernetes Operator for Apache Spark](https://github.com/kubeflow/spark-operator) deployment using the [Helm](https://helm.sh) package manager.

## Prerequisites

- Helm >= 3
- Kubernetes >= 1.16

## Previous Helm Chart

The previous `spark-operator` Helm chart hosted at [helm/charts](https://github.com/helm/charts) has been moved to this repository in accordance with the [Deprecation timeline](https://github.com/helm/charts#deprecation-timeline). Note that a few things have changed between this version and the old version:

- This repository **only** supports Helm chart installations using Helm 3+ since the `apiVersion` on the chart has been marked as `v2`.
- Previous versions of the Helm chart have not been migrated, and the version has been set to `1.0.0` at the onset. If you are looking for old versions of the chart, it's best to run `helm pull incubator/sparkoperator --version <your-version>` until you are ready to move to this repository's version.
- Several configuration properties have been changed, carefully review the [values](#values) section below to make sure you're aligned with the new values.

## Usage

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

For example, if you want to create a release with name `spark-operator` in the `default` namespace:

```shell
helm install spark-operator spark-operator/spark-operator
```

Note that `helm` will fail to install if the namespace doesn't exist. Either create the namespace beforehand or pass the `--create-namespace` flag to the `helm install` command.

```shell
helm install spark-operator spark-operator/spark-operator \
    --namespace spark-operator \
    --create-namespace
```

See [helm install](https://helm.sh/docs/helm/helm_install) for command documentation.

### Upgrade the chart

```shell
helm upgrade [RELEASE_NAME] spark-operator/spark-operator [flags]
```

See [helm upgrade](https://helm.sh/docs/helm/helm_upgrade) for command documentation.

### Uninstall the chart

```shell
helm uninstall [RELEASE_NAME]
```

This removes all the Kubernetes resources associated with the chart and deletes the release, except for the `crds`, those will have to be removed manually.

See [helm uninstall](https://helm.sh/docs/helm/helm_uninstall) for command documentation.

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` | Affinity for pod assignment |
| batchScheduler.enable | bool | `false` | Enable batch scheduler for spark jobs scheduling. If enabled, users can specify batch scheduler name in spark application |
| commonLabels | object | `{}` | Common labels to add to the resources |
| controller.ingressUrlFormat | string | `""` | Ingress URL format. Requires the UI service to be enabled by setting `controller.uiService.enable` to true. |
| controller.labelSelectorFilter | string | `""` | A comma-separated list of key=value, or key labels to filter resources during watch and list based on the specified labels. |
| controller.logLevel | int | `1` | Set higher levels for more verbose logging |
| controller.rbac.annotations | object | `{}` | Optional annotations for the controller RBAC resources |
| controller.rbac.create | bool | `true` | Specifies whether to create RBAC resources for the controller |
| controller.replicaCount | int | `1` | Number of replicas of controller, leader election will be enabled if this is greater than 1 |
| controller.resyncInterval | int | `30` | Operator resync interval. Note that the operator will respond to events (e.g. create, update) unrelated to this setting |
| controller.serviceAccount.annotations | object | `{}` | Optional annotations for the controller service account |
| controller.serviceAccount.create | bool | `true` | Specifies whether to create a service account for the controller |
| controller.serviceAccount.name | string | `""` | Optional name for the controller service account |
| controller.uiService.enable | bool | `true` | Enable UI service creation for Spark application |
| controller.workers | int | `10` | Operator concurrency, higher values might increase memory usage |
| envFrom | list | `[]` | Pod environment variable sources |
| fullnameOverride | string | `""` | String to override release name |
| image.pullPolicy | string | `"IfNotPresent"` | Image pull policy |
| image.pullSecrets | list | `[]` | Image pull secrets |
| image.repository | string | `"docker.io/kubeflow/spark-operator"` | Image repository |
| image.tag | string | `""` | Image tag, if not set, the chart appVersion will be used. |
| istio.enable | bool | `false` | When using `istio`, spark jobs need to run without a sidecar to properly terminate |
| nameOverride | string | `""` | String to partially override `spark-operator.fullname` template (will maintain the release name) |
| nodeSelector | object | `{}` | Node labels for pod assignment |
| podAnnotations | object | `{}` | Additional annotations to add to the pod |
| podLabels | object | `{}` | Additional labels to add to the pod |
| podSecurityContext | object | `{}` | Pod security context |
| priorityClassName | string | `""` | A priority class to be used for running spark-operator pod. |
| prometheus.metrics.enable | bool | `true` | Specifies whether to enable prometheus metrics scraping |
| prometheus.metrics.endpoint | string | `"/metrics"` | Metrics serving endpoint |
| prometheus.metrics.port | int | `10254` | Metrics port |
| prometheus.metrics.portName | string | `"metrics"` | Metrics port name |
| prometheus.metrics.prefix | string | `""` | Metric prefix, will be added to all exported metrics |
| prometheus.podMonitor.create | bool | `false` | Specifies whether to create pod monitor. Note that prometheus metrics should be enabled as well. |
| prometheus.podMonitor.jobLabel | string | `"spark-operator-podmonitor"` | The label to use to retrieve the job name from |
| prometheus.podMonitor.labels | object | `{}` | Pod monitor labels |
| prometheus.podMonitor.podMetricsEndpoint | object | `{"interval":"5s","scheme":"http"}` | Prometheus metrics endpoint properties. `metrics.portName` will be used as a port |
| resourceQuotaEnforcement.enable | bool | `false` | Whether to enable the ResourceQuota enforcement for SparkApplication resources. Requires the webhook to be enabled by setting `webhook.enable` to true. Ref: https://github.com/kubeflow/spark-operator/blob/master/docs/user-guide.md#enabling-resource-quota-enforcement. |
| resources | object | `{}` | Pod resource requests and limits Note, that each job submission will spawn a JVM within the Spark Operator Pod using "/usr/local/openjdk-11/bin/java -Xmx128m". Kubernetes may kill these Java processes at will to enforce resource limits. When that happens, you will see the following error: 'failed to run spark-submit for SparkApplication [...]: signal: killed' - when this happens, you may want to increase memory limits. |
| securityContext | object | `{}` | Container security context |
| sidecars | list | `[]` | Sidecar containers |
| spark.jobNamespaces | list | `["default"]` | List of namespaces where to run spark jobs |
| spark.rbac.annotations | object | `{}` | Optional annotations for the spark application RBAC resources |
| spark.rbac.create | bool | `true` | Specifies whether to create RBAC resources for spark applications |
| spark.serviceAccount.annotations | object | `{}` | Optional annotations for the spark service account |
| spark.serviceAccount.create | bool | `true` | Specifies whether to create a service account for spark applications |
| spark.serviceAccount.name | string | `""` | Optional name for the spark service account |
| tolerations | list | `[]` | List of node taints to tolerate |
| volumeMounts | list | `[]` | Operator volumeMounts |
| volumes | list | `[]` |  |
| webhook.enable | bool | `true` | Specifies whether to enable webhook server |
| webhook.failurePolicy | string | `"Fail"` | Specifies how unrecognized errors are handled, allowed values are `Ignore` or `Fail`. |
| webhook.logLevel | int | `1` | Set higher levels for more verbose logging |
| webhook.port | int | `9443` | Specifies webhook port |
| webhook.portName | string | `"webhook"` | Specifies webhook service port name |
| webhook.rbac.annotations | object | `{}` | Optional annotations for the webhook RBAC resources |
| webhook.rbac.create | bool | `true` | Specifies whether to create RBAC resources for the webhook |
| webhook.replicaCount | int | `1` | Number of replicas of webhook server |
| webhook.serviceAccount.annotations | object | `{}` | Optional annotations for the webhook service account |
| webhook.serviceAccount.create | bool | `true` | Specifies whether to create a service account for the webhook |
| webhook.serviceAccount.name | string | `""` | Optional name for the webhook service account |
| webhook.timeoutSeconds | int | `10` | Specifies the timeout seconds of the webhook, the value must be between 1 and 30. |

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| yuchaoran2011 | <yuchaoran2011@gmail.com> |  |
| ChenYi015 | <github@chenyicn.net> |  |
