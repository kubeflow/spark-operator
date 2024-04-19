# spark-operator

![Version: 1.2.12](https://img.shields.io/badge/Version-1.2.12-informational?style=flat-square) ![AppVersion: v1beta2-1.4.4-3.5.0](https://img.shields.io/badge/AppVersion-v1beta2--1.4.4--3.5.0-informational?style=flat-square)

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
| controllerThreads | int | `10` | Operator concurrency, higher values might increase memory usage |
| envFrom | list | `[]` | Pod environment variable sources |
| fullnameOverride | string | `""` | String to override release name |
| image.pullPolicy | string | `"IfNotPresent"` | Image pull policy |
| image.repository | string | `"docker.io/kubeflow/spark-operator"` | Image repository |
| image.tag | string | `""` | if set, override the image tag whose default is the chart appVersion. |
| imagePullSecrets | list | `[]` | Image pull secrets |
| ingressUrlFormat | string | `""` | Ingress URL format. Requires the UI service to be enabled by setting `uiService.enable` to true. |
| istio.enabled | bool | `false` | When using `istio`, spark jobs need to run without a sidecar to properly terminate |
| labelSelectorFilter | string | `""` | A comma-separated list of key=value, or key labels to filter resources during watch and list based on the specified labels. |
| leaderElection.lockName | string | `"spark-operator-lock"` | Leader election lock name. Ref: https://github.com/kubeflow/spark-operator/blob/master/docs/user-guide.md#enabling-leader-election-for-high-availability. |
| leaderElection.lockNamespace | string | `""` | Optionally store the lock in another namespace. Defaults to operator's namespace |
| logLevel | int | `2` | Set higher levels for more verbose logging |
| metrics.enable | bool | `true` | Enable prometheus metric scraping |
| metrics.endpoint | string | `"/metrics"` | Metrics serving endpoint |
| metrics.port | int | `10254` | Metrics port |
| metrics.portName | string | `"metrics"` | Metrics port name |
| metrics.prefix | string | `""` | Metric prefix, will be added to all exported metrics |
| nameOverride | string | `""` | String to partially override `spark-operator.fullname` template (will maintain the release name) |
| nodeSelector | object | `{}` | Node labels for pod assignment |
| podAnnotations | object | `{}` | Additional annotations to add to the pod |
| podLabels | object | `{}` | Additional labels to add to the pod |
| podMonitor | object | `{"enable":false,"jobLabel":"spark-operator-podmonitor","labels":{},"podMetricsEndpoint":{"interval":"5s","scheme":"http"}}` | Prometheus pod monitor for operator's pod. |
| podMonitor.enable | bool | `false` | If enabled, a pod monitor for operator's pod will be submitted. Note that prometheus metrics should be enabled as well. |
| podMonitor.jobLabel | string | `"spark-operator-podmonitor"` | The label to use to retrieve the job name from |
| podMonitor.labels | object | `{}` | Pod monitor labels |
| podMonitor.podMetricsEndpoint | object | `{"interval":"5s","scheme":"http"}` | Prometheus metrics endpoint properties. `metrics.portName` will be used as a port |
| podSecurityContext | object | `{}` | Pod security context |
| rbac.annotations | object | `{}` | Optional annotations for rbac |
| rbac.create | bool | `false` | **DEPRECATED** use `createRole` and `createClusterRole` |
| rbac.createClusterRole | bool | `true` | Create and use RBAC `ClusterRole` resources |
| rbac.createRole | bool | `true` | Create and use RBAC `Role` resources |
| replicaCount | int | `1` | Desired number of pods, leaderElection will be enabled if this is greater than 1 |
| resourceQuotaEnforcement.enable | bool | `false` | Whether to enable the ResourceQuota enforcement for SparkApplication resources. Requires the webhook to be enabled by setting `webhook.enable` to true. Ref: https://github.com/kubeflow/spark-operator/blob/master/docs/user-guide.md#enabling-resource-quota-enforcement. |
| resources | object | `{}` | Pod resource requests and limits Note, that each job submission will spawn a JVM within the Spark Operator Pod using "/usr/local/openjdk-11/bin/java -Xmx128m". Kubernetes may kill these Java processes at will to enforce resource limits. When that happens, you will see the following error: 'failed to run spark-submit for SparkApplication [...]: signal: killed' - when this happens, you may want to increase memory limits. |
| resyncInterval | int | `30` | Operator resync interval. Note that the operator will respond to events (e.g. create, update) unrelated to this setting |
| securityContext | object | `{}` | Operator container security context |
| serviceAccounts.spark.annotations | object | `{}` | Optional annotations for the spark service account |
| serviceAccounts.spark.create | bool | `true` | Create a service account for spark apps |
| serviceAccounts.spark.name | string | `""` | Optional name for the spark service account |
| serviceAccounts.sparkoperator.annotations | object | `{}` | Optional annotations for the operator service account |
| serviceAccounts.sparkoperator.create | bool | `true` | Create a service account for the operator |
| serviceAccounts.sparkoperator.name | string | `""` | Optional name for the operator service account |
| sidecars | list | `[]` | Sidecar containers |
| sparkJobNamespaces | list | `[]` | List of namespaces where to run spark jobs, operator namespace is included only when list of namespaces is empty |
| tolerations | list | `[]` | List of node taints to tolerate |
| uiService.enable | bool | `true` | Enable UI service creation for Spark application |
| volumeMounts | list | `[]` |  |
| volumes | list | `[]` |  |
| webhook.cleanupAnnotations | object | `{"helm.sh/hook":"pre-delete, pre-upgrade","helm.sh/hook-delete-policy":"hook-succeeded"}` | The annotations applied to the cleanup job, required for helm lifecycle hooks |
| webhook.cleanupPodLabels | object | `{}` | The podLabels applied to the pod of the cleanup job |
| webhook.cleanupResources | object | `{}` | Resources applied to cleanup job |
| webhook.enable | bool | `false` | Enable webhook server |
| webhook.initAnnotations | object | `{"helm.sh/hook":"pre-install, pre-upgrade","helm.sh/hook-weight":"50"}` | The annotations applied to init job, required to restore certs deleted by the cleanup job during upgrade |
| webhook.initPodLabels | object | `{}` | The podLabels applied to the pod of the init job |
| webhook.initResources | object | `{}` | Resources applied to init job |
| webhook.namespaceSelector | string | `""` | The webhook server will only operate on namespaces with this label, specified in the form key1=value1,key2=value2. Empty string (default) will operate on all namespaces |
| webhook.port | int | `8080` | Webhook service port |
| webhook.portName | string | `"webhook"` | Webhook container port name and service target port name |
| webhook.timeout | int | `30` |  |

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| yuchaoran2011 | <yuchaoran2011@gmail.com> |  |
