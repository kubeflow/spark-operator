# spark-operator

A Helm chart for Spark on Kubernetes operator

## Introduction

This chart bootstraps a [Kubernetes Operator for Apache Spark](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator) deployment using the [Helm](https://helm.sh) package manager.

## Prerequisites

- Helm >= 3
- Kubernetes >= 1.13

## Previous Helm Chart

The previous `spark-operator` Helm chart hosted at [helm/charts](https://github.com/helm/charts) has been moved to this repository in accordance with the [Deprecation timeline](https://github.com/helm/charts#deprecation-timeline). Note that a few things have changed between this version and the old version:

- This repository **only** supports Helm chart installations using Helm 3+ since the `apiVersion` on the chart has been marked as `v2`.
- Previous versions of the Helm chart have not been migrated, and the version has been set to `1.0.0` at the onset. If you are looking for old versions of the chart, it's best to run `helm pull incubator/sparkoperator --version <your-version>` until you are ready to move to this repository's version.
- Several configuration properties have been changed, carefully review the [values](#values) section below to make sure you're aligned with the new values.

## Installing the chart

```shell

$ helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator

$ helm install my-release spark-operator/spark-operator
```

This will create a release of `spark-operator` in the default namespace. To install in a different one:

```shell
$ helm install -n spark my-release spark-operator/spark-operator
```

Note that `helm` will fail to install if the namespace doesn't exist. Either create the namespace beforehand or pass the `--create-namespace` flag to the `helm install` command.

## Uninstalling the chart

To uninstall `my-release`:

```shell
$ helm uninstall my-release
```

The command removes all the Kubernetes components associated with the chart and deletes the release, except for the `crds`, those will have to be removed manually.

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` | Affinity for pod assignment |
| batchScheduler.enable | bool | `false` | Enable batch scheduler for spark jobs scheduling. If enabled, users can specify batch scheduler name in spark application |
| controllerThreads | int | `10` | Operator concurrency, higher values might increase memory usage |
| fullnameOverride | string | `""` | String to override release name |
| image.pullPolicy | string | `"IfNotPresent"` | Image pull policy |
| image.repository | string | `"gcr.io/spark-operator/spark-operator"` | Image repository |
| image.tag | string | `""` | Overrides the image tag whose default is the chart appVersion. |
| imagePullSecrets | list | `[]` | Image pull secrets |
| ingressUrlFormat | string | `""` | Ingress URL format |
| istio.enabled | bool | `false` | When using `istio`, spark jobs need to run without a sidecar to properly terminate |
| leaderElection.lockName | string | `"spark-operator-lock"` | Leader election lock name. Ref: https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/master/docs/user-guide.md#enabling-leader-election-for-high-availability. |
| leaderElection.lockNamespace | string | `""` | Optionally store the lock in another namespace. Defaults to operator's namespace |
| logLevel | int | `2` | Set higher levels for more verbose logging |
| metrics.enable | bool | `true` | Enable prometheus metric scraping |
| metrics.endpoint | string | `"/metrics"` | Metrics serving endpoint |
| metrics.port | int | `10254` | Metrics port |
| metrics.portName | string | `metrics` | Metrics port name |
| metrics.prefix | string | `""` | Metric prefix, will be added to all exported metrics |
| nameOverride | string | `""` | String to partially override `spark-operator.fullname` template (will maintain the release name) |
| nodeSelector | object | `{}` | Node labels for pod assignment |
| podAnnotations | object | `{}` | Additional annotations to add to the pod |
| podMonitor.enable | bool| `false` | Submit a prometheus pod monitor for operator's pod. Note that prometheus metrics should be enabled as well.|
| podMonitor.labels | object | `{}` | Pod monitor labels |
| podMonitor.jobLabel | string | `spark-operator-podmonitor` | The label to use to retrieve the job name from |
| podMonitor.podMetricsEndpoint.scheme | string | `http` | Prometheus metrics endpoint scheme |
| podMonitor.podMetricsEndpoint.interval | string | `5s` | Interval at which metrics should be scraped |
| podSecurityContext | object | `{}` | Pod security context |
| rbac.create | bool | `true` | Create and use `rbac` resources |
| replicaCount | int | `1` | Desired number of pods, leaderElection will be enabled if this is greater than 1 |
| resourceQuotaEnforcement.enable | bool | `false` | Whether to enable the ResourceQuota enforcement for SparkApplication resources. Requires the webhook to be enabled by setting `webhook.enable` to true. Ref: https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/master/docs/user-guide.md#enabling-resource-quota-enforcement. |
| resources | object | `{}` | Pod resource requests and limits |
| resyncInterval | int | `30` | Operator resync interval. Note that the operator will respond to events (e.g. create, update) unrelated to this setting |
| securityContext | object | `{}` | Operator container security context |
| serviceAccounts.spark.create | bool | `true` | Create a service account for spark apps |
| serviceAccounts.spark.name | string | `""` | Optional name for the spark service account |
| serviceAccounts.sparkoperator.create | bool | `true` | Create a service account for the operator |
| serviceAccounts.sparkoperator.name | string | `""` | Optional name for the operator service account |
| sparkJobNamespace | string | `""` | Set this if running spark jobs in a different namespace than the operator |
| tolerations | list | `[]` | List of node taints to tolerate |
| webhook.enable | bool | `false` | Enable webhook server |
| webhook.namespaceSelector | string | `""` | The webhook server will only operate on namespaces with this label, specified in the form key1=value1,key2=value2. Empty string (default) will operate on all namespaces |
| webhook.port | int | `8080` | Webhook service port |
| labelSelectorFilter | string | `""` | Set this if only operator watches spark jobs with certain labels are allowed |

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| yuchaoran2011 | yuchaoran2011@gmail.com |  |
