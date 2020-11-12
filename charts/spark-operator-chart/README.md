# spark-operator

A Helm chart for Spark on Kubernetes operator

## Introduction

This chart bootstraps a [Kubernetes Operator for Apache Spark](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator) deployment using the [Helm](https://helm.sh) package manager.

## Prerequisites

- Helm >= 3
- Kubernetes >= 1.13.x

## Previous Helm Chart

The previous `spark-operator` Helm chart hosted at [helm/charts](https://github.com/helm/charts) has been moved to this repository in accordance with the [Deprecation timeline](https://github.com/helm/charts#deprecation-timeline). Note that a few things have changed between this version and the old version:

- This repository **only** supports Helm chart installations using Helm 3+ since the `apiVersion` on the chart has been marked as `v2`.
- Previous versions of the Helm chart have not been migrated, and the version has been set to `1.0.0` at the onset. If you are looking for old versions of the chart, it's best to run `helm pull incubator/sparkoperator --version <your-version>` until you are ready to move to this repository's version.

## Installing the chart

TBD

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` | Affinity for pod assignment |
| batchScheduler.enable | bool | `false` | Enable batch scheduler for spark jobs scheduling. If enabled, end user can specify batch scheduler name in spark application |
| controllerThreads | int | `10` |  |
| fullnameOverride | string | `""` |  |
| image.pullPolicy | string | `"IfNotPresent"` |  |
| image.repository | string | `"gcr.io/spark-operator/spark-operator"` |  |
| image.tag | string | `""` | Overrides the image tag whose default is the chart appVersion. |
| imagePullSecrets | list | `[]` |  |
| ingressUrlFormat | string | `""` |  |
| istio.enabled | bool | `false` | When using istio, spark jobs need to run without a sidecar to properly terminate |
| leaderElection.lockName | string | `"spark-operator-lock"` | Leader election lock name. Ref: https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/master/docs/user-guide.md#enabling-leader-election-for-high-availability. |
| leaderElection.lockNamespace | string | `""` | Optionally store the lock in another namespace. Defaults to operator's namespace |
| logLevel | int | `2` |  |
| metrics.enable | bool | `true` | Enable prometheus mertic scraping |
| metrics.endpoint | string | `"/metrics"` |  |
| metrics.port | int | `10254` |  |
| metrics.prefix | string | `""` |  |
| nameOverride | string | `""` |  |
| nodeSelector | object | `{}` | Node labels for pod assignment |
| podAnnotations | object | `{}` | Additional annotations to add to the pod |
| podSecurityContext | object | `{}` |  |
| rbac.create | bool | `true` | Create and use `rbac` resources |
| replicaCount | int | `1` | Desired number of pods, leaderElection will be enabled if this is greater than 1 |
| resourceQuotaEnforcement.enable | bool | `false` | Whether to enable the ResourceQuota enforcement for SparkApplication resources. Requires the webhook to be enabled by setting webhook.enable to true. Ref: https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/master/docs/user-guide.md#enabling-resource-quota-enforcement. |
| resources | object | `{}` | Pod resource requests and limits |
| resyncInterval | int | `30` |  |
| securityContext | object | `{}` |  |
| serviceAccounts.spark.create | bool | `true` | Create a service account for spark apps |
| serviceAccounts.spark.name | string | `""` | Optional name for the spark service account |
| serviceAccounts.sparkoperator.create | bool | `true` | Create a service account for the operator |
| serviceAccounts.sparkoperator.name | string | `""` | Optional name for the operator service account |
| sparkJobNamespace | string | `""` | Set this if running spark jobs in a different namespace than the operator |
| tolerations | list | `[]` | List of node taints to tolerate |
| webhook.enable | bool | `false` | Enable webhook server |
| webhook.namespaceSelector | string | `""` | The webhook server will only operate on namespaces with this label, specified in the form key1=value1,key2=value2. Empty string (default) will operate on all namespaces |
| webhook.port | int | `8080` | Webhook service port |

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| yuchaoran2011 | yuchaoran2011@gmail.com |  |
