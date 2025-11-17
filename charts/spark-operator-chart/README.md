# spark-operator

![Version: 2.4.0](https://img.shields.io/badge/Version-2.4.0-informational?style=flat-square) ![AppVersion: 2.4.0](https://img.shields.io/badge/AppVersion-2.4.0-informational?style=flat-square)

A Helm chart for Spark on Kubernetes operator.

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

For example, if you want to create a release with name `spark-operator` in the `spark-operator` namespace:

```shell
helm install spark-operator spark-operator/spark-operator \
    --namespace spark-operator \
    --create-namespace
```

Note that by passing the `--create-namespace` flag to the `helm install` command, `helm` will create the release namespace if it does not exist.

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
| nameOverride | string | `""` | String to partially override release name. |
| fullnameOverride | string | `""` | String to fully override release name. |
| commonLabels | object | `{}` | Common labels to add to the resources. |
| image.registry | string | `"ghcr.io"` | Image registry. |
| image.repository | string | `"kubeflow/spark-operator/controller"` | Image repository. |
| image.tag | string | If not set, the chart appVersion will be used. | Image tag. |
| image.pullPolicy | string | `"IfNotPresent"` | Image pull policy. |
| image.pullSecrets | list | `[]` | Image pull secrets for private image registry. |
| hook.upgradeCrd | bool | `false` | Whether to create a Helm pre-install/pre-upgrade hook Job to update CRDs. |
| hook.image.registry | string | `"ghcr.io"` | Image registry. |
| hook.image.repository | string | `"kubeflow/spark-operator/kubectl"` | Image repository. |
| hook.image.tag | string | If not set, the chart appVersion will be used. | Image tag. |
| controller.replicas | int | `1` | Number of replicas of controller. |
| controller.revisionHistoryLimit | int | `10` | The number of old history to retain to allow rollback. |
| controller.leaderElection.enable | bool | `true` | Specifies whether to enable leader election for controller. |
| controller.leaderElection.leaseDuration | string | `"15s"` | Leader election lease duration. |
| controller.leaderElection.renewDeadline | string | `"10s"` | Leader election renew deadline. |
| controller.leaderElection.retryPeriod | string | `"2s"` | Leader election retry period. |
| controller.workers | int | `10` | Reconcile concurrency, higher values might increase memory usage. |
| controller.logLevel | string | `"info"` | Configure the verbosity of logging, can be one of `debug`, `info`, `error`. |
| controller.logEncoder | string | `"console"` | Configure the encoder of logging, can be one of `console` or `json`. |
| controller.driverPodCreationGracePeriod | string | `"10s"` | Grace period after a successful spark-submit when driver pod not found errors will be retried. Useful if the driver pod can take some time to be created. |
| controller.maxTrackedExecutorPerApp | int | `1000` | Specifies the maximum number of Executor pods that can be tracked by the controller per SparkApplication. |
| controller.uiService.enable | bool | `true` | Specifies whether to create service for Spark web UI. |
| controller.uiIngress.enable | bool | `false` | Specifies whether to create ingress for Spark web UI. `controller.uiService.enable` must be `true` to enable ingress. |
| controller.uiIngress.urlFormat | string | `""` | Ingress URL format. Required if `controller.uiIngress.enable` is true. |
| controller.uiIngress.ingressClassName | string | `""` | Optionally set the ingressClassName. |
| controller.uiIngress.tls | list | `[]` | Optionally set default TLS configuration for the Spark UI's ingress. `ingressTLS` in the SparkApplication spec overrides this. |
| controller.uiIngress.annotations | object | `{}` | Optionally set default ingress annotations for the Spark UI's ingress. `ingressAnnotations` in the SparkApplication spec overrides this. |
| controller.batchScheduler.enable | bool | `false` | Specifies whether to enable batch scheduler for spark jobs scheduling. If enabled, users can specify batch scheduler name in spark application. |
| controller.batchScheduler.kubeSchedulerNames | list | `[]` | Specifies a list of kube-scheduler names for scheduling Spark pods. |
| controller.batchScheduler.default | string | `""` | Default batch scheduler to be used if not specified by the user. If specified, this value must be either "volcano" or "yunikorn". Specifying any other value will cause the controller to error on startup. |
| controller.serviceAccount.create | bool | `true` | Specifies whether to create a service account for the controller. |
| controller.serviceAccount.name | string | `""` | Optional name for the controller service account. |
| controller.serviceAccount.annotations | object | `{}` | Extra annotations for the controller service account. |
| controller.serviceAccount.automountServiceAccountToken | bool | `true` | Auto-mount service account token to the controller pods. |
| controller.rbac.create | bool | `true` | Specifies whether to create RBAC resources for the controller. |
| controller.rbac.annotations | object | `{}` | Extra annotations for the controller RBAC resources. |
| controller.labels | object | `{}` | Extra labels for controller pods. |
| controller.annotations | object | `{}` | Extra annotations for controller pods. |
| controller.volumes | list | `[{"emptyDir":{"sizeLimit":"1Gi"},"name":"tmp"}]` | Volumes for controller pods. |
| controller.nodeSelector | object | `{}` | Node selector for controller pods. |
| controller.affinity | object | `{}` | Affinity for controller pods. |
| controller.tolerations | list | `[]` | List of node taints to tolerate for controller pods. |
| controller.priorityClassName | string | `""` | Priority class for controller pods. |
| controller.podSecurityContext | object | `{"fsGroup":185}` | Security context for controller pods. |
| controller.topologySpreadConstraints | list | `[]` | Topology spread constraints rely on node labels to identify the topology domain(s) that each Node is in. Ref: [Pod Topology Spread Constraints](https://kubernetes.io/docs/concepts/workloads/pods/pod-topology-spread-constraints/). The labelSelector field in topology spread constraint will be set to the selector labels for controller pods if not specified. |
| controller.env | list | `[]` | Environment variables for controller containers. |
| controller.envFrom | list | `[]` | Environment variable sources for controller containers. |
| controller.volumeMounts | list | `[{"mountPath":"/tmp","name":"tmp","readOnly":false}]` | Volume mounts for controller containers. |
| controller.resources | object | `{}` | Pod resource requests and limits for controller containers. Note, that each job submission will spawn a JVM within the controller pods using "/usr/local/openjdk-11/bin/java -Xmx128m". Kubernetes may kill these Java processes at will to enforce resource limits. When that happens, you will see the following error: 'failed to run spark-submit for SparkApplication [...]: signal: killed' - when this happens, you may want to increase memory limits. |
| controller.securityContext | object | `{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"privileged":false,"readOnlyRootFilesystem":true,"runAsNonRoot":true,"seccompProfile":{"type":"RuntimeDefault"}}` | Security context for controller containers. |
| controller.sidecars | list | `[]` | Sidecar containers for controller pods. |
| controller.podDisruptionBudget.enable | bool | `false` | Specifies whether to create pod disruption budget for controller. Ref: [Specifying a Disruption Budget for your Application](https://kubernetes.io/docs/tasks/run-application/configure-pdb/) |
| controller.podDisruptionBudget.minAvailable | int | `1` | The number of pods that must be available. Require `controller.replicas` to be greater than 1 |
| controller.pprof.enable | bool | `false` | Specifies whether to enable pprof. |
| controller.pprof.port | int | `6060` | Specifies pprof port. |
| controller.pprof.portName | string | `"pprof"` | Specifies pprof service port name. |
| controller.workqueueRateLimiter.bucketQPS | int | `50` | Specifies the average rate of items process by the workqueue rate limiter. |
| controller.workqueueRateLimiter.bucketSize | int | `500` | Specifies the maximum number of items that can be in the workqueue at any given time. |
| controller.workqueueRateLimiter.maxDelay.enable | bool | `true` | Specifies whether to enable max delay for the workqueue rate limiter. This is useful to avoid losing events when the workqueue is full. |
| controller.workqueueRateLimiter.maxDelay.duration | string | `"6h"` | Specifies the maximum delay duration for the workqueue rate limiter. |
| webhook.enable | bool | `true` | Specifies whether to enable webhook. |
| webhook.replicas | int | `1` | Number of replicas of webhook server. |
| webhook.revisionHistoryLimit | int | `10` | The number of old history to retain to allow rollback. |
| webhook.leaderElection.enable | bool | `true` | Specifies whether to enable leader election for webhook. |
| webhook.logLevel | string | `"info"` | Configure the verbosity of logging, can be one of `debug`, `info`, `error`. |
| webhook.logEncoder | string | `"console"` | Configure the encoder of logging, can be one of `console` or `json`. |
| webhook.port | int | `9443` | Specifies webhook port. |
| webhook.portName | string | `"webhook"` | Specifies webhook service port name. |
| webhook.failurePolicy | string | `"Fail"` | Specifies how unrecognized errors are handled. Available options are `Ignore` or `Fail`. |
| webhook.timeoutSeconds | int | `10` | Specifies the timeout seconds of the webhook, the value must be between 1 and 30. |
| webhook.resourceQuotaEnforcement.enable | bool | `false` | Specifies whether to enable the ResourceQuota enforcement for SparkApplication resources. |
| webhook.serviceAccount.create | bool | `true` | Specifies whether to create a service account for the webhook. |
| webhook.serviceAccount.name | string | `""` | Optional name for the webhook service account. |
| webhook.serviceAccount.annotations | object | `{}` | Extra annotations for the webhook service account. |
| webhook.serviceAccount.automountServiceAccountToken | bool | `true` | Auto-mount service account token to the webhook pods. |
| webhook.rbac.create | bool | `true` | Specifies whether to create RBAC resources for the webhook. |
| webhook.rbac.annotations | object | `{}` | Extra annotations for the webhook RBAC resources. |
| webhook.labels | object | `{}` | Extra labels for webhook pods. |
| webhook.annotations | object | `{}` | Extra annotations for webhook pods. |
| webhook.sidecars | list | `[]` | Sidecar containers for webhook pods. |
| webhook.volumes | list | `[{"emptyDir":{"sizeLimit":"500Mi"},"name":"serving-certs"}]` | Volumes for webhook pods. |
| webhook.nodeSelector | object | `{}` | Node selector for webhook pods. |
| webhook.affinity | object | `{}` | Affinity for webhook pods. |
| webhook.tolerations | list | `[]` | List of node taints to tolerate for webhook pods. |
| webhook.priorityClassName | string | `""` | Priority class for webhook pods. |
| webhook.podSecurityContext | object | `{"fsGroup":185}` | Security context for webhook pods. |
| webhook.topologySpreadConstraints | list | `[]` | Topology spread constraints rely on node labels to identify the topology domain(s) that each Node is in. Ref: [Pod Topology Spread Constraints](https://kubernetes.io/docs/concepts/workloads/pods/pod-topology-spread-constraints/). The labelSelector field in topology spread constraint will be set to the selector labels for webhook pods if not specified. |
| webhook.env | list | `[]` | Environment variables for webhook containers. |
| webhook.envFrom | list | `[]` | Environment variable sources for webhook containers. |
| webhook.volumeMounts | list | `[{"mountPath":"/etc/k8s-webhook-server/serving-certs","name":"serving-certs","readOnly":false,"subPath":"serving-certs"}]` | Volume mounts for webhook containers. |
| webhook.resources | object | `{}` | Pod resource requests and limits for webhook pods. |
| webhook.securityContext | object | `{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"privileged":false,"readOnlyRootFilesystem":true,"runAsNonRoot":true,"seccompProfile":{"type":"RuntimeDefault"}}` | Security context for webhook containers. |
| webhook.podDisruptionBudget.enable | bool | `false` | Specifies whether to create pod disruption budget for webhook. Ref: [Specifying a Disruption Budget for your Application](https://kubernetes.io/docs/tasks/run-application/configure-pdb/) |
| webhook.podDisruptionBudget.minAvailable | int | `1` | The number of pods that must be available. Require `webhook.replicas` to be greater than 1 |
| spark.jobNamespaces | list | `["default"]` | List of namespaces where to run spark jobs. If empty string is included, all namespaces will be allowed. Make sure the namespaces have already existed. |
| spark.serviceAccount.create | bool | `true` | Specifies whether to create a service account for spark applications. |
| spark.serviceAccount.name | string | `""` | Optional name for the spark service account. |
| spark.serviceAccount.annotations | object | `{}` | Optional annotations for the spark service account. |
| spark.serviceAccount.automountServiceAccountToken | bool | `true` | Auto-mount service account token to the spark applications pods. |
| spark.rbac.create | bool | `true` | Specifies whether to create RBAC resources for spark applications. |
| spark.rbac.annotations | object | `{}` | Optional annotations for the spark application RBAC resources. |
| prometheus.metrics.enable | bool | `true` | Specifies whether to enable prometheus metrics scraping. |
| prometheus.metrics.port | int | `8080` | Metrics port. |
| prometheus.metrics.portName | string | `"metrics"` | Metrics port name. |
| prometheus.metrics.endpoint | string | `"/metrics"` | Metrics serving endpoint. |
| prometheus.metrics.prefix | string | `""` | Metrics prefix, will be added to all exported metrics. |
| prometheus.metrics.jobStartLatencyBuckets | string | `"30,60,90,120,150,180,210,240,270,300"` | Job Start Latency histogram buckets. Specified in seconds. |
| prometheus.podMonitor.create | bool | `false` | Specifies whether to create pod monitor. Note that prometheus metrics should be enabled as well. |
| prometheus.podMonitor.labels | object | `{}` | Pod monitor labels |
| prometheus.podMonitor.jobLabel | string | `"spark-operator-podmonitor"` | The label to use to retrieve the job name from |
| prometheus.podMonitor.podMetricsEndpoint | object | `{"interval":"5s","scheme":"http"}` | Prometheus metrics endpoint properties. `metrics.portName` will be used as a port |
| certManager.enable | bool | `false` | Specifies whether to use [cert-manager](https://cert-manager.io) to generate certificate for webhook. `webhook.enable` must be set to `true` to enable cert-manager. |
| certManager.issuerRef | object | A self-signed issuer will be created and used if not specified. | The reference to the issuer. |
| certManager.duration | string | `2160h` (90 days) will be used if not specified. | The duration of the certificate validity (e.g. `2160h`). See [cert-manager.io/v1.Certificate](https://cert-manager.io/docs/reference/api-docs/#cert-manager.io/v1.Certificate). |
| certManager.renewBefore | string | 1/3 of issued certificate’s lifetime. | The duration before the certificate expiration to renew the certificate (e.g. `720h`). See [cert-manager.io/v1.Certificate](https://cert-manager.io/docs/reference/api-docs/#cert-manager.io/v1.Certificate). |

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| yuchaoran2011 | <yuchaoran2011@gmail.com> | <https://github.com/yuchaoran2011> |
| ChenYi015 | <github@chenyicn.net> | <https://github.com/ChenYi015> |
