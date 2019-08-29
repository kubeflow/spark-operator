# SparkApplication API

The Kubernetes Operator for Apache Spark uses  [CustomResourceDefinitions](https://kubernetes.io/docs/concepts/api-extension/custom-resources/) named `SparkApplication` and `ScheduledSparkApplication` for specifying one-time Spark applications and Spark applications
that are supposed to run on a standard [cron](https://en.wikipedia.org/wiki/Cron) schedule. Similarly to other kinds of Kubernetes resources, they consist of a specification in a `Spec` field and a `Status` field. The definitions are organized in the following structure. The v1beta1 version of the API definition is implemented [here](../pkg/apis/sparkoperator.k8s.io/v1beta1/types.go).

```
ScheduledSparkApplication
|__ ScheduledSparkApplicationSpec
    |__ SparkApplication
|__ ScheduledSparkApplicationStatus

SparkApplication
|__ SparkApplicationSpec
    |__ DriverSpec
        |__ SparkPodSpec
    |__ ExecutorSpec
        |__ SparkPodSpec
    |__ Dependencies
    |__ MonitoringSpec
        |__ PrometheusSpec
|__ SparkApplicationStatus
    |__ DriverInfo    
```

## API Definition

### `SparkApplicationSpec`

A `SparkApplicationSpec` has the following top-level fields:

| Field | Spark configuration property or `spark-submit` option | Note |
| ------------- | ------------- | ------------- |
| `Type`  | N/A  | The type of the Spark application. Valid values are `Java`, `Scala`, `Python`, and `R`. |
| `PythonVersion`  | `spark.kubernetes.pyspark.pythonVersion`  | This sets the major Python version of the docker image used to run the driver and executor containers. Can either be 2 or 3, default 2. |
| `Mode`  | `--mode` | Spark deployment mode. Valid values are `cluster` and `client`. |
| `Image` | `spark.kubernetes.container.image` | Unified container image for the driver, executor, and init-container. |
| `InitContainerImage` | `spark.kubernetes.initContainer.image` | Custom init-container image. |
| `ImagePullPolicy` | `spark.kubernetes.container.image.pullPolicy` | Container image pull policy. |
| `ImagePullSecrets` | `spark.kubernetes.container.image.pullSecrets` | Container image pull secrets. |
| `MainClass` | `--class` | Main application class to run. |
| `MainApplicationFile` | N/A | Main application file, e.g., a bundled jar containing the main class and its dependencies. |
| `Arguments` | N/A | List of application arguments. |
| `SparkConf` | N/A | A map of extra Spark configuration properties. |
| `HadoopConf` | N/A | A map of Hadoop configuration properties. The operator will add the prefix `spark.hadoop.` to the properties when adding it through the `--conf` option. |
| `SparkConfigMap` | N/A | Name of a Kubernetes ConfigMap carrying Spark configuration files, e.g., `spark-env.sh`. The controller sets the environment variable `SPARK_CONF_DIR` to where the ConfigMap is mounted. |
| `HadoopConfigMap` | N/A | Name of a Kubernetes ConfigMap carrying Hadoop configuration files, e.g., `core-site.xml`. The controller sets the environment variable `HADOOP_CONF_DIR` to where the ConfigMap is mounted. |
| `Volumes` | N/A | List of Kubernetes [volumes](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.9/#volume-v1-core) the driver and executors need collectively. |
| `Driver` | N/A | A [`DriverSpec`](#driverspec) field. |
| `Executor` | N/A | An [`ExecutorSpec`](#executorspec) field. |
| `Deps` | N/A | A [`Dependencies`](#dependencies) field. |
| `RestartPolicy` | N/A | The policy regarding if and in which conditions the controller should restart a terminated application. |
| `NodeSelector` | `spark.kubernetes.node.selector.[labelKey]` | Node selector of the driver pod and executor pods, with key `labelKey` and value as the label's value. |
| `MemoryOverheadFactor` | `spark.kubernetes.memoryOverheadFactor` | This sets the Memory Overhead Factor that will allocate memory to non-JVM memory. For JVM-based jobs this value will default to 0.10, for non-JVM jobs 0.40. Value of this field will be overridden by `Spec.Driver.MemoryOverhead` and `Spec.Executor.MemoryOverhead` if they are set. |
| `Monitoring` | N/A | This specifies how monitoring of the Spark application should be handled, e.g., how driver and executor metrics are to be exposed. Currently only exposing metrics to Prometheus is supported. |


#### `DriverSpec`

A `DriverSpec` embeds a [`SparkPodSpec`](#sparkpodspec) and additionally has the following fields:

| Field | Spark configuration property or `spark-submit` option | Note |
| ------------- | ------------- | ------------- |
| `PodName` | `spark.kubernetes.driver.pod.name` | Name of the driver pod. |
| `ServiceAccount` | `spark.kubernetes.authenticate.driver.serviceAccountName` | Name of the Kubernetes service account to use for the driver pod. |

#### `ExecutorSpec`

Similarly to the `DriverSpec`, an `ExecutorSpec` also embeds a a [`SparkPodSpec`](#sparkpodspec) and additionally has the following fields:

| Field | Spark configuration property or `spark-submit` option | Note |
| ------------- | ------------- | ------------- |
| `Instances` | `spark.executor.instances` | Number of executor instances to request for. |
| `CoreRequest` | `spark.kubernetes.executor.request.cores` | Physical CPU request for the executors. |

#### `SparkPodSpec`

A `SparkPodSpec` defines common attributes of a driver or executor pod, summarized in the following table.

| Field | Spark configuration property or `spark-submit` option | Note |
| ------------- | ------------- | ------------- |
| `Cores` | `spark.driver.cores` or `spark.executor.cores` | Number of CPU cores for the driver or executor pod. |
| `CoreLimit` | `spark.kubernetes.driver.limit.cores` or `spark.kubernetes.executor.limit.cores` | Hard limit on the number of CPU cores for the driver or executor pod. |
| `Memory` | `spark.driver.memory` or `spark.executor.memory` | Amount of memory to request for the driver or executor pod. |
| `MemoryOverhead` | `spark.driver.memoryOverhead` or `spark.executor.memoryOverhead` | Amount of off-heap memory to allocate for the driver or executor pod in cluster mode, in `MiB` unless otherwise specified. |
| `Image` | `spark.kubernetes.driver.container.image` or `spark.kubernetes.executor.container.image` | Custom container image for the driver or executor. |
| `ConfigMaps` | N/A | A map of Kubernetes ConfigMaps to mount into the driver or executor pod. Keys are ConfigMap names and values are mount paths. |
| `Secrets` | `spark.kubernetes.driver.secrets.[SecretName]` or `spark.kubernetes.executor.secrets.[SecretName]` | A map of Kubernetes secrets to mount into the driver or executor pod. Keys are secret names and values specify the mount paths and secret types. |
| `EnvVars` | `spark.kubernetes.driverEnv.[EnvironmentVariableName]` or `spark.executorEnv.[EnvironmentVariableName]` | A map of environment variables to add to the driver or executor pod. Keys are variable names and values are variable values. |
| `EnvSecretKeyRefs` | `spark.kubernetes.driver.secretKeyRef.[EnvironmentVariableName]` or `spark.kubernetes.executor.secretKeyRef.[EnvironmentVariableName]` | A map of environment variables to SecretKeyRefs. Keys are variable names and values are pairs of a secret name and a secret key. |
| `Labels` | `spark.kubernetes.driver.label.[LabelName]` or `spark.kubernetes.executor.label.[LabelName]` | A map of Kubernetes labels to add to the driver or executor pod. Keys are label names and values are label values. |
| `Annotations` | `spark.kubernetes.driver.annotation.[AnnotationName]` or `spark.kubernetes.executor.annotation.[AnnotationName]` | A map of Kubernetes annotations to add to the driver or executor pod. Keys are annotation names and values are annotation values. |
| `VolumeMounts` | N/A | List of Kubernetes [volume mounts](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.9/#volumemount-v1-core) for volumes that should be mounted to the pod. |
| `Tolerations` | N/A | List of Kubernetes [tolerations](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.9/#toleration-v1-core) that should be applied to the pod. |

#### `Dependencies`

A `Dependencies` specifies the various types of dependencies of a Spark application in a central place.

| Field | Spark configuration property or `spark-submit` option | Note |
| ------------- | ------------- | ------------- |
| `Jars` | `spark.jars` or `--jars` | List of jars the application depends on. |
| `Files` | `spark.files` or `--files` | List of files the application depends on. |

#### `MonitoringSpec`

A `MonitoringSpec` specifies how monitoring of the Spark application should be handled, e.g., how driver and executor metrics are to be exposed. Currently only exposing metrics to Prometheus is supported.

| Field | Spark configuration property or `spark-submit` option | Note |
| ------------- | ------------- | ------------- |
| `ExposeDriverMetrics` | N/A | This specifies if driver metrics should be exposed. Defaults to `false`. |
| `ExposeExecutorMetrics` | N/A | This specifies if executor metrics should be exposed. Defaults to `false`. |
| `MetricsProperties` | N/A | If specified, this contains the content of a custom `metrics.properties` that configures the Spark metrics system. Otherwise, the content of `spark-docker/conf/metrics.properties` will be used. |
| `PrometheusSpec` | N/A | If specified, this configures how metrics are exposed to Prometheus. |

#### `PrometheusSpec`

A `PrometheusSpec` configures how metrics are exposed to Prometheus.

| Field | Spark configuration property or `spark-submit` option | Note |
| ------------- | ------------- | ------------- |
| `JmxExporterJar` | N/A | This specifies the path to the [Prometheus JMX exporter](https://github.com/prometheus/jmx_exporter) jar. |
| `Port` | N/A | If specified, the value will be used in the Java agent configuration for the Prometheus JMX exporter. The Java agent gets bound to the specified port if specified or `8090` otherwise by default. |
| `ConfigFile` | N/A | This specifies the full path of the Prometheus configuration file in the Spark image. If specified, it will override the default configurations and take precedence over `Configuration` shown below. |
| `Configuration` | N/A | If specified, this contains the contents of a custom Prometheus configuration used by the Prometheus JMX exporter. Otherwise, the contents of `spark-docker/conf/prometheus.yaml` will be used, unless `ConfigFile` is specified. |

### `SparkApplicationStatus`

A `SparkApplicationStatus` captures the status of a Spark application including the state of every executors.

| Field | Note |
| ------------- | ------------- |
| `AppID` | A randomly generated ID used to group all Kubernetes resources of an application. |
| `LastSubmissionAttemptTime` | Time for the last application submission attempt. |
| `CompletionTime` | Time the application completes (if it does). |
| `DriverInfo` | A [`DriverInfo`](#driverinfo) field. |
| `AppState` | Current state of the application. |
| `ExecutorState` | A map of executor pod names to executor state. |
| `ExecutionAttempts` | The number of attempts made for an application. |
| `SubmissionAttempts` | The number of submission attempts made for an application. |


#### `DriverInfo`

A `DriverInfo` captures information about the driver pod and the Spark web UI running in the driver.

| Field | Note |
| ------------- | ------------- |
| `WebUIServiceName` | Name of the service for the Spark web UI. |
| `WebUIPort` | Port on which the Spark web UI runs on the Node. |
| `WebUIAddress` | Address to access the web UI from within the cluster. |
| `WebUIIngressName` | Name of the ingress for the Spark web UI. |
| `WebUIIngressAddress` | Address to access the web UI via the Ingress. |
| `PodName` | Name of the driver pod. |

### `ScheduledSparkApplicationSpec`

A `ScheduledSparkApplicationSpec` has the following top-level fields:

| Field | Optional | Default | Note |
| ------------- | ------------- | ------------- | ------------- |
| `Schedule` | No | N/A | The cron schedule on which the application should run. |
| `Template` | No | N/A | A template from which `SparkApplication` instances of scheduled runs of the application can be created. |
| `Suspend` | Yes | `false` | A flag telling the controller to suspend subsequent runs of the application if set to `true`. |
| `ConcurrencyPolicy` | `Allow` | Yes | the policy governing concurrent runs of the application. Valid values are `Allow`, `Forbid`, and `Replace` |
| `SuccessfulRunHistoryLimit` | Yes | 1 | The number of past successful runs of the application to keep track of. |
| `FailedRunHistoryLimit` | Yes | 1 | The number of past failed runs of the application to keep track of. |

### `ScheduledSparkApplicationStatus`

A `ScheduledSparkApplicationStatus` captures the status of a Spark application including the state of every executors.

| Field | Note |
| ------------- | ------------- |
| `LastRun` | The time when the last run of the application started. |
| `NextRun` | The time when the next run of the application is estimated to start. |
| `PastSuccessfulRunNames` | The names of `SparkApplication` objects of past successful runs of the application. The maximum number of names to keep track of is controlled by `SuccessfulRunHistoryLimit`. |
| `PastFailedRunNames` | The names of `SparkApplication` objects of past failed runs of the application. The maximum number of names to keep track of is controlled by `FailedRunHistoryLimit`. |
| `ScheduleState` | The current scheduling state of the application. Valid values are `FailedValidation` and `Scheduled`. |
| `Reason` | Human readable message on why the `ScheduledSparkApplication` is in the particular `ScheduleState`. |
