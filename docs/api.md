# SparkApplication API

The Spark Operator uses a [CustomResourceDefinition](https://kubernetes.io/docs/concepts/api-extension/custom-resources/) 
named `SparkApplication` for specifying Spark applications to be run in a Kubernetes cluster. Similarly to other kinds of Kubernetes resources, a `SparkApplication` consists of a specification in a `Spec` field of type `SparkApplicationSpec`and a `Status` field of type `SparkApplicationStatus`. The definition is organized in the following structure. The v1alpha1 version of the API definition is implemented [here](../pkg/apis/sparkoperator.k8s.io/v1alpha1/types.go).

```
SparkApplication
|__ SparkApplicationSpec
    |__ DriverSpec
        |__ SparkPodSpec
    |__ ExecutorSpec
        |__ SparkPodSpec
    |__ Dependencies
|__ SparkApplicationStatus
    |__ DriverInfo    
```

## API Definition

### `SparkApplicationSpec`

A `SparkApplicationSpec` has the following top-level fields:

| Field | Spark configuration property or `spark-submit` option | Note |
| ------------- | ------------- | ------------- |
| `Type`  | N/A  | The type of the Spark application. Valid values are `Java`, `Scala`, `Python`, and `R`. |
| `Mode`  | `--mode` | Spark deployment mode. Valid values are `cluster` and `client`. |
| `Image` | `spark.kubernetes.container.image` | Unified container image for the driver, executor, and init-container. |
| `InitContainerImage` | `spark.kubernetes.initContainer.image` | Custom init-container image. |
| `ImagePullPolicy` | `spark.kubernetes.container.image.pullPolicy` | Container image pull policy. |
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
| `RestartPolicy` | N/A | The policy regarding if and in which conditions the controller should restart a terminated application. Valid values are `Never`, `Always`, and `OnFailure`. |
| `NodeSelector` | `spark.kubernetes.node.selector.[labelKey]` | Node selector of the driver pod and executor pods, with key `labelKey` and value as the label's value. |
| `MaxSubmissionRetries` | N/A | The maximum number of times to retry a failed submission. |
| `SubmissionRetryInterval` | N/A | The unit of intervals in seconds between submission retries. Depending on the implementation, the actual interval between two submission retries may be a multiple of `SubmissionRetryInterval`, e.g., if linear or exponential backoff is used. |

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

#### `SparkPodSpec`

A `SparkPodSpec` defines common attributes of a driver or executor pod, summarized in the following table.

| Field | Spark configuration property or `spark-submit` option | Note |
| ------------- | ------------- | ------------- |
| `Cores` | `spark.driver.cores` or `spark.executor.cores` | Number of CPU cores for the driver or executor pod. |
| `CoreLimit` | `spark.kubernetes.driver.limit.cores` or `spark.kubernetes.executor.limit.cores` | Hard limit on the number of CPU cores for the driver or executor pod. |
| `Memory` | `spark.driver.memory` or `spark.executor.memory` | Amount of memory to request for the driver or executor pod. |
| `Image` | `spark.kubernetes.driver.container.image` or `spark.kubernetes.executor.container.image` | Custom container image for the driver or executor. |
| `ConfigMaps` | N/A | A map of Kubernetes ConfigMaps to mount into the driver or executor pod. Keys are ConfigMap names and values are mount paths. |
| `Secrets` | `spark.kubernetes.driver.secrets.[SecretName]` or `spark.kubernetes.executor.secrets.[SecretName]` | A map of Kubernetes secrets to mount into the driver or executor pod. Keys are secret names and values specify the mount paths and secret types. |
| `EnvVars` | `spark.kubernetes.driverEnv.[EnvironmentVariableName]` or `spark.executorEnv.[EnvironmentVariableName]` | A map of environment variables to add to the driver or executor pod. Keys are variable names and values are variable values. |
| `Labels` | `spark.kubernetes.driver.label.[LabelName]` or `spark.kubernetes.executor.label.[LabelName]` | A map of Kubernetes labels to add to the driver or executor pod. Keys are label names and values are label values. |
| `Annotations` | `spark.kubernetes.driver.annotation.[AnnotationName]` or `spark.kubernetes.executor.annotation.[AnnotationName]` | A map of Kubernetes annotations to add to the driver or executor pod. Keys are annotation names and values are annotation values. |
| `VolumeMounts` | N/A | List of Kubernetes [volume mounts](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.9/#volumemount-v1-core) for volumes that should be mounted to the pod. |

#### `Dependencies`

A `Dependencies` specifies the various types of dependencies of a Spark application in a central place.

| Field | Spark configuration property or `spark-submit` option | Note |
| ------------- | ------------- | ------------- |
| `Jars` | `spark.jars` or `--jars` | List of jars the application depends on. |
| `Files` | `spark.files` or `--files` | List of files the application depends on. |

### `SparkApplicationStatus`

A `SparkApplicationStatus` captures the status of a Spark application including the state of every executors.

| Field | Note |
| ------------- | ------------- |
| `AppID` | A randomly generated ID used to group all Kubernetes resources of an application. |
| `SubmissionTime` | Time the application is submitted to run. |
| `CompletionTime` | Time the application completes (if it does). |
| `DriverInfo` | A [`DriverInfo`](#driverinfo) field. |
| `AppState` | Current state of the application. |
| `ExecutorState` | A map of executor pod names to executor state. |
| `SubmissionRetries` | The number of submission retries for an application. |

#### `DriverInfo`

A `DriverInfo` captures information about the driver pod and the Spark web UI running in the driver.

| Field | Note |
| ------------- | ------------- |
| `WebUIServiceName` | Name of the service for the Spark web UI. |
| `WebUIPort` | Port on which the Spark web UI runs. |
| `WebUIAddress` | Address to access the web UI from outside the cluster. |
| `PodName` | Name of the driver pod. |
