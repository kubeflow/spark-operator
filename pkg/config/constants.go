/*
Copyright 2017 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package config

const (
	// DefaultSparkConfDir is the default directory for Spark configuration files if not specified.
	// This directory is where the Spark ConfigMap is mounted in the driver and executor containers.
	DefaultSparkConfDir = "/etc/spark/conf"
	// SparkConfigMapVolumeName is the name of the ConfigMap volume of Spark configuration files.
	SparkConfigMapVolumeName = "spark-configmap-volume"
	// DefaultHadoopConfDir is the default directory for Spark configuration files if not specified.
	// This directory is where the Hadoop ConfigMap is mounted in the driver and executor containers.
	DefaultHadoopConfDir = "/etc/hadoop/conf"
	// HadoopConfigMapVolumeName is the name of the ConfigMap volume of Hadoop configuration files.
	HadoopConfigMapVolumeName = "hadoop-configmap-volume"
	// SparkConfDirEnvVar is the environment variable to add to the driver and executor Pods that point
	// to the directory where the Spark ConfigMap is mounted.
	SparkConfDirEnvVar = "SPARK_CONF_DIR"
	// HadoopConfDirEnvVar is the environment variable to add to the driver and executor Pods that point
	// to the directory where the Hadoop ConfigMap is mounted.
	HadoopConfDirEnvVar = "HADOOP_CONF_DIR"
)

const (
	// LabelAnnotationPrefix is the prefix of every labels and annotations added by the controller.
	LabelAnnotationPrefix = "sparkoperator.k8s.io/"
	// SparkAppNameLabel is the name of the label for the SparkApplication object name.
	SparkAppNameLabel = LabelAnnotationPrefix + "app-name"
	// ScheduledSparkAppNameLabel is the name of the label for the ScheduledSparkApplication object name.
	ScheduledSparkAppNameLabel = LabelAnnotationPrefix + "scheduled-app-name"
	// LaunchedBySparkOperatorLabel is a label on Spark pods launched through the Spark Operator.
	LaunchedBySparkOperatorLabel = LabelAnnotationPrefix + "launched-by-spark-operator"
	// SparkApplicationSelectorLabel is the AppID set by the spark-distribution on the driver/executors Pods.
	SparkApplicationSelectorLabel = "spark-app-selector"
	// SparkRoleLabel is the driver/executor label set by the operator/spark-distribution on the driver/executors Pods.
	SparkRoleLabel = "spark-role"
	// SparkDriverRole is the value of the spark-role label for the driver.
	SparkDriverRole = "driver"
	// SparkExecutorRole is the value of the spark-role label for the executors.
	SparkExecutorRole = "executor"
	// SubmissionIDLabel is the label that records the submission ID of the current run of an application.
	SubmissionIDLabel = LabelAnnotationPrefix + "submission-id"
)

const (
	// SparkAppNameKey is the configuration property for application name.
	SparkAppNameKey = "spark.app.name"
	// SparkAppNamespaceKey is the configuration property for application namespace.
	SparkAppNamespaceKey = "spark.kubernetes.namespace"
	// SparkContainerImageKey is the configuration property for specifying the unified container image.
	SparkContainerImageKey = "spark.kubernetes.container.image"
	// SparkImagePullSecretKey is the configuration property for specifying the comma-separated list of image-pull
	// secrets.
	SparkImagePullSecretKey = "spark.kubernetes.container.image.pullSecrets"
	// SparkContainerImagePullPolicyKey is the configuration property for specifying the container image pull policy.
	SparkContainerImagePullPolicyKey = "spark.kubernetes.container.image.pullPolicy"
	// SparkNodeSelectorKeyPrefix is the configuration property prefix for specifying node selector for the pods.
	SparkNodeSelectorKeyPrefix = "spark.kubernetes.node.selector."
	// SparkDriverContainerImageKey is the configuration property for specifying a custom driver container image.
	SparkDriverContainerImageKey = "spark.kubernetes.driver.container.image"
	// SparkExecutorContainerImageKey is the configuration property for specifying a custom executor container image.
	SparkExecutorContainerImageKey = "spark.kubernetes.executor.container.image"
	// SparkDriverCoreRequestKey is the configuration property for specifying the physical CPU request for the driver.
	SparkDriverCoreRequestKey = "spark.kubernetes.driver.request.cores"
	// SparkExecutorCoreRequestKey is the configuration property for specifying the physical CPU request for executors.
	SparkExecutorCoreRequestKey = "spark.kubernetes.executor.request.cores"
	// SparkDriverCoreLimitKey is the configuration property for specifying the hard CPU limit for the driver pod.
	SparkDriverCoreLimitKey = "spark.kubernetes.driver.limit.cores"
	// SparkExecutorCoreLimitKey is the configuration property for specifying the hard CPU limit for the executor pods.
	SparkExecutorCoreLimitKey = "spark.kubernetes.executor.limit.cores"
	// SparkDriverSecretKeyPrefix is the configuration property prefix for specifying secrets to be mounted into the
	// driver.
	SparkDriverSecretKeyPrefix = "spark.kubernetes.driver.secrets."
	// SparkExecutorSecretKeyPrefix is the configuration property prefix for specifying secrets to be mounted into the
	// executors.
	SparkExecutorSecretKeyPrefix = "spark.kubernetes.executor.secrets."
	// SparkDriverSecretKeyRefKeyPrefix is the configuration property prefix for specifying environment variables
	// from SecretKeyRefs for the driver.
	SparkDriverSecretKeyRefKeyPrefix = "spark.kubernetes.driver.secretKeyRef."
	// SparkExecutorSecretKeyRefKeyPrefix is the configuration property prefix for specifying environment variables
	// from SecretKeyRefs for the executors.
	SparkExecutorSecretKeyRefKeyPrefix = "spark.kubernetes.executor.secretKeyRef."
	// SparkDriverEnvVarConfigKeyPrefix is the Spark configuration prefix for setting environment variables
	// into the driver.
	SparkDriverEnvVarConfigKeyPrefix = "spark.kubernetes.driverEnv."
	// SparkExecutorEnvVarConfigKeyPrefix is the Spark configuration prefix for setting environment variables
	// into the executor.
	SparkExecutorEnvVarConfigKeyPrefix = "spark.executorEnv."
	// SparkDriverAnnotationKeyPrefix is the Spark configuration key prefix for annotations on the driver Pod.
	SparkDriverAnnotationKeyPrefix = "spark.kubernetes.driver.annotation."
	// SparkExecutorAnnotationKeyPrefix is the Spark configuration key prefix for annotations on the executor Pods.
	SparkExecutorAnnotationKeyPrefix = "spark.kubernetes.executor.annotation."
	// SparkDriverLabelKeyPrefix is the Spark configuration key prefix for labels on the driver Pod.
	SparkDriverLabelKeyPrefix = "spark.kubernetes.driver.label."
	// SparkExecutorLabelKeyPrefix is the Spark configuration key prefix for labels on the executor Pods.
	SparkExecutorLabelKeyPrefix = "spark.kubernetes.executor.label."
	// SparkDriverVolumesPrefix is the Spark volumes configuration for mounting a volume into the driver pod.
	SparkDriverVolumesPrefix = "spark.kubernetes.driver.volumes."
	// SparkExecutorVolumesPrefix is the Spark volumes configuration for mounting a volume into the driver pod.
	SparkExecutorVolumesPrefix = "spark.kubernetes.executor.volumes."
	// SparkDriverPodNameKey is the Spark configuration key for driver pod name.
	SparkDriverPodNameKey = "spark.kubernetes.driver.pod.name"
	// SparkDriverServiceAccountName is the Spark configuration key for specifying name of the Kubernetes service
	// account used by the driver pod.
	SparkDriverServiceAccountName = "spark.kubernetes.authenticate.driver.serviceAccountName"
	// account used by the executor pod.
	SparkExecutorAccountName = "spark.kubernetes.authenticate.executor.serviceAccountName"
	// SparkInitContainerImage is the Spark configuration key for specifying a custom init-container image.
	SparkInitContainerImage = "spark.kubernetes.initContainer.image"
	// SparkJarsDownloadDir is the Spark configuration key for specifying the download path in the driver and
	// executors for remote jars.
	SparkJarsDownloadDir = "spark.kubernetes.mountDependencies.jarsDownloadDir"
	// SparkFilesDownloadDir is the Spark configuration key for specifying the download path in the driver and
	// executors for remote files.
	SparkFilesDownloadDir = "spark.kubernetes.mountDependencies.filesDownloadDir"
	// SparkDownloadTimeout is the Spark configuration key for specifying the timeout in seconds of downloading
	// remote dependencies.
	SparkDownloadTimeout = "spark.kubernetes.mountDependencies.timeout"
	// SparkMaxSimultaneousDownloads is the Spark configuration key for specifying the maximum number of remote
	// dependencies to download.
	SparkMaxSimultaneousDownloads = "spark.kubernetes.mountDependencies.maxSimultaneousDownloads"
	// SparkWaitAppCompletion is the Spark configuration key for specifying whether to wait for application to complete.
	SparkWaitAppCompletion = "spark.kubernetes.submission.waitAppCompletion"
	// SparkPythonVersion is the Spark configuration key for specifying python version used.
	SparkPythonVersion = "spark.kubernetes.pyspark.pythonVersion"
	// SparkMemoryOverheadFactor is the Spark configuration key for specifying memory overhead factor used for Non-JVM memory.
	SparkMemoryOverheadFactor = "spark.kubernetes.memoryOverheadFactor"
	// SparkDriverJavaOptions is the Spark configuration key for a string of extra JVM options to pass to driver.
	SparkDriverJavaOptions = "spark.driver.extraJavaOptions"
	// SparkExecutorJavaOptions is the Spark configuration key for a string of extra JVM options to pass to executors.
	SparkExecutorJavaOptions = "spark.executor.extraJavaOptions"
	// SparkExecutorDeleteOnTermination is the Spark configuration for specifying whether executor pods should be deleted in case of failure or normal termination
	SparkExecutorDeleteOnTermination = "spark.kubernetes.executor.deleteOnTermination"
	// SparkDriverKubernetesMaster is the Spark configuration key for specifying the Kubernetes master the driver use
	// to manage executor pods and other Kubernetes resources.
	SparkDriverKubernetesMaster = "spark.kubernetes.driver.master"
	// SparkDriverServiceAnnotationKeyPrefix is the key prefix of annotations to be added to the driver service.
	SparkDriverServiceAnnotationKeyPrefix = "spark.kubernetes.driver.service.annotation."
	// SparkDynamicAllocationEnabled is the Spark configuration key for specifying if dynamic
	// allocation is enabled or not.
	SparkDynamicAllocationEnabled = "spark.dynamicAllocation.enabled"
	// SparkDynamicAllocationShuffleTrackingEnabled is the Spark configuration key for
	// specifying if shuffle data tracking is enabled.
	SparkDynamicAllocationShuffleTrackingEnabled = "spark.dynamicAllocation.shuffleTracking.enabled"
	// SparkDynamicAllocationShuffleTrackingTimeout is the Spark configuration key for specifying
	// the shuffle tracking timeout in milliseconds if shuffle tracking is enabled.
	SparkDynamicAllocationShuffleTrackingTimeout = "spark.dynamicAllocation.shuffleTracking.timeout"
	// SparkDynamicAllocationInitialExecutors is the Spark configuration key for specifying
	// the initial number of executors to request if dynamic allocation is enabled.
	SparkDynamicAllocationInitialExecutors = "spark.dynamicAllocation.initialExecutors"
	// SparkDynamicAllocationMinExecutors is the Spark configuration key for specifying the
	// lower bound of the number of executors to request if dynamic allocation is enabled.
	SparkDynamicAllocationMinExecutors = "spark.dynamicAllocation.minExecutors"
	// SparkDynamicAllocationMaxExecutors is the Spark configuration key for specifying the
	// upper bound of the number of executors to request if dynamic allocation is enabled.
	SparkDynamicAllocationMaxExecutors = "spark.dynamicAllocation.maxExecutors"
)

const (
	// GoogleApplicationCredentialsEnvVar is the environment variable used by the
	// Application Default Credentials mechanism. More details can be found at
	// https://developers.google.com/identity/protocols/application-default-credentials.
	GoogleApplicationCredentialsEnvVar = "GOOGLE_APPLICATION_CREDENTIALS"
	// ServiceAccountJSONKeyFileName is the assumed name of the service account
	// Json key file. This name is added to the service account secret mount path to
	// form the path to the Json key file referred to by GOOGLE_APPLICATION_CREDENTIALS.
	ServiceAccountJSONKeyFileName = "key.json"
	// HadoopTokenFileLocationEnvVar is the environment variable for specifying the location
	// where the file storing the Hadoop delegation token is located.
	HadoopTokenFileLocationEnvVar = "HADOOP_TOKEN_FILE_LOCATION"
	// HadoopDelegationTokenFileName is the assumed name of the file storing the Hadoop
	// delegation token. This name is added to the delegation token secret mount path to
	// form the path to the file referred to by HADOOP_TOKEN_FILE_LOCATION.
	HadoopDelegationTokenFileName = "hadoop.token"
)

const (
	// PrometheusConfigMapNameSuffix is the name prefix of the Prometheus ConfigMap.
	PrometheusConfigMapNameSuffix = "prom-conf"
	// PrometheusConfigMapMountPath is the mount path of the Prometheus ConfigMap.
	PrometheusConfigMapMountPath = "/etc/metrics/conf"
)

// DefaultMetricsProperties is the default content of metrics.properties.
const DefaultMetricsProperties = `
*.sink.jmx.class=org.apache.spark.metrics.sink.JmxSink
driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource
executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource`

// DefaultPrometheusConfiguration is the default content of prometheus.yaml.
const DefaultPrometheusConfiguration = `
lowercaseOutputName: true
attrNameSnakeCase: true
rules:
  - pattern: metrics<name=(\S+)\.(\S+)\.driver\.(BlockManager|DAGScheduler|jvm)\.(\S+)><>Value
    name: spark_driver_$3_$4
    type: GAUGE
    labels:
      app_namespace: "$1"
      app_id: "$2"
  - pattern: metrics<name=(\S+)\.(\S+)\.driver\.(\S+)\.StreamingMetrics\.streaming\.(\S+)><>Value
    name: spark_streaming_driver_$4
    type: GAUGE
    labels:
      app_namespace: "$1"
      app_id: "$2"
  - pattern: metrics<name=(\S+)\.(\S+)\.driver\.spark\.streaming\.(\S+)\.(\S+)><>Value
    name: spark_structured_streaming_driver_$4
    type: GAUGE
    labels:
      app_namespace: "$1"
      app_id: "$2"
      query_name: "$3"
  - pattern: metrics<name=(\S+)\.(\S+)\.(\S+)\.executor\.(\S+)><>Value
    name: spark_executor_$4
    type: GAUGE
    labels:
      app_namespace: "$1"
      app_id: "$2"
      executor_id: "$3"
  - pattern: metrics<name=(\S+)\.(\S+)\.driver\.DAGScheduler\.(.*)><>Count
    name: spark_driver_DAGScheduler_$3_count
    type: COUNTER
    labels:
      app_namespace: "$1"
      app_id: "$2"
  - pattern: metrics<name=(\S+)\.(\S+)\.driver\.HiveExternalCatalog\.(.*)><>Count
    name: spark_driver_HiveExternalCatalog_$3_count
    type: COUNTER
    labels:
      app_namespace: "$1"
      app_id: "$2"
  - pattern: metrics<name=(\S+)\.(\S+)\.driver\.CodeGenerator\.(.*)><>Count
    name: spark_driver_CodeGenerator_$3_count
    type: COUNTER
    labels:
      app_namespace: "$1"
      app_id: "$2"
  - pattern: metrics<name=(\S+)\.(\S+)\.driver\.LiveListenerBus\.(.*)><>Count
    name: spark_driver_LiveListenerBus_$3_count
    type: COUNTER
    labels:
      app_namespace: "$1"
      app_id: "$2"
  - pattern: metrics<name=(\S+)\.(\S+)\.driver\.LiveListenerBus\.(.*)><>Value
    name: spark_driver_LiveListenerBus_$3
    type: GAUGE
    labels:
      app_namespace: "$1"
      app_id: "$2"
  - pattern: metrics<name=(\S+)\.(\S+)\.(.*)\.executor\.(.*)><>Count
    name: spark_executor_$4_count
    type: COUNTER
    labels:
      app_namespace: "$1"
      app_id: "$2"
      executor_id: "$3"
  - pattern: metrics<name=(\S+)\.(\S+)\.([0-9]+)\.(jvm|NettyBlockTransfer)\.(.*)><>Value
    name: spark_executor_$4_$5
    type: GAUGE
    labels:
      app_namespace: "$1"
      app_id: "$2"
      executor_id: "$3"
  - pattern: metrics<name=(\S+)\.(\S+)\.([0-9]+)\.HiveExternalCatalog\.(.*)><>Count
    name: spark_executor_HiveExternalCatalog_$4_count
    type: COUNTER
    labels:
      app_namespace: "$1"
      app_id: "$2"
      executor_id: "$3"
  - pattern: metrics<name=(\S+)\.(\S+)\.([0-9]+)\.CodeGenerator\.(.*)><>Count
    name: spark_executor_CodeGenerator_$4_count
    type: COUNTER
    labels:
      app_namespace: "$1"
      app_id: "$2"
      executor_id: "$3"
`

// DefaultPrometheusJavaAgentPort is the default port used by the Prometheus JMX exporter.
const DefaultPrometheusJavaAgentPort int32 = 8090

// DefaultPrometheusPortProtocol is the default protocol used by the Prometheus JMX exporter.
const DefaultPrometheusPortProtocol string = "TCP"

// DefaultPrometheusPortName is the default port name used by the Prometheus JMX exporter.
const DefaultPrometheusPortName string = "jmx-exporter"

const (
	// SparkDriverContainerName is name of driver container in spark driver pod
	SparkDriverContainerName = "spark-kubernetes-driver"
	// SparkExecutorContainerName is name of executor container in spark executor pod
	SparkExecutorContainerName = "executor"
	// Spark3DefaultExecutorContainerName is the default executor container name in
	// Spark 3.x, which allows the container name to be configured through the pod
	// template support.
	Spark3DefaultExecutorContainerName = "spark-kubernetes-executor"
	// SparkLocalDirVolumePrefix is the volume name prefix for "scratch" space directory
	SparkLocalDirVolumePrefix = "spark-local-dir-"
)
