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

package common

// Spark environment variables.
const (
	EnvSparkHome = "SPARK_HOME"

	EnvKubernetesServiceHost = "KUBERNETES_SERVICE_HOST"

	EnvKubernetesServicePort = "KUBERNETES_SERVICE_PORT"
)

// Spark properties.
const (
	// SparkAppName is the configuration property for application name.
	SparkAppName = "spark.app.name"

	SparkDriverCores = "spark.driver.cores"

	SparkDriverMemory = "spark.driver.memory"

	SparkDriverMemoryOverhead = "spark.driver.memoryOverhead"

	SparkExecutorInstances = "spark.executor.instances"

	SparkExecutorEnvTemplate = "spark.executor.env.%s"

	SparkExecutorCores = "spark.executor.cores"

	SparkExecutorMemory = "spark.executor.memory"

	SparkExecutorMemoryOverhead = "spark.executor.memoryOverhead"

	SparkUIProxyBase = "spark.ui.proxyBase"

	SparkUIProxyRedirectURI = "spark.ui.proxyRedirectUri"
)

// Spark on Kubernetes properties.
const (

	// SparkKubernetesDriverMaster is the Spark configuration key for specifying the Kubernetes master the driver use
	// to manage executor pods and other Kubernetes resources.
	SparkKubernetesDriverMaster = "spark.kubernetes.driver.master"

	// SparkKubernetesNamespace is the configuration property for application namespace.
	SparkKubernetesNamespace = "spark.kubernetes.namespace"

	// SparkKubernetesContainerImage is the configuration property for specifying the unified container image.
	SparkKubernetesContainerImage = "spark.kubernetes.container.image"

	// SparkKubernetesContainerImagePullPolicy is the configuration property for specifying the container image pull policy.
	SparkKubernetesContainerImagePullPolicy = "spark.kubernetes.container.image.pullPolicy"

	// SparkKubernetesContainerImagePullSecrets is the configuration property for specifying the comma-separated list of image-pull
	// secrets.
	SparkKubernetesContainerImagePullSecrets = "spark.kubernetes.container.image.pullSecrets"

	SparkKubernetesAllocationBatchSize = "spark.kubernetes.allocation.batch.size"

	SparkKubernetesAllocationBatchDelay = "spark.kubernetes.allocation.batch.delay"

	// SparkKubernetesAuthenticateDriverServiceAccountName is the Spark configuration key for specifying name of the Kubernetes service
	// account used by the driver pod.
	SparkKubernetesAuthenticateDriverServiceAccountName = "spark.kubernetes.authenticate.driver.serviceAccountName"

	// account used by the executor pod.
	SparkKubernetesAuthenticateExecutorServiceAccountName = "spark.kubernetes.authenticate.executor.serviceAccountName"

	// SparkKubernetesDriverLabelPrefix is the Spark configuration key prefix for labels on the driver Pod.
	SparkKubernetesDriverLabelTemplate = "spark.kubernetes.driver.label.%s"

	// SparkKubernetesDriverAnnotationPrefix is the Spark configuration key prefix for annotations on the driver Pod.
	SparkKubernetesDriverAnnotationTemplate = "spark.kubernetes.driver.annotation.%s"

	// SparkKubernetesDriverServiceLabelPrefix is the key prefix of annotations to be added to the driver service.
	SparkKubernetesDriverServiceLabelTemplate = "spark.kubernetes.driver.service.label.%s"

	// SparkKubernetesDriverServiceAnnotationPrefix is the key prefix of annotations to be added to the driver service.
	SparkKubernetesDriverServiceAnnotationTemplate = "spark.kubernetes.driver.service.annotation.%s"

	// SparkKubernetesExecutorLabelPrefix is the Spark configuration key prefix for labels on the executor Pods.
	SparkKubernetesExecutorLabelTemplate = "spark.kubernetes.executor.label.%s"

	// SparkKubernetesExecutorAnnotationPrefix is the Spark configuration key prefix for annotations on the executor Pods.
	SparkKubernetesExecutorAnnotationTemplate = "spark.kubernetes.executor.annotation.%s"

	// SparkKubernetesDriverPodName is the Spark configuration key for driver pod name.
	SparkKubernetesDriverPodName = "spark.kubernetes.driver.pod.name"

	SparkKubernetesExecutorPodNamePrefix = "spark.kubernetes.executor.podNamePrefix"

	// SparkKubernetesDriverRequestCores is the configuration property for specifying the physical CPU request for the driver.
	SparkKubernetesDriverRequestCores = "spark.kubernetes.driver.request.cores"

	// SparkKubernetesDriverLimitCores is the configuration property for specifying the hard CPU limit for the driver pod.
	SparkKubernetesDriverLimitCores = "spark.kubernetes.driver.limit.cores"

	// SparkKubernetesExecutorRequestCores is the configuration property for specifying the physical CPU request for executors.
	SparkKubernetesExecutorRequestCores = "spark.kubernetes.executor.request.cores"

	// SparkKubernetesExecutorLimitCores is the configuration property for specifying the hard CPU limit for the executor pods.
	SparkKubernetesExecutorLimitCores = "spark.kubernetes.executor.limit.cores"

	// SparkKubernetesNodeSelectorPrefix is the configuration property prefix for specifying node selector for the pods.
	SparkKubernetesNodeSelectorTemplate = "spark.kubernetes.node.selector.%s"

	SparkKubernetesDriverNodeSelectorTemplate = "spark.kubernetes.driver.node.selector.%s"

	SparkKubernetesExecutorNodeSelectorTemplate = "spark.kubernetes.executor.node.selector.%s"

	// SparkKubernetesDriverEnvPrefix is the Spark configuration prefix for setting environment variables
	// into the driver.
	SparkKubernetesDriverEnvTemplate = "spark.kubernetes.driverEnv.%s"

	// SparkKubernetesDriverSecretsPrefix is the configuration property prefix for specifying secrets to be mounted into the
	// driver.
	SparkKubernetesDriverSecretsTemplate = "spark.kubernetes.driver.secrets.%s"

	// SparkKubernetesExecutorSecretsPrefix is the configuration property prefix for specifying secrets to be mounted into the
	// executors.
	SparkKubernetesExecutorSecretsTemplate = "spark.kubernetes.executor.secrets.%s"

	// SparkKubernetesDriverSecretKeyRefPrefix is the configuration property prefix for specifying environment variables
	// from SecretKeyRefs for the driver.
	SparkKubernetesDriverSecretKeyRefTemplate = "spark.kubernetes.driver.secretKeyRef.%s"

	// SparkKubernetesExecutorSecretKeyRefPrefix is the configuration property prefix for specifying environment variables
	// from SecretKeyRefs for the executors.
	SparkKubernetesExecutorSecretKeyRefTemplate = "spark.kubernetes.executor.secretKeyRef.%s"

	// SparkKubernetesDriverContainerImage is the configuration property for specifying a custom driver container image.
	SparkKubernetesDriverContainerImage = "spark.kubernetes.driver.container.image"

	// SparkKubernetesExecutorContainerImage is the configuration property for specifying a custom executor container image.
	SparkKubernetesExecutorContainerImage = "spark.kubernetes.executor.container.image"

	// SparkKubernetesDriverVolumesPrefix is the Spark volumes configuration for mounting a volume into the driver pod.
	SparkKubernetesDriverVolumesPrefix                = "spark.kubernetes.driver.volumes."
	SparkKubernetesDriverVolumesMountPathTemplate     = "spark.kubernetes.driver.volumes.%s.%s.mount.path"
	SparkKubernetesDriverVolumesMountSubPathTemplate  = "spark.kubernetes.driver.volumes.%s.%s.mount.subPath"
	SparkKubernetesDriverVolumesMountReadOnlyTemplate = "spark.kubernetes.driver.volumes.%s.%s.mount.readOnly"
	SparkKubernetesDriverVolumesOptionsTemplate       = "spark.kubernetes.driver.volumes.%s.%s.options.%s"

	// SparkKubernetesExecutorVolumesPrefix is the Spark volumes configuration for mounting a volume into the driver pod.
	SparkKubernetesExecutorVolumesPrefix                = "spark.kubernetes.executor.volumes."
	SparkKubernetesExecutorVolumesMountPathTemplate     = "spark.kubernetes.executor.volumes.%s.%s.mount.path"
	SparkKubernetesExecutorVolumesMountSubPathTemplate  = "spark.kubernetes.executor.volumes.%s.%s.mount.subPath"
	SparkKubernetesExecutorVolumesMountReadOnlyTemplate = "spark.kubernetes.executor.volumes.%s.%s.mount.readOnly"
	SparkKubernetesExecutorVolumesOptionsTemplate       = "spark.kubernetes.executor.volumes.%s.%s.options.%s"

	// SparkKubernetesMemoryOverheadFactor is the Spark configuration key for specifying memory overhead factor used for Non-JVM memory.
	SparkKubernetesMemoryOverheadFactor = "spark.kubernetes.memoryOverheadFactor"

	// SparkKubernetesPysparkPythonVersion is the Spark configuration key for specifying python version used.
	SparkKubernetesPysparkPythonVersion = "spark.kubernetes.pyspark.pythonVersion"

	SparkKubernetesDriverPodTemplateFile = "spark.kubernetes.driver.podTemplateFile"

	SparkKubernetesDriverPodTemplateContainerName = "spark.kubernetes.driver.podTemplateContainerName"

	SparkKubernetesExecutorPodTemplateFile = "spark.kubernetes.executor.podTemplateFile"

	SparkKubernetesExecutorPodTemplateContainerName = "spark.kubernetes.executor.podTemplateContainerName"

	SparkKubernetesDriverSchedulerName = "spark.kubernetes.driver.schedulerName"

	SparkKubernetesExecutorSchedulerName = "spark.kubernetes.executor.schedulerName"

	// SparkExecutorEnvVarConfigKeyPrefix is the Spark configuration prefix for setting environment variables
	// into the executor.
	SparkExecutorEnvVarConfigKeyPrefix = "spark.executorEnv."

	// SparkKubernetesInitContainerImage is the Spark configuration key for specifying a custom init-container image.
	SparkKubernetesInitContainerImage = "spark.kubernetes.initContainer.image"

	// SparkKubernetesMountDependenciesJarsDownloadDir is the Spark configuration key for specifying the download path in the driver and
	// executors for remote jars.
	SparkKubernetesMountDependenciesJarsDownloadDir = "spark.kubernetes.mountDependencies.jarsDownloadDir"

	// SparkKubernetesMountDependenciesFilesDownloadDir is the Spark configuration key for specifying the download path in the driver and
	// executors for remote files.
	SparkKubernetesMountDependenciesFilesDownloadDir = "spark.kubernetes.mountDependencies.filesDownloadDir"

	// SparkKubernetesMountDependenciesTimeout is the Spark configuration key for specifying the timeout in seconds of downloading
	// remote dependencies.
	SparkKubernetesMountDependenciesTimeout = "spark.kubernetes.mountDependencies.timeout"

	// SparkKubernetesMountDependenciesMaxSimultaneousDownloads is the Spark configuration key for specifying the maximum number of remote
	// dependencies to download.
	SparkKubernetesMountDependenciesMaxSimultaneousDownloads = "spark.kubernetes.mountDependencies.maxSimultaneousDownloads"

	// SparkKubernetesSubmissionWaitAppCompletion is the Spark configuration key for specifying whether to wait for application to complete.
	SparkKubernetesSubmissionWaitAppCompletion = "spark.kubernetes.submission.waitAppCompletion"

	// SparkDriverExtraJavaOptions is the Spark configuration key for a string of extra JVM options to pass to driver.
	SparkDriverExtraJavaOptions = "spark.driver.extraJavaOptions"

	// SparkExecutorExtraJavaOptions is the Spark configuration key for a string of extra JVM options to pass to executors.
	SparkExecutorExtraJavaOptions = "spark.executor.extraJavaOptions"

	// SparkKubernetesExecutorDeleteOnTermination is the Spark configuration for specifying whether executor pods should be deleted in case of failure or normal termination.
	SparkKubernetesExecutorDeleteOnTermination = "spark.kubernetes.executor.deleteOnTermination"
)

// Dynamic allocation properties.
// Ref: https://spark.apache.org/docs/latest/configuration.html#dynamic-allocation
const (
	// SparkDynamicAllocationEnabled is the Spark configuration key for specifying if dynamic
	// allocation is enabled or not.
	SparkDynamicAllocationEnabled = "spark.dynamicAllocation.enabled"

	SparkDynamicAllocationExecutorIdleTimeout = "spark.dynamicAllocation.executorIdleTimeout"

	SparkDynamicAllocationCachedExecutorIdleTimeout = "spark.dynamicAllocation.cachedExecutorIdleTimeout"

	// SparkDynamicAllocationInitialExecutors is the Spark configuration key for specifying
	// the initial number of executors to request if dynamic allocation is enabled.
	SparkDynamicAllocationInitialExecutors = "spark.dynamicAllocation.initialExecutors"

	// SparkDynamicAllocationMaxExecutors is the Spark configuration key for specifying the
	// upper bound of the number of executors to request if dynamic allocation is enabled.
	SparkDynamicAllocationMaxExecutors = "spark.dynamicAllocation.maxExecutors"

	// SparkDynamicAllocationMinExecutors is the Spark configuration key for specifying the
	// lower bound of the number of executors to request if dynamic allocation is enabled.
	SparkDynamicAllocationMinExecutors = "spark.dynamicAllocation.minExecutors"

	SparkDynamicAllocationExecutorAllocationRatio = "spark.dynamicAllocation.executorAllocationRatio"

	SparkDynamicAllocationSchedulerBacklogTimeout = "spark.dynamicAllocation.schedulerBacklogTimeout"

	SparkDynamicAllocationSustainedSchedulerBacklogTimeout = "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout"

	// SparkDynamicAllocationShuffleTrackingEnabled is the Spark configuration key for
	// specifying if shuffle data tracking is enabled.
	SparkDynamicAllocationShuffleTrackingEnabled = "spark.dynamicAllocation.shuffleTracking.enabled"

	// SparkDynamicAllocationShuffleTrackingTimeout is the Spark configuration key for specifying
	// the shuffle tracking timeout in milliseconds if shuffle tracking is enabled.
	SparkDynamicAllocationShuffleTrackingTimeout = "spark.dynamicAllocation.shuffleTracking.timeout"
)

const (
	// SparkRoleDriver is the value of the spark-role label for the driver.
	SparkRoleDriver = "driver"

	// SparkRoleExecutor is the value of the spark-role label for the executors.
	SparkRoleExecutor = "executor"
)

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

	// EnvSparkConfDir is the environment variable to add to the driver and executor Pods that point
	// to the directory where the Spark ConfigMap is mounted.
	EnvSparkConfDir = "SPARK_CONF_DIR"

	// EnvHadoopConfDir is the environment variable to add to the driver and executor Pods that point
	// to the directory where the Hadoop ConfigMap is mounted.
	EnvHadoopConfDir = "HADOOP_CONF_DIR"
)

const (
	// LabelSparkApplicationSelector is the AppID set by the spark-distribution on the driver/executors Pods.
	LabelSparkApplicationSelector = "spark-app-selector"

	// LabelSparkRole is the driver/executor label set by the operator/spark-distribution on the driver/executors Pods.
	LabelSparkRole = "spark-role"

	// LabelAnnotationPrefix is the prefix of every labels and annotations added by the controller.
	LabelAnnotationPrefix = "sparkoperator.k8s.io/"

	// LabelSparkAppName is the name of the label for the SparkApplication object name.
	LabelSparkAppName = LabelAnnotationPrefix + "app-name"

	// LabelScheduledSparkAppName is the name of the label for the ScheduledSparkApplication object name.
	LabelScheduledSparkAppName = LabelAnnotationPrefix + "scheduled-app-name"

	// LabelLaunchedBySparkOperator is a label on Spark pods launched through the Spark Operator.
	LabelLaunchedBySparkOperator = LabelAnnotationPrefix + "launched-by-spark-operator"

	// LabelMutatedBySparkOperator is a label on Spark pods that need to be mutated by webhook.
	LabelMutatedBySparkOperator = LabelAnnotationPrefix + "mutated-by-spark-operator"

	// LabelSubmissionID is the label that records the submission ID of the current run of an application.
	LabelSubmissionID = LabelAnnotationPrefix + "submission-id"

	// LabelSparkExecutorID is the label that records executor pod ID
	LabelSparkExecutorID = "spark-exec-id"
)

const (
	// SparkDriverContainerName is name of driver container in spark driver pod.
	SparkDriverContainerName = "spark-kubernetes-driver"

	// SparkExecutorContainerName is name of executor container in spark executor pod.
	SparkExecutorContainerName = "executor"

	// Spark3DefaultExecutorContainerName is the default executor container name in
	// Spark 3.x, which allows the container name to be configured through the pod
	// template support.
	Spark3DefaultExecutorContainerName = "spark-kubernetes-executor"

	// SparkLocalDirVolumePrefix is the volume name prefix for "scratch" space directory.
	SparkLocalDirVolumePrefix = "spark-local-dir-"
)

const (
	SparkUIPortKey = "spark.ui.port"

	DefaultSparkWebUIPort int32 = 4040

	DefaultSparkWebUIPortName = "spark-driver-ui-port"
)

// https://spark.apache.org/docs/latest/configuration.html
const (
	DefaultCPUMilliCores = 1000

	DefaultMemoryBytes = 1 << 30 // 1 Gi

	DefaultJVMMemoryOverheadFactor = 0.1

	DefaultNonJVMMemoryOverheadFactor = 0.4

	MinMemoryOverhead = 384 * (1 << 20) // 384 Mi
)

const (
	// EnvGoogleApplicationCredentials is the environment variable used by the
	// Application Default Credentials mechanism. More details can be found at
	// https://developers.google.com/identity/protocols/application-default-credentials.
	EnvGoogleApplicationCredentials = "GOOGLE_APPLICATION_CREDENTIALS"

	// ServiceAccountJSONKeyFileName is the assumed name of the service account
	// Json key file. This name is added to the service account secret mount path to
	// form the path to the Json key file referred to by GOOGLE_APPLICATION_CREDENTIALS.
	ServiceAccountJSONKeyFileName = "key.json"

	// EnvHadoopTokenFileLocation is the environment variable for specifying the location
	// where the file storing the Hadoop delegation token is located.
	EnvHadoopTokenFileLocation = "HADOOP_TOKEN_FILE_LOCATION"

	// HadoopDelegationTokenFileName is the assumed name of the file storing the Hadoop
	// delegation token. This name is added to the delegation token secret mount path to
	// form the path to the file referred to by HADOOP_TOKEN_FILE_LOCATION.
	HadoopDelegationTokenFileName = "hadoop.token"
)
