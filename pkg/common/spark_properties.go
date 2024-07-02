/*
Copyright 2024 The Kubeflow authors.

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

// Spark properties
const (
	// SparkAppName is the configuration property for application name.
	SparkAppName = "spark.app.name"

	SparkDriverCores = "spark.driver.cores"

	SparkDriverMemory = "spark.driver.memory"

	SparkDriverMemoryOverhead = "spark.driver.memoryOverhead"

	SparkExecutorInstances = "spark.executor.instances"

	SparkExecutorCores = "spark.executor.cores"

	SparkExecutorMemory = "spark.executor.memory"

	SparkExecutorMemoryOverhead = "spark.executor.memoryOverhead"

	SparkUIProxyBase = "spark.ui.proxyBase"

	SparkUIProxyRedirectURI = "spark.ui.proxyRedirectUri"
)

// Spark on Kubernetes properties
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
	SparkKubernetesDriverLabelPrefix   = "spark.kubernetes.driver.label."
	SparkKubernetesDriverLabelTemplate = "spark.kubernetes.driver.label.%s"

	// SparkKubernetesDriverAnnotationPrefix is the Spark configuration key prefix for annotations on the driver Pod.
	SparkKubernetesDriverAnnotationPrefix   = "spark.kubernetes.driver.annotation."
	SparkKubernetesDriverAnnotationTemplate = "spark.kubernetes.driver.annotation.%s"

	// SparkKubernetesDriverServiceLabelPrefix is the key prefix of annotations to be added to the driver service.
	SparkKubernetesDriverServiceLabelPrefix   = "spark.kubernetes.driver.service.label."
	SparkKubernetesDriverServiceLabelTemplate = "spark.kubernetes.driver.service.label.%s"

	// SparkKubernetesDriverServiceAnnotationPrefix is the key prefix of annotations to be added to the driver service.
	SparkKubernetesDriverServiceAnnotationPrefix   = "spark.kubernetes.driver.service.annotation."
	SparkKubernetesDriverServiceAnnotationTemplate = "spark.kubernetes.driver.service.annotation.%s"

	// SparkKubernetesExecutorLabelPrefix is the Spark configuration key prefix for labels on the executor Pods.
	SparkKubernetesExecutorLabelPrefix   = "spark.kubernetes.executor.label."
	SparkKubernetesExecutorLabelTemplate = "spark.kubernetes.executor.label.%s"

	// SparkKubernetesExecutorAnnotationPrefix is the Spark configuration key prefix for annotations on the executor Pods.
	SparkKubernetesExecutorAnnotationPrefix   = "spark.kubernetes.executor.annotation."
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
	SparkKubernetesNodeSelectorPrefix   = "spark.kubernetes.node.selector."
	SparkKubernetesNodeSelectorTemplate = "spark.kubernetes.node.selector.%s"

	SparkKubernetesDriverNodeSelectorPrefix   = "spark.kubernetes.driver.node.selector."
	SparkKubernetesDriverNodeSelectorTemplate = "spark.kubernetes.driver.node.selector.%s"

	SparkKubernetesExecutorNodeSelectorPrefix   = "spark.kubernetes.executor.node.selector."
	SparkKubernetesExecutorNodeSelectorTemplate = "spark.kubernetes.executor.node.selector.%s"

	// SparkKubernetesDriverEnvPrefix is the Spark configuration prefix for setting environment variables
	// into the driver.
	SparkKubernetesDriverEnvPrefix   = "spark.kubernetes.driverEnv."
	SparkKubernetesDriverEnvTemplate = "spark.kubernetes.driverEnv.%s"

	// SparkKubernetesDriverSecretsPrefix is the configuration property prefix for specifying secrets to be mounted into the
	// driver.
	SparkKubernetesDriverSecretsPrefix   = "spark.kubernetes.driver.secrets."
	SparkKubernetesDriverSecretsTemplate = "spark.kubernetes.driver.secrets.%s"

	// SparkKubernetesExecutorSecretsPrefix is the configuration property prefix for specifying secrets to be mounted into the
	// executors.
	SparkKubernetesExecutorSecretsPrefix   = "spark.kubernetes.executor.secrets."
	SparkKubernetesExecutorSecretsTemplate = "spark.kubernetes.executor.secrets.%s"

	// SparkKubernetesDriverSecretKeyRefPrefix is the configuration property prefix for specifying environment variables
	// from SecretKeyRefs for the driver.
	SparkKubernetesDriverSecretKeyRefPrefix   = "spark.kubernetes.driver.secretKeyRef."
	SparkKubernetesDriverSecretKeyRefTemplate = "spark.kubernetes.driver.secretKeyRef.%s"

	// SparkKubernetesExecutorSecretKeyRefPrefix is the configuration property prefix for specifying environment variables
	// from SecretKeyRefs for the executors.
	SparkKubernetesExecutorSecretKeyRefPrefix   = "spark.kubernetes.executor.secretKeyRef."
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

	// SparkKubernetesExecutorDeleteOnTermination is the Spark configuration for specifying whether executor pods should be deleted in case of failure or normal termination
	SparkKubernetesExecutorDeleteOnTermination = "spark.kubernetes.executor.deleteOnTermination"
)

// Dynamic allocation properties.
// Ref: https://spark.apache.org/docs/latest/configuration.html#dynamic-allocation
const (
	// SparkDynamicAllocationEnabled is the Spark configuration key for specifying if dynamic
	// allocation is enabled or not.
	// @since 1.2.0
	SparkDynamicAllocationEnabled = "spark.dynamicAllocation.enabled"

	// @since 1.4.0
	SparkDynamicAllocationExecutorIdleTimeout = "spark.dynamicAllocation.executorIdleTimeout"

	// @since 1.4.0
	SparkDynamicAllocationCachedExecutorIdleTimeout = "spark.dynamicAllocation.cachedExecutorIdleTimeout"

	// SparkDynamicAllocationInitialExecutors is the Spark configuration key for specifying
	// the initial number of executors to request if dynamic allocation is enabled.
	// @since 1.3.0
	SparkDynamicAllocationInitialExecutors = "spark.dynamicAllocation.initialExecutors"

	// SparkDynamicAllocationMaxExecutors is the Spark configuration key for specifying the
	// upper bound of the number of executors to request if dynamic allocation is enabled.
	// @since 1.2.0
	SparkDynamicAllocationMaxExecutors = "spark.dynamicAllocation.maxExecutors"

	// SparkDynamicAllocationMinExecutors is the Spark configuration key for specifying the
	// lower bound of the number of executors to request if dynamic allocation is enabled.
	// @since 1.2.0
	SparkDynamicAllocationMinExecutors = "spark.dynamicAllocation.minExecutors"

	// @since 2.4.0
	SparkDynamicAllocationExecutorAllocationRatio = "spark.dynamicAllocation.executorAllocationRatio"

	// @since 1.2.0
	SparkDynamicAllocationSchedulerBacklogTimeout = "spark.dynamicAllocation.schedulerBacklogTimeout"

	// @since 1.2.0
	SparkDynamicAllocationSustainedSchedulerBacklogTimeout = "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout"

	// SparkDynamicAllocationShuffleTrackingEnabled is the Spark configuration key for
	// specifying if shuffle data tracking is enabled.
	// @since 3.0.0
	SparkDynamicAllocationShuffleTrackingEnabled = "spark.dynamicAllocation.shuffleTracking.enabled"

	// SparkDynamicAllocationShuffleTrackingTimeout is the Spark configuration key for specifying
	// the shuffle tracking timeout in milliseconds if shuffle tracking is enabled.
	// @since 3.0.0
	SparkDynamicAllocationShuffleTrackingTimeout = "spark.dynamicAllocation.shuffleTracking.timeout"
)
