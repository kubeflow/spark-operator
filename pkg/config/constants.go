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
	// SparkConfigMapAnnotation is the name of the annotation added to the driver and executor Pods
	// that indicates the presence of a Spark ConfigMap that should be mounted to the driver and
	// executor Pods with the environment variable SPARK_CONF_DIR set to point to the mount path.
	SparkConfigMapAnnotation = LabelAnnotationPrefix + "spark-configmap"
	// HadoopConfigMapAnnotation is the name of the annotation added to the driver and executor Pods
	// that indicates the presence of a Hadoop ConfigMap that should be mounted to the driver and
	// executor Pods with the environment variable HADOOP_CONF_DIR set to point to the mount path.
	HadoopConfigMapAnnotation = LabelAnnotationPrefix + "hadoop-configmap"
	// GeneralConfigMapsAnnotationPrefix is the prefix of general annotations that specifies the name
	// and mount paths of additional ConfigMaps to be mounted.
	GeneralConfigMapsAnnotationPrefix = LabelAnnotationPrefix + "configmap."
	// VolumesAnnotationPrefix is the prefix of annotations that specify a Volume.
	VolumesAnnotationPrefix = LabelAnnotationPrefix + "volumes."
	// VolumeMountsAnnotationPrefix is the prefix of annotations that specify a VolumeMount.
	VolumeMountsAnnotationPrefix = LabelAnnotationPrefix + "volumemounts."
	// OwnerReferenceAnnotation is the name of the annotation added to the driver and executor Pods
	// that specifies the OwnerReference of the owning SparkApplication.
	OwnerReferenceAnnotation = LabelAnnotationPrefix + "ownerreference"
	// AffinityAnnotation is the name of the annotation added to the driver and executor Pods that
	// specifies the value of the Pod Affinity.
	AffinityAnnotation = LabelAnnotationPrefix + "affinity"
	// SparkAppIDLabel is the name of the label used to group API objects, e.g., Spark UI service, Pods,
	// ConfigMaps, etc., belonging to the same Spark application.
	SparkAppIDLabel = LabelAnnotationPrefix + "app-id"
	// SparkAppNameLabel is the name of the label for the SparkApplication object name.
	SparkAppNameLabel = LabelAnnotationPrefix + "app-name"
	// LaunchedBySparkOperatorLabel is a label on Spark pods launched through the Spark Operator.
	LaunchedBySparkOperatorLabel = LabelAnnotationPrefix + "launched-by-spark-operator"
)

const (
	// SparkContainerImageKey is the configuration property for specifying the unified container image.
	SparkContainerImageKey = "spark.kubernetes.container.image"
	// SparkImagePullSecretKey is the configuration property for specifying the comma-separated list of image-pull
	// secrets.
	SparkImagePullSecretKey = "spark.kubernetes.container.image.pullSecrets"
	// SparkContainerImageKey is the configuration property for specifying the container image pull policy.
	SparkContainerImagePullPolicyKey = "spark.kubernetes.container.image.pullPolicy"
	// SparkNodeSelectorKeyPrefix is the configuration property prefix for specifying node selector for the pods.
	SparkNodeSelectorKeyPrefix = "spark.kubernetes.node.selector."
	// SparkDriverContainerImageKey is the configuration property for specifying a custom driver container image.
	SparkDriverContainerImageKey = "spark.kubernetes.driver.container.image"
	// SparkExecutorContainerImageKey is the configuration property for specifying a custom executor container image.
	SparkExecutorContainerImageKey = "spark.kubernetes.executor.container.image"
	// SparkDriverCoreLimitKey is the configuration property for specifying the hard CPU limit for the driver pod.
	SparkDriverCoreLimitKey = "spark.kubernetes.driver.limit.cores"
	// SparkDriverCoreLimitKey is the configuration property for specifying the hard CPU limit for the executor pods.
	SparkExecutorCoreLimitKey = "spark.kubernetes.executor.limit.cores"
	// SparkExecutorCoreRequestKey is the configuration property for specifying the physical CPU request for executors.
	SparkExecutorCoreRequestKey = "spark.kubernetes.executor.request.cores"
	// SparkDriverSecretKeyPrefix is the configuration property prefix for specifying secrets to be mounted into the
	// driver.
	SparkDriverSecretKeyPrefix = "spark.kubernetes.driver.secrets."
	// SparkDriverSecretKeyPrefix is the configuration property prefix for specifying secrets to be mounted into the
	// executors.
	SparkExecutorSecretKeyPrefix = "spark.kubernetes.executor.secrets."
	// SparkDriverSecretKeyRefKeyPrefix is the configuration property prefix for specifying environment variables
	// from SecretKeyRefs for the driver.
	SparkDriverSecretKeyRefKeyPrefix = "spark.kubernetes.driver.secretKeyRef."
	// SparkDriverSecretKeyRefKeyPrefix is the configuration property prefix for specifying environment variables
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
	// SparkDriverPodNameKey is the Spark configuration key for driver pod name.
	SparkDriverPodNameKey = "spark.kubernetes.driver.pod.name"
	// SparkDriverServiceAccountName is the Spark configuration key for specifying name of the Kubernetes service
	// account used by the driver pod.
	SparkDriverServiceAccountName = "spark.kubernetes.authenticate.driver.serviceAccountName"
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
