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
	// SparkConfigMapNamePrefix is the name prefix of the Spark ConfigMap created from the directory
	// in the submission client container specified by SparkApplicationSpec.SparkConfDir.
	SparkConfigMapNamePrefix = "spark-config-map"
	// SparkConfigMapVolumeName is the name of the ConfigMap volume of Spark configuration files.
	SparkConfigMapVolumeName = "spark-config-map-volume"
	// DefaultHadoopConfDir is the default directory for Spark configuration files if not specified.
	// This directory is where the Hadoop ConfigMap is mounted in the driver and executor containers.
	DefaultHadoopConfDir = "/etc/hadoop/conf"
	// HadoopConfigMapNamePrefix is the name prefix of the Hadoop ConfigMap created from the directory
	// in the submission client container specified by.
	HadoopConfigMapNamePrefix = "hadoop-config-map"
	// HadoopConfigMapVolumeName is the name of the ConfigMap volume of Hadoop configuration files.
	HadoopConfigMapVolumeName = "hadoop-config-map-volume"
	// SparkConfDirEnvVar is the environment variable to add to the driver and executor Pods that point
	// to the directory where the Spark ConfigMap is mounted.
	SparkConfDirEnvVar = "SPARK_CONF_DIR"
	// HadoopConfDirEnvVar is the environment variable to add to the driver and executor Pods that point
	// to the directory where the Hadoop ConfigMap is mounted.
	HadoopConfDirEnvVar = "HADOOP_CONF_DIR"
	// SparkClasspathEnvVar is the environment variable in the driver and executor containers for
	// specifying the classpath.
	SparkClasspathEnvVar = "SPARK_CLASSPATH"
)

const (
	// LabelAnnotationPrefix is the prefix of every labels and annotations added by the controller.
	LabelAnnotationPrefix = "sparkoperator.k8s.io/"
	// SparkConfigMapAnnotation is the name of the annotation added to the driver and executor Pods
	// that indicates the presence of a Spark ConfigMap that should be mounted to the driver and
	// executor Pods with the environment variable SPARK_CONF_DIR set to point to the mount path.
	SparkConfigMapAnnotation = LabelAnnotationPrefix + "sparkConfigMap"
	// HadoopConfigMapAnnotation is the name of the annotation added to the driver and executor Pods
	// that indicates the presence of a Hadoop ConfigMap that should be mounted to the driver and
	// executor Pods with the environment variable HADOOP_CONF_DIR set to point to the mount path.
	HadoopConfigMapAnnotation = LabelAnnotationPrefix + "hadoopConfigMap"
	// GeneralConfigMapsAnnotationPrefix is the name of the general annotation that specifies the name
	// and mount paths of additional ConfigMaps to be mounted.
	GeneralConfigMapsAnnotationPrefix = LabelAnnotationPrefix + "configMap."
	// GeneralSecretsAnnotationPrefix is the name of the general annotation that specifies the name,
	// mount path, and type of secrets to be mounted.
	GeneralSecretsAnnotationPrefix = LabelAnnotationPrefix + "secret."
	// GCPServiceAccountSecretAnnotationPrefix is the prefix of annotation that specifies a GCP service
	// account secret to be mounted. GCP service account secret needs the special handling of also
	// setting the environment variable GOOGLE_APPLICATION_CREDENTIALS.
	GCPServiceAccountSecretAnnotationPrefix = LabelAnnotationPrefix + "GCPServiceAccount."
	// OwnerReferenceAnnotation is the name of the annotation added to the driver and executor Pods
	// that specifies the OwnerReference of the owning SparkApplication.
	OwnerReferenceAnnotation = LabelAnnotationPrefix + "ownerReference"
	// SparkAppIDLabel is the name of the label used to group API objects, e.g., Spark UI service, Pods,
	// ConfigMaps, etc., belonging to the same Spark application.
	SparkAppIDLabel = LabelAnnotationPrefix + "appId"
)

const (
	// SparkContainerImageKey is the configuration property for specifying the unified container image.
	SparkContainerImageKey = "spark.kubernetes.container.image"
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
	// DriverEnvVarConfigKeyPrefix is the Spark configuration prefix for setting environment variables
	// into the driver.
	DriverEnvVarConfigKeyPrefix = "spark.kubernetes.driverEnv."
	// ExecutorEnvVarConfigKeyPrefix is the Spark configuration prefix for setting environment variables
	// into the executor.
	ExecutorEnvVarConfigKeyPrefix = "spark.executorEnv."
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
