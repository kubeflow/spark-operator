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
)

const (
	// LabelAnnotationPrefix is the prefix of every labels and annotations added by the controller.
	LabelAnnotationPrefix = "spark.apache.k8s.io/"
	// SparkConfigMapAnnotation is the name of the annotation added to the driver and executor Pods
	// that indicates the presence of a Spark ConfigMap that should be mounted to the driver and
	// executor Pods with the environment variable SPARK_CONF_DIR set to point to the mount path.
	SparkConfigMapAnnotation = LabelAnnotationPrefix + "spark-config-map"
	// HadoopConfigMapAnnotation is the name of the annotation added to the driver and executor Pods
	// that indicates the presence of a Hadoop ConfigMap that should be mounted to the driver and
	// executor Pods with the environment variable HADOOP_CONF_DIR set to point to the mount path.
	HadoopConfigMapAnnotation = LabelAnnotationPrefix + "hadoop-config-map"
	// GeneralConfigMapsAnnotation is the name of the general annotation that specifies the name
	// and mount paths of additional ConfigMaps to be mounted.
	GeneralConfigMapsAnnotation = LabelAnnotationPrefix + "config-maps"
	// GeneralSecretsAnnotation is the name of the general annotation that specifies the name,
	// mount path, and type of secrets to be mounted.
	GeneralSecretsAnnotation = LabelAnnotationPrefix + "secrets"
	// GCPServiceAccountSecretAnnotation is the name of the annotation that specifies a GCP service
	// account secret to be mounted. GCP service account secret needs the special handling of also
	// setting the environment variable GOOGLE_APPLICATION_CREDENTIALS.
	GCPServiceAccountSecretAnnotation = LabelAnnotationPrefix + "gcp-service-account"
	// OwnerReferenceAnnotation is the name of the annotation added to the driver and executor Pods
	// that specifies the OwnerReference of the owning SparkApplication.
	OwnerReferenceAnnotation = LabelAnnotationPrefix + "owner-reference"
	// SparkAppIDLabel is the name of the label used to group API objects, e.g., Spark UI service, Pods,
	// ConfigMaps, etc., belonging to the same Spark application.
	SparkAppIDLabel = LabelAnnotationPrefix + "app-id"
)

const (
	// DriverEnvVarConfigKeyPrefix is the Spark configruation prefix for setting environment variables
	// into the driver.
	DriverEnvVarConfigKeyPrefix = "spark.kubernetes.driverEnv."
	// ExecutorEnvVarConfigKeyPrefix is the Spark configruation prefix for setting environment variables
	// into the executor.
	ExecutorEnvVarConfigKeyPrefix = "spark.executorEnv."
	// SparkDriverAnnotationKeyPrefix is the Spark configuation key prefix for annotations on the driver Pod.
	SparkDriverAnnotationKeyPrefix = "spark.kubernetes.driver.annotations."
	// SparkExecutorAnnotationKeyPrefix is the Spark configuation key prefix for annotations on the executor Pod.
	SparkExecutorAnnotationKeyPrefix = "spark.kubernetes.executor.annotations."
	// SparkConfDirEnvVar is the environment variable to add to the driver and executor Pods that point
	// to the directory where the Spark ConfigMap is mounted.
	SparkConfDirEnvVar = "SPARK_CONF_DIR"
	// HadoopConfDirEnvVar is the environment variable to add to the driver and executor Pods that point
	// to the directory where the Hadoop ConfigMap is mounted.
	HadoopConfDirEnvVar = "HADOOP_CONF_DIR"
)
