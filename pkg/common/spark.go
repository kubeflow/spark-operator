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
)

const (
	// LabelAnnotationPrefix is the prefix of every labels and annotations added by the controller.
	LabelAnnotationPrefix = "sparkoperator.k8s.io/"

	// LabelSparkAppName is the name of the label for the SparkApplication object name.
	LabelSparkAppName = LabelAnnotationPrefix + "app-name"

	// LabelScheduledSparkAppName is the name of the label for the ScheduledSparkApplication object name.
	LabelScheduledSparkAppName = LabelAnnotationPrefix + "scheduled-app-name"

	// LabelLaunchedBySparkOperator is a label on Spark pods launched through the Spark Operator.
	LabelLaunchedBySparkOperator = LabelAnnotationPrefix + "launched-by-spark-operator"

	// LabelSubmissionID is the label that records the submission ID of the current run of an application.
	LabelSubmissionID = LabelAnnotationPrefix + "submission-id"
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
	SparkUIPortKey                  = "spark.ui.port"
	DefaultSparkWebUIPort     int32 = 4040
	DefaultSparkWebUIPortName       = "spark-driver-ui-port"
)
