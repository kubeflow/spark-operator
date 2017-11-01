package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SparkApplicationType describes the type of a Spark application.
type SparkApplicationType string

// Different types of Spark applications.
const (
	JavaApplicationType   SparkApplicationType = "Java"
	ScalaApplicationType  SparkApplicationType = "Scala"
	PythonApplicationType SparkApplicationType = "Python"
	RApplocationType      SparkApplicationType = "R"
)

// DeployMode describes the type of deployment of a Spark application.
type DeployMode string

// Different types of deployments.
const (
	ClusterMode         DeployMode = "cluster"
	ClientMode          DeployMode = "client"
	InClusterClientMode DeployMode = "in-cluster-client"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SparkApplication represents a Spark application running on and using Kubernetes as a cluster manager.
type SparkApplication struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`
	Spec              SparkApplicationSpec   `json:"spec"`
	Status            SparkApplicationStatus `json:"status,omitempty"`
}

// SparkApplicationSpec describes the specification of a Spark application using Kubernetes as a cluster manager.
// It carries every pieces of information a spark-submit command takes and recognizes.
type SparkApplicationSpec struct {
	// Type tells the type of the Spark application.
	Type SparkApplicationType `jason:"type"`
	// Mode is the deployment mode of the Spark application.
	Mode DeployMode `jason:"mode"`
	// MainClass is the fully-qualified main class of the Spark application.
	// This only applies to Java/Scala Spark applications.
	MainClass *string `jason:"mainClass,omitempty"`
	// MainFile is the path to a bundled JAR or Python file including the Spark application and its dependencies.
	MainApplicationFile string `jason:"mainApplicationFile"`
	// Arguments is a list of arguments to be passed to the application.
	Arguments []string `jason:"arguments,omitempty"`
	// SparkConf carries the user-specified Spark configuration properties as they would use the "--conf" option in spark-submit.
	SparkConf map[string]string `jason:"sparkConf,omitempty"`
	// SparkConfigMap carries the name of the ConfigMap containing Spark configuration files such as log4j.properties.
	// The controller will add environment variable SPARK_CONF_DIR to the path where the ConfigMap is mounted to.
	SparkConfigMap *string `jason:"sparkConigMap,omitempty"`
	// HadoopConfigMap carries the name of the ConfigMap containing Hadoop configuration files such as core-site.xml.
	// The controller will add environment variable HADOOP_CONF_DIR to the path where the ConfigMap is mounted to.
	HadoopConfigMap *string `jason:"hadoopConigMap,omitempty"`
	// Driver is the driver specification.
	Driver DriverSpec `jason:"driver"`
	// Executor is the executor specification.
	Executor ExecutorSpec `jason:"executor"`
	// Deps captures all possible types of dependencies of a Spark application.
	Deps Dependencies `jason:"deps"`
	// LogsLocation is the location where application logs get written to on local file system.
	// This is only applicable if application logs are not written to stdout/stderr.
	LogsLocation *string `jason:"logsLocation,omitempty"`
	// SubmissionByUser indicates if the application is to be submitted by the user.
	// The custom controller should not submit the application on behalf of the user if this is true.
	// It defaults to false.
	SubmissionByUser bool `jason:"submissionByUser"`
}

// ApplicationStateType represents the type of the current state of an application.
type ApplicationStateType string

// Different states an application may have.
const (
	NewState       ApplicationStateType = "NEW"
	SubmittedState ApplicationStateType = "SUBMITTED"
	RunningState   ApplicationStateType = "RUNNING"
	CompletedState ApplicationStateType = "COMPLETED"
	FailedState    ApplicationStateType = "FAILED"
)

type applicationState struct {
	State        ApplicationStateType
	ErrorMessage string
}

// ExecutorState tells the current state of an executor.
type ExecutorState string

// Different states an executor may have.
const (
	ExecutorRunningState   ExecutorState = "RUNNING"
	ExecutorCompletedState ExecutorState = "COMPLETED"
	ExecutorFailedState    ExecutorState = "FAILED"
)

// SparkApplicationStatus describes the current status of a Spark application.
type SparkApplicationStatus struct {
	// AppId is the application ID that's also added as a label to the SparkApplication object
	// and driver and executor Pods, and is used to group the objects for the same application.
	AppID string `json:"appId"`
	// WebUIServiceName is the name of the service for the Spark web UI running on the driver.
	UIServiceInfo UIServiceInfo `json:"uiServiceInfo"`
	// AppState tells the overall application state.
	AppState applicationState `json:"applicationState"`
	// RequestedExecutors is the number of executors requested.
	// In case dynamic allocation is enabled, this is the number of initial executors requested.
	RequestedExecutors int32 `json:"requestedExecutors"`
	RunningExecutors   int32 `json:"runningExecutors"`
	CompletedExecutors int32 `json:"completedExecutors"`
	FailedExecutors    int32 `json:"failedExecutors"`
	// ExecutorState records the state of executors by executor Pod names.
	ExecutorState map[string]ExecutorState `json:"executorState"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SparkApplicationList carries a list of SparkApplication objects.
type SparkApplicationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SparkApplication `jason:"items,omitempty"`
}

// UIServiceInfo captures information about the Spark UI service.
type UIServiceInfo struct {
	Name string
	Port int32
}

// Dependencies specifies all possible types of dependencies of a Spark application.
type Dependencies struct {
	// JarFiles is a list of JAR files the Spark application depends on.
	JarFiles []string `jason:"jarFiles,omitempty"`
	// Files is a list of files the Spark application depends on.
	Files []string `jason:"files,omitempty"`
	// PyFiles is a list of Python files the Spark application depends on.
	PyFiles []string `jason:"pyFiles,omitempty"`
}

// DriverSpec is specification of the driver.
type DriverSpec struct {
	// Image is the driver Docker image to use.
	Image string `jason:"image"`
	// DriverConfigMaps carries information of other ConfigMaps to add to the driver Pod.
	DriverConfigMaps []NamePath `jason:"driverConigMaps,omitempty"`
	// DriverSecrets carries information of secrets to add to the driver Pod.
	DriverSecrets []SecretInfo `jason:"driverSecrets,omitempty"`
	// DriverEnvVars carries the environment variables to add to the driver Pod.
	DriverEnvVars map[string]string `jason:"driverEnvVars,omitempty"`
}

// ExecutorSpec is specification of the executor.
type ExecutorSpec struct {
	// Image is the executor Docker image to use.
	Image string `jason:"image"`
	// Instances is the number of executor instances.
	Instances int32 `jason:"instances"`
	// ExecutorConfigMaps carries information of other ConfigMaps to add to the executor Pods.
	ExecutorConfigMaps []NamePath `jason:"executorConigMaps,omitempty"`
	// ExecutorSecrets carries information of secrets to add to the executor Pods.
	ExecutorSecrets []SecretInfo `jason:"executorSecrets,omitempty"`
	// ExecutorEnvVars carries the environment variables to add to the executor Pods.
	ExecutorEnvVars map[string]string `jason:"executorEnvVars,omitempty"`
}

// NamePath is a pair of a name and a path to which the named objects should be mounted to.
type NamePath struct {
	Name string
	Path string
}

// SecretType tells the type of a secret.
type SecretType string

// An enumeration of secret types supported.
const (
	// GCPServiceAccountSecret is for secrets from a GCP service account Json key file that needs
	// the environment variable GOOGLE_APPLICATION_CREDENTIALS.
	GCPServiceAccountSecret SecretType = "GCPServiceAccount"
	// HDFSDelegationTokenSecret is for secrets from an HDFS delegation token that needs the
	// environment variable HADOOP_TOKEN_FILE_LOCATION.
	HDFSDelegationTokenSecret SecretType = "HDFSDelegationToken"
	// GenericType is for secrets that needs no special handling.
	GenericType SecretType = "Generic"
)

// SecretInfo captures information of a secret.
type SecretInfo struct {
	Name string
	Path string
	Type SecretType
}
