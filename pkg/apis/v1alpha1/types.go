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
	Type SparkApplicationType `json:"type"`
	// Mode is the deployment mode of the Spark application.
	Mode DeployMode `json:"mode"`
	// MainClass is the fully-qualified main class of the Spark application.
	// This only applies to Java/Scala Spark applications.
	MainClass *string `json:"mainClass,omitempty"`
	// MainFile is the path to a bundled JAR or Python file including the Spark application and its dependencies.
	MainApplicationFile string `json:"mainApplicationFile"`
	// Arguments is a list of arguments to be passed to the application.
	Arguments []string `json:"arguments,omitempty"`
	// SparkConf carries the user-specified Spark configuration properties as they would use the "--conf" option in spark-submit.
	SparkConf map[string]string `json:"sparkConf,omitempty"`
	// SparkConfigMap carries the name of the ConfigMap containing Spark configuration files such as log4j.properties.
	// The controller will add environment variable SPARK_CONF_DIR to the path where the ConfigMap is mounted to.
	SparkConfigMap *string `json:"sparkConigMap,omitempty"`
	// HadoopConfigMap carries the name of the ConfigMap containing Hadoop configuration files such as core-site.xml.
	// The controller will add environment variable HADOOP_CONF_DIR to the path where the ConfigMap is mounted to.
	HadoopConfigMap *string `json:"hadoopConigMap,omitempty"`
	// Driver is the driver specification.
	Driver DriverSpec `json:"driver"`
	// Executor is the executor specification.
	Executor ExecutorSpec `json:"executor"`
	// Deps captures all possible types of dependencies of a Spark application.
	Deps Dependencies `json:"deps"`
	// LogsLocation is the location where application logs get written to on local file system.
	// This is only applicable if application logs are not written to stdout/stderr.
	LogsLocation *string `json:"logsLocation,omitempty"`
	// SubmissionByUser indicates if the application is to be submitted by the user.
	// The custom controller should not submit the application on behalf of the user if this is true.
	// It defaults to false.
	SubmissionByUser bool `json:"submissionByUser"`
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
	State        ApplicationStateType `json:"state"`
	ErrorMessage string               `json:"errorMessage"`
}

// ExecutorState tells the current state of an executor.
type ExecutorState string

// Different states an executor may have.
const (
	ExecutorPendingState   ExecutorState = "PENDING"
	ExecutorRunningState   ExecutorState = "RUNNING"
	ExecutorCompletedState ExecutorState = "COMPLETED"
	ExecutorFailedState    ExecutorState = "FAILED"
)

// SparkApplicationStatus describes the current status of a Spark application.
type SparkApplicationStatus struct {
	// AppId is the application ID that's also added as a label to the SparkApplication object
	// and driver and executor Pods, and is used to group the objects for the same application.
	AppID string `json:"appId"`
	// DriverInfo has information about the driver.
	DriverInfo DriverInfo `json:"driverInfo"`
	// AppState tells the overall application state.
	AppState applicationState `json:"applicationState"`
	// ExecutorState records the state of executors by executor Pod names.
	ExecutorState map[string]ExecutorState `json:"executorState"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SparkApplicationList carries a list of SparkApplication objects.
type SparkApplicationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SparkApplication `json:"items,omitempty"`
}

// DriverInfo captures information about the driver.
type DriverInfo struct {
	WebUIServiceName string `json:"webUIServiceName"`
	WebUIPort        int32  `json:"webUIPort"`
	PodName          string `json:"podName"`
}

// Dependencies specifies all possible types of dependencies of a Spark application.
type Dependencies struct {
	// JarFiles is a list of JAR files the Spark application depends on.
	JarFiles []string `json:"jarFiles,omitempty"`
	// Files is a list of files the Spark application depends on.
	Files []string `json:"files,omitempty"`
	// PyFiles is a list of Python files the Spark application depends on.
	PyFiles []string `json:"pyFiles,omitempty"`
}

// DriverSpec is specification of the driver.
type DriverSpec struct {
	// Image is the driver Docker image to use.
	Image string `json:"image"`
	// DriverConfigMaps carries information of other ConfigMaps to add to the driver Pod.
	DriverConfigMaps []NamePath `json:"driverConigMaps,omitempty"`
	// DriverSecrets carries information of secrets to add to the driver Pod.
	DriverSecrets []SecretInfo `json:"driverSecrets,omitempty"`
	// DriverEnvVars carries the environment variables to add to the driver Pod.
	DriverEnvVars map[string]string `json:"driverEnvVars,omitempty"`
}

// ExecutorSpec is specification of the executor.
type ExecutorSpec struct {
	// Image is the executor Docker image to use.
	Image string `json:"image"`
	// Instances is the number of executor instances.
	Instances int32 `json:"instances"`
	// ExecutorConfigMaps carries information of other ConfigMaps to add to the executor Pods.
	ExecutorConfigMaps []NamePath `json:"executorConigMaps,omitempty"`
	// ExecutorSecrets carries information of secrets to add to the executor Pods.
	ExecutorSecrets []SecretInfo `json:"executorSecrets,omitempty"`
	// ExecutorEnvVars carries the environment variables to add to the executor Pods.
	ExecutorEnvVars map[string]string `json:"executorEnvVars,omitempty"`
}

// NamePath is a pair of a name and a path to which the named objects should be mounted to.
type NamePath struct {
	Name string `json:"name"`
	Path string `json:"path"`
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
	Name string     `json:"name"`
	Path string     `json:"path"`
	Type SecretType `json:"secretType"`
}
