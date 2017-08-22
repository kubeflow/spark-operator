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
	// The controller will add environment variable HADOOP_CONF_DIR to the path where the ConfigMap is mounted to.
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
}

// SparkApplicationStatus describes the current status of a Spark application.
type SparkApplicationStatus struct {
	WebUIURL string `jason:"webUIURL,omitempty"`
	// ClientPodName is the name of the client Pod.
	ClientPodName string `json:"clientPodName"`
}

// SparkApplicationList carries a list of SparkApplication objects.
type SparkApplicationList struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`
	Item              []SparkApplication `jason:"items,omitempty"`
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
	// DriverConfigMaps carries information of other ConfigMaps to add to the driver Pod.
	DriverConfigMaps []NamePath `jason:"driverConigMaps,omitempty"`
	// DriverSecrets carries information of secrets to add to the driver Pod.
	DriverSecrets []NamePath `jason:"driverSecrets,omitempty"`
	// DriverEnvVars carries the environment variables to add to the driver Pod.
	DriverEnvVars map[string]string `jason:"driverEnvVars,omitempty"`
}

// ExecutorSpec is specification of the executor.
type ExecutorSpec struct {
	// ExecutorConfigMaps carries information of other ConfigMaps to add to the executor Pods.
	ExecutorConfigMaps []NamePath `jason:"executorConigMaps,omitempty"`
	// ExecutorSecrets carries information of secrets to add to the executor Pods.
	ExecutorSecrets []NamePath `jason:"executorSecrets,omitempty"`
	// ExecutorEnvVars carries the environment variables to add to the executor Pods.
	ExecutorEnvVars map[string]string `jason:"executorEnvVars,omitempty"`
}

// NamePath is a pair of a name and a path to which the named objects should be mounted to.
type NamePath struct {
	Name string
	Path string
}
