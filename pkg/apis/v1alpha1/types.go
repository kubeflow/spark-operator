package v1alpha1

import (
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SparkApplicationType describes the type of a Spark application.
type SparkApplicationType string

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
	// SparkConf carries the user-specified Spark configuration properties.
	SparkConf map[string]string `jason:"sparkConf,omitempty"`
	// JarFiles is a list of JAR files the Spark application depends on.
	JarFiles []string `jason:"jarFiles,omitempty"`
	// Files is a list of files the Spark application depends on.
	Files []string `jason:"files,omitempty"`
	// MainFile is the path to a bundled JAR or Python file including the Spark application and its dependencies.
	MainApplicationFile string `jason:"mainApplicationFile"`
	// Arguments is a list of arguments to be passed to the application.
	Arguments []string `jason:"arguments,omitempty"`
	// SparkConfDir is the path to the directory where additional Spark configuration files,
	// e.g. log4j.properties, are located in the submission client container.
	// The SparkApplication controller is responsible for creating a ConfigMap for the configuration files and mount
	// them into the driver and executor containers pointed to by the SPARK_CONF_DIR environment variable.
	SparkConfDir *string `jason:"sparkConfDir,omitempty"`
	// SparkConfDir is the path to the directory where additional Hadoop configuration files,
	// e.g. core-site.xml, are located in the submission client container.
	// The SparkApplication controller is responsible for creating a ConfigMap for the configuration files and mount
	// them into the driver and executor containers pointed to by the HADOOP_CONF_DIR environment variable.
	HadoopConfigDir *string `jason:"hadoopConfDir,omitempty"`
	// SubmissionClientTemplate is the Pod template for the submission client Pod.
	// Use a Pod template to allow users to easily customize the submission client Pod.
	SubmissionClientTemplate v1.PodTemplateSpec `json:"submissionClientTemplate,omitempty"`
}

// SparkApplicationSpecStatus describes the current status of a Spark application.
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
