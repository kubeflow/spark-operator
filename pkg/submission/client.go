package submission

import (
	"hash/fnv"

	"github.com/liyinan926/spark-operator/pkg/apis/v1alpha1"

	"k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
)

const (
	// DefaultSubmissionClientPodNamePrefix is the default name prefix of
	// the submission client Pod if no name is specified for it.
	DefaultSubmissionClientPodNamePrefix = "spark-submission-client"
	// SparkAppIDLabel is the label that records the Spark application ID that is used to
	// identify the submission client Pod, driver Pod, and executor Pods that all belong
	// to the same Spark application.
	SparkAppIDLabel = "spark-app-id"
	// SparkAppNameAnnotation is the annotation that records the Spark application name.
	// Spark-on-Kubernetes adds this annotation to the driver and executor Pods, so we
	// should also add it to the submission client Pod.
	SparkAppNameAnnotation = "spark-app-name"
)

// SparkSubmissionClient represents a Saprk submission client that submits
// a Spark application to run by configuring and creating the driver Pod.
type SparkSubmissionClient struct {
	KubeClient clientset.Interface
}

func copyLabels(template *v1.PodTemplateSpec) map[string]string {
	labels := make(map[string]string)
	for key, value := range template.Labels {
		labels[key] = value
	}
	return labels
}

func copyAnnotations(template *v1.PodTemplateSpec) map[string]string {
	annotations := make(map[string]string)
	for key, value := range template.Annotations {
		annotations[key] = value
	}
	return annotations
}

func buildHash(app *v1alpha1.SparkApplication) uint32 {
	hasher := fnv.New32()
	return hasher.Sum32()
}
