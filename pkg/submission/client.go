package submission

import (
	"fmt"
	"github.com/golang/glog"
	"hash/fnv"

	"github.com/liyinan926/spark-operator/pkg/apis/v1alpha1"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// CreateClientPod creates a Pod for the submission client.
func (ssc *SparkSubmissionClient) CreateClientPod(app *v1alpha1.SparkApplication) error {
	pod := buildClientPod(app)
	app.Status = v1alpha1.SparkApplicationStatus{
		ClientPodName: pod.Name,
	}
	_, err := ssc.KubeClient.CoreV1().Pods(pod.Namespace).Create(pod)
	return err
}

// buildClientPod builds a Pod for the submission client.
func buildClientPod(app *v1alpha1.SparkApplication) *v1.Pod {
	glog.Infof("Creating the submission client Pod for application %s", app.Name)

	template := app.Spec.SubmissionClientTemplate
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        template.Name,
			Namespace:   template.Namespace,
			Labels:      copyLabels(&template),
			Annotations: copyAnnotations(&template),
		},
		Spec: app.Spec.SubmissionClientTemplate.Spec,
	}
	if pod.Name == "" {
		pod.GenerateName = DefaultSubmissionClientPodNamePrefix
	}
	if pod.Namespace == "" {
		pod.Namespace = app.Namespace
	}
	pod.Labels[SparkAppIDLabel] = fmt.Sprintf("spark-app-%d", buildHash(app))
	pod.Annotations[SparkAppNameAnnotation] = app.Name

	return pod
}

func buildClientContainer() *v1.Container {
	container := &v1.Container{}
	return container
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
