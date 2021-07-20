package sparkapplication

import (
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	kubeclientfake "k8s.io/client-go/kubernetes/fake"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/stretchr/testify/assert"
)

func newFakePodManager(pods ...*corev1.Pod) clientModeSubmissionPodManager {
	kubeClient := kubeclientfake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0*time.Second)
	podInformer := informerFactory.Core().V1().Pods()
	lister := podInformer.Lister()
	for _, pod := range pods {
		if pod != nil {
			podInformer.Informer().GetIndexer().Add(pod)
			kubeClient.CoreV1().Pods(pod.GetNamespace()).Create(pod)
		}
	}
	return &realClientModeSubmissionPodManager{
		podLister:  lister,
		kubeClient: kubeClient,
	}
}

func TestCreateDriverPod(t *testing.T) {
	var core int32 = 1
	memory := "512m"

	os.Setenv(kubernetesServiceHostEnvVar, "localhost")
	os.Setenv(kubernetesServicePortEnvVar, "443")
	// Case 1: Image doesn't exist.
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Status: v1beta2.SparkApplicationStatus{},
	}

	podManager := newFakePodManager(nil)
	submissionID, driverPodName, err := podManager.createClientDriverPod(app)
	assert.NotNil(t, err)
	assert.Empty(t, submissionID)
	assert.Empty(t, driverPodName)

	// Case 2:  Driver Pod created successfully.
	app = &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Image: stringptr("spark-base-image"),
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					InitContainers: []corev1.Container{
						{
							Name:  "spark-kubernetes-driver",
							Image: "spark-driver:latest",
						},
					},
					Memory: &memory,
					Cores:  &core,
				},
			},
		},
		Status: v1beta2.SparkApplicationStatus{},
	}
	podManager = newFakePodManager(nil)
	submissionID, driverPodName, err = podManager.createClientDriverPod(app)
	assert.Nil(t, err)
	assert.NotNil(t, submissionID)
	assert.NotNil(t, driverPodName)

}

func TestGetDriverPod(t *testing.T) {
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Status: v1beta2.SparkApplicationStatus{},
	}

	// Case 1: driver pod does not exist
	podManager := newFakePodManager(nil)
	podResult, err := podManager.getClientDriverPod(app)
	assert.NotNil(t, err)
	assert.True(t, errors.IsNotFound(err))
	assert.Nil(t, podResult)

	// Case 2: driver pod created
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo-driver",
			Namespace: "default",
		},
	}
	podManager = newFakePodManager(pod)
	podResult, err = podManager.getClientDriverPod(app)
	assert.Nil(t, err)
	assert.NotNil(t, podResult)
	assert.Equal(t, pod, podResult)

}
