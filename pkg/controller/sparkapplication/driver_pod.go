package sparkapplication

import (
	"fmt"
	"strings"

	"github.com/golang/glog"
	"github.com/google/uuid"

	"k8s.io/apimachinery/pkg/api/resource"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	v1 "k8s.io/client-go/listers/core/v1"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
)

const (
	driverPodMemoryRequest = "896Mi"
	driverPodCpuRequest    = "100m"
	driverPodMemoryLimit   = "900Mi"
	driverPodCpuLimit      = "200m"
)

type clientSubmissionPodManager interface {
	createClientDriverPod(app *v1beta2.SparkApplication) (string, string, error)
	//deleteClientDriverPod(app *v1beta2.SparkApplication) error
	//getClientDriverPod(app *v1beta2.SparkApplication) (*corev1.Pod, error)
	//isClientDriverPodCreated(app *v1beta2.SparkApplication) (*bool, *metav1.Time, error)
}

type realClientSubmissionPodManager struct {
	kubeClient kubernetes.Interface
	podLister  v1.PodLister
}

func (spm *realClientSubmissionPodManager) createClientDriverPod(app *v1beta2.SparkApplication) (string, string, error) {
	var image string
	if app.Spec.Image != nil {
		image = *app.Spec.Image
	} else if app.Spec.Driver.Image != nil {
		image = *app.Spec.Driver.Image
	}
	if image == "" {
		return "", "", fmt.Errorf("no image specified in .spec.image or .spec.driver.image in SparkApplication %s/%s",
			app.Namespace, app.Name)
	}

	driverPodName := getDriverPodName(app)
	submissionID := uuid.New().String()
	submissionCmdArgs, err := buildSubmissionCommandArgs(app, driverPodName, submissionID)
	if err != nil {
		return "", "", err
	}

	command := []string{"sh", "-c", fmt.Sprintf("$SPARK_HOME/bin/spark-submit %s", strings.Join(submissionCmdArgs, " "))}

	labels := map[string]string{
		config.SparkAppNameLabel:            app.Name,
		config.LaunchedBySparkOperatorLabel: "true",
	}

	imagePullSecrets := make([]corev1.LocalObjectReference, len(app.Spec.ImagePullSecrets))
	for i, secret := range app.Spec.ImagePullSecrets {
		imagePullSecrets[i] = corev1.LocalObjectReference{Name: secret}
	}
	imagePullPolicy := corev1.PullIfNotPresent
	if app.Spec.ImagePullPolicy != nil {
		imagePullPolicy = corev1.PullPolicy(*app.Spec.ImagePullPolicy)
	}

	//nodename is questionable
	//NodeName: app.Status.OwnerId,
	clientDriver := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            driverPodName,
			Namespace:       app.Namespace,
			Labels:          labels,
			Annotations:     app.Annotations,
			OwnerReferences: []metav1.OwnerReference{*getOwnerReference(app)},
		},
		Spec: corev1.PodSpec{
			ImagePullSecrets: imagePullSecrets,
			Containers: []corev1.Container{
				{
					Name:            "spark-kubernetes-driver",
					Image:           image,
					Command:         command,
					ImagePullPolicy: imagePullPolicy,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(driverPodCpuRequest),
							corev1.ResourceMemory: resource.MustParse(driverPodMemoryRequest),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(driverPodCpuLimit),
							corev1.ResourceMemory: resource.MustParse(driverPodMemoryLimit),
						},
					},
				},
			},
		},
	}

	if app.Spec.ServiceAccount != nil {
		clientDriver.Spec.ServiceAccountName = *app.Spec.ServiceAccount
	} else if app.Spec.Driver.ServiceAccount != nil {
		clientDriver.Spec.ServiceAccountName = *app.Spec.Driver.ServiceAccount
	}
	//.spec.template.spec.containers

	// Copy the labels on the SparkApplication to the Job.
	for key, val := range app.Labels {
		clientDriver.Labels[key] = val
	}

	glog.Infof("Creating %s for running spark in client mode", clientDriver.Name)
	_, err = spm.kubeClient.CoreV1().Pods(app.Namespace).Create(clientDriver)
	if err != nil {
		return "", "", err
	}

	return submissionID, driverPodName, nil
}

//func (spm *realClientSubmissionPodManager) getClientDriverPod(app *v1beta2.SparkApplication) (*batchv1.Job, error) {
//	//theres a getDriverpod in controller -- how to use this here?
//	return spm.podLister.Pods(app.Namespace).Get(getDriverPodName(app))
//
//}

//delete method in the controller
//func (spm *realSubmissionPodManager) deleteDriverPod(app *v1beta2.SparkApplication) error {
//
//}
