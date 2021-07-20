package sparkapplication

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/api/resource"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	v1 "k8s.io/client-go/listers/core/v1"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/webhook/resourceusage"
)

type clientModeSubmissionPodManager interface {
	createClientDriverPod(app *v1beta2.SparkApplication) (string, string, error)
	getClientDriverPod(app *v1beta2.SparkApplication) (*corev1.Pod, error)
}

type realClientModeSubmissionPodManager struct {
	kubeClient kubernetes.Interface
	podLister  v1.PodLister
}

func (spm *realClientModeSubmissionPodManager) createClientDriverPod(app *v1beta2.SparkApplication) (string, string, error) {
	var image string
	if app.Spec.Image != nil {
		image = *app.Spec.Image
	} else if app.Spec.Driver.Image != nil {
		image = *app.Spec.Driver.Image
	}

	if image == "" {
		return "", "", fmt.Errorf("no image specified in .spec.image or .spec.driver.image in SparkApplication %s,%s",
			app.Namespace, app.Name)
	}

	driverPodName := getDriverPodName(app)
	submissionID := uuid.New().String()
	submissionCmdArgs, err := buildSubmissionCommandArgs(app, driverPodName, submissionID)

	if err != nil {
		return "", "", err
	}

	command := []string{"sh", "-c", fmt.Sprintf("$SPARK_HOME/bin/spark-submit %s", strings.Join(submissionCmdArgs, " "))}

	labels := make(map[string]string)
	labels[config.SparkRoleLabel] = "client-driver"
	labels[config.SparkAppNameLabel] = app.Name
	labels[config.SubmissionIDLabel] = submissionID

	for key, val := range app.Labels {
		labels[key] = val
	}
	for key, val := range app.Spec.Driver.Labels {
		labels[key] = val
	}

	for key, value := range app.Spec.SparkConf {
		if strings.HasPrefix(key, "spark.kubernetes.driver.label.") {
			label := strings.ReplaceAll(key, "spark.kubernetes.driver.label.", "")
			labels[label] = value
		}
	}

	imagePullSecrets := make([]corev1.LocalObjectReference, len(app.Spec.ImagePullSecrets))
	for i, secret := range app.Spec.ImagePullSecrets {
		imagePullSecrets[i] = corev1.LocalObjectReference{Name: secret}
	}
	imagePullPolicy := corev1.PullIfNotPresent
	if app.Spec.ImagePullPolicy != nil {
		imagePullPolicy = corev1.PullPolicy(*app.Spec.ImagePullPolicy)
	}

	var driverCpuQuantity string
	if app.Spec.Driver.CoreRequest != nil {
		driverCpuQuantity = *app.Spec.Driver.CoreRequest
	} else {
		driverCpuQuantity = fmt.Sprintf("%d", *app.Spec.Driver.Cores)
	}

	var driverCpuQuantityLimit string
	if app.Spec.Driver.CoreLimit != nil {
		driverCpuQuantityLimit = *app.Spec.Driver.CoreLimit
	} else {
		driverCpuQuantityLimit = driverCpuQuantity
	}

	driverOverheadMemory, err :=
		resourceusage.MemoryRequiredForSparkPod(app.Spec.Driver.SparkPodSpec, app.Spec.MemoryOverheadFactor, app.Spec.Type, 1)
	if err != nil {
		return "", "", err
	}

	var args []string
	for _, argument := range app.Spec.Arguments {
		args = append(args, argument)
	}

	//append all env variables
	var envVars []corev1.EnvVar
	for key, value := range app.Spec.Driver.EnvVars {
		envVars = append(envVars, corev1.EnvVar{Name: key, Value: value})
	}

	for key, value := range app.Spec.SparkConf {
		if strings.HasPrefix(key, "spark.kubernetes.driverEnv.") {
			env := strings.ReplaceAll(key, "spark.kubernetes.driverEnv.", "")
			envVars = append(envVars, corev1.EnvVar{Name: env, Value: value})
		}
	}

	envVars = append(envVars,
		corev1.EnvVar{Name: "SPARK_K8S_DRIVER_POD_IP",
			ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}})

	envVars = append(envVars,
		corev1.EnvVar{Name: "SPARK_DRIVER_BIND_ADDRESS",
			ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}})

	//append all annotations
	annotations := make(map[string]string)
	for key, value := range app.Spec.Driver.Annotations {
		annotations[key] = value
	}
	for key, value := range app.Annotations {
		annotations[key] = value
	}

	for key, value := range app.Spec.SparkConf {
		if strings.HasPrefix(key, "spark.kubernetes.driver.annotation.") {
			annotation := strings.ReplaceAll(key, "spark.kubernetes.driver.annotation.", "")
			annotations[annotation] = value
		}
	}
	nodeSelectors := make(map[string]string)
	for key, value := range app.Spec.SparkConf {
		if strings.HasPrefix(key, "spark.kubernetes.node.selector.") {
			nodeSelector := strings.ReplaceAll(key, "spark.kubernetes.node.selector.", "")
			nodeSelectors[nodeSelector] = value
		}
	}

	//append all volumes
	var volumes []corev1.Volume
	var volumeMounts []corev1.VolumeMount

	for _, value := range app.Spec.Volumes {
		volumes = append(volumes, corev1.Volume{Name: value.Name,
			VolumeSource: value.VolumeSource})
	}
	volumes = append(volumes, corev1.Volume{Name: "spark-local-dir-1",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: "Memory"}}})
	volumes = append(volumes, corev1.Volume{Name: "hadoop-properties",
		VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: fmt.Sprintf("%s-%s-hadoop-config", app.Name, uuid.New().String())},
		}}})
	volumes = append(volumes, corev1.Volume{Name: "spark-conf-volume",
		VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: fmt.Sprintf("%s-%s-driver-conf-map", app.Name, uuid.New().String())},
		}}})

	for _, value := range app.Spec.Driver.VolumeMounts {
		volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: value.Name,
			MountPath: value.MountPath})
	}
	volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: "spark-local-dir-1",
		MountPath: fmt.Sprintf("/var/data/spark-%s", uuid.New().String())})
	clientDriver := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            driverPodName,
			Namespace:       app.Namespace,
			Labels:          labels,
			Annotations:     annotations,
			OwnerReferences: []metav1.OwnerReference{*getOwnerReference(app)},
		},
		Spec: corev1.PodSpec{
			ImagePullSecrets: imagePullSecrets,
			Volumes:          volumes,
			NodeSelector:     nodeSelectors,
			Containers: []corev1.Container{
				{
					Name:            "spark-kubernetes-driver",
					Image:           image,
					Command:         command,
					Args:            args,
					ImagePullPolicy: imagePullPolicy,
					Env:             envVars,
					VolumeMounts:    volumeMounts,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(driverCpuQuantity),
							corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%d", driverOverheadMemory)),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(driverCpuQuantityLimit),
							corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%d", driverOverheadMemory)),
						},
					},
				},
			},
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}

	if app.Spec.ServiceAccount != nil {
		clientDriver.Spec.ServiceAccountName = *app.Spec.ServiceAccount
	} else if app.Spec.Driver.ServiceAccount != nil {
		clientDriver.Spec.ServiceAccountName = *app.Spec.Driver.ServiceAccount
	}

	glog.Infof("Creating the %s for running spark in client mode", clientDriver.Name)
	_, err = spm.kubeClient.CoreV1().Pods(app.Namespace).Create(clientDriver)
	if err != nil {
		return "", "", err
	}

	return submissionID, driverPodName, nil
}

func (spm *realClientModeSubmissionPodManager) getClientDriverPod(app *v1beta2.SparkApplication) (*corev1.Pod, error) {
	return spm.podLister.Pods(app.Namespace).Get(getDriverPodName(app))
}
