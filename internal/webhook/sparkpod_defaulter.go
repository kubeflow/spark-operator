/*
Copyright 2024 The Kubeflow authors.

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

package webhook

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
)

const (
	maxNameLength = 63
)

// +kubebuilder:webhook:admissionReviewVersions=v1,failurePolicy=fail,groups="",matchPolicy=Exact,mutating=true,name=mutate-pod.sparkoperator.k8s.io,path=/mutate--v1-pod,reinvocationPolicy=Never,resources=pods,sideEffects=None,verbs=create;update,versions=v1,webhookVersions=v1

// SparkPodDefaulter defaults Spark pods.
type SparkPodDefaulter struct {
	client             client.Client
	sparkJobNamespaces map[string]bool
}

// SparkPodDefaulter implements admission.CustomDefaulter.
var _ admission.CustomDefaulter = &SparkPodDefaulter{}

// NewSparkPodDefaulter creates a new SparkPodDefaulter instance.
func NewSparkPodDefaulter(client client.Client, sparkJobNamespaces []string) *SparkPodDefaulter {
	m := make(map[string]bool)
	for _, ns := range sparkJobNamespaces {
		m[ns] = true
	}

	return &SparkPodDefaulter{
		client:             client,
		sparkJobNamespaces: m,
	}
}

// Default implements admission.CustomDefaulter.
func (d *SparkPodDefaulter) Default(ctx context.Context, obj runtime.Object) error {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}

	namespace := pod.Namespace
	if !d.isSparkJobNamespace(namespace) {
		return nil
	}

	appName := pod.Labels[common.LabelSparkAppName]
	if appName == "" {
		return nil
	}

	app := &v1beta2.SparkApplication{}
	if err := d.client.Get(ctx, types.NamespacedName{Name: appName, Namespace: namespace}, app); err != nil {
		return fmt.Errorf("failed to get SparkApplication %s/%s: %v", namespace, appName, err)
	}

	logger.Info("Mutating Spark pod", "name", pod.Name, "namespace", namespace, "phase", pod.Status.Phase)
	if err := mutateSparkPod(pod, app); err != nil {
		logger.Info("Denying Spark pod", "name", pod.Name, "namespace", namespace, "errorMessage", err.Error())
		return fmt.Errorf("failed to mutate Spark pod: %v", err)
	}

	return nil
}

func (d *SparkPodDefaulter) isSparkJobNamespace(ns string) bool {
	return d.sparkJobNamespaces[ns]
}

type mutateSparkPodOption func(pod *corev1.Pod, app *v1beta2.SparkApplication) error

func mutateSparkPod(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	options := []mutateSparkPodOption{
		addOwnerReference,
		addEnvVars,
		addEnvFrom,
		addHadoopConfigMap,
		addSparkConfigMap,
		addGeneralConfigMaps,
		addVolumes,
		addContainerPorts,
		addHostNetwork,
		addHostAliases,
		addInitContainers,
		addSidecarContainers,
		addDNSConfig,
		addPriorityClassName,
		addSchedulerName,
		addNodeSelectors,
		addAffinity,
		addTolerations,
		addGPU,
		addPrometheusConfig,
		addContainerSecurityContext,
		addPodSecurityContext,
		addTerminationGracePeriodSeconds,
		addPodLifeCycleConfig,
		addShareProcessNamespace,
	}

	for _, option := range options {
		if err := option(pod, app); err != nil {
			return err
		}
	}

	return nil
}

func addOwnerReference(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	if !util.IsDriverPod(pod) {
		return nil
	}
	ownerReference := util.GetOwnerReference(app)
	pod.ObjectMeta.OwnerReferences = append(pod.ObjectMeta.OwnerReferences, ownerReference)
	return nil
}

func addVolumes(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	volumes := app.Spec.Volumes

	volumeMap := make(map[string]corev1.Volume)
	for _, v := range volumes {
		volumeMap[v.Name] = v
	}

	var volumeMounts []corev1.VolumeMount
	if util.IsDriverPod(pod) {
		volumeMounts = app.Spec.Driver.VolumeMounts
	} else if util.IsExecutorPod(pod) {
		volumeMounts = app.Spec.Executor.VolumeMounts
	}

	addedVolumeMap := make(map[string]corev1.Volume)
	for _, m := range volumeMounts {
		// Skip adding localDirVolumes
		if strings.HasPrefix(m.Name, common.SparkLocalDirVolumePrefix) {
			continue
		}

		if v, ok := volumeMap[m.Name]; ok {
			if _, ok := addedVolumeMap[m.Name]; !ok {
				_ = addVolume(pod, v)
				addedVolumeMap[m.Name] = v
			}
			_ = addVolumeMount(pod, m)
		}
	}
	return nil
}

func addVolume(pod *corev1.Pod, volume corev1.Volume) error {
	pod.Spec.Volumes = append(pod.Spec.Volumes, volume)
	return nil
}

func addVolumeMount(pod *corev1.Pod, mount corev1.VolumeMount) error {
	i := findContainer(pod)
	if i < 0 {
		logger.Info("not able to add VolumeMount %s as Spark container was not found in pod %s", mount.Name, pod.Name)
	}

	pod.Spec.Containers[i].VolumeMounts = append(pod.Spec.Containers[i].VolumeMounts, mount)
	return nil
}

func addEnvVars(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	i := findContainer(pod)
	if util.IsDriverPod(pod) {
		if len(app.Spec.Driver.Env) == 0 {
			return nil
		} else if i < 0 {
			return fmt.Errorf("failed to add envs as driver container not found")
		}
		pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, app.Spec.Driver.Env...)
	} else if util.IsExecutorPod(pod) {
		if len(app.Spec.Driver.Env) == 0 {
			return nil
		} else if i < 0 {
			return fmt.Errorf("failed to add envs as executor container not found")
		}
		pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, app.Spec.Executor.Env...)
	}
	return nil
}

func addEnvFrom(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	var envFrom []corev1.EnvFromSource
	if util.IsDriverPod(pod) {
		envFrom = app.Spec.Driver.EnvFrom
	} else if util.IsExecutorPod(pod) {
		envFrom = app.Spec.Executor.EnvFrom
	}

	i := findContainer(pod)
	if i < 0 {
		return fmt.Errorf("not able to add EnvFrom as Spark container was not found in pod")
	}

	pod.Spec.Containers[i].EnvFrom = append(pod.Spec.Containers[i].EnvFrom, envFrom...)
	return nil
}

func addEnvironmentVariable(pod *corev1.Pod, name, value string) error {
	i := findContainer(pod)
	if i < 0 {
		return fmt.Errorf("not able to add environment variable as Spark container was not found")
	}

	pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, corev1.EnvVar{
		Name:  name,
		Value: value,
	})
	return nil
}

func addSparkConfigMap(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	if app.Spec.SparkConfigMap == nil {
		return nil
	}

	if err := addConfigMapVolume(pod, *app.Spec.SparkConfigMap, common.SparkConfigMapVolumeName); err != nil {
		return err
	}

	if err := addConfigMapVolumeMount(pod, common.SparkConfigMapVolumeName, common.DefaultSparkConfDir); err != nil {
		return err
	}

	if err := addEnvironmentVariable(pod, common.EnvSparkConfDir, common.DefaultSparkConfDir); err != nil {
		return err
	}

	return nil
}

func addHadoopConfigMap(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	if app.Spec.HadoopConfigMap == nil {
		return nil
	}

	if err := addConfigMapVolume(pod, *app.Spec.HadoopConfigMap, common.HadoopConfigMapVolumeName); err != nil {
		return err
	}

	if err := addConfigMapVolumeMount(pod, common.HadoopConfigMapVolumeName, common.DefaultHadoopConfDir); err != nil {
		return err
	}

	if err := addEnvironmentVariable(pod, common.EnvHadoopConfDir, common.DefaultHadoopConfDir); err != nil {
		return err
	}

	return nil
}

func addGeneralConfigMaps(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	var configMaps []v1beta2.NamePath
	if util.IsDriverPod(pod) {
		configMaps = app.Spec.Driver.ConfigMaps
	} else if util.IsExecutorPod(pod) {
		configMaps = app.Spec.Executor.ConfigMaps
	}

	for _, namePath := range configMaps {
		volumeName := namePath.Name + "-vol"
		if len(volumeName) > maxNameLength {
			volumeName = volumeName[0:maxNameLength]
			logger.Info(fmt.Sprintf("ConfigMap volume name is too long. Truncating to length %d. Result: %s.", maxNameLength, volumeName))
		}
		addConfigMapVolume(pod, namePath.Name, volumeName)
		addConfigMapVolumeMount(pod, volumeName, namePath.Path)
	}
	return nil
}

func addPrometheusConfig(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	// Skip if Prometheus Monitoring is not enabled or an in-container ConfigFile is used,
	// in which cases a Prometheus ConfigMap won't be created.
	if !util.PrometheusMonitoringEnabled(app) || (util.HasMetricsPropertiesFile(app) && util.HasPrometheusConfigFile(app)) {
		return nil
	}

	if util.IsDriverPod(pod) && !util.ExposeDriverMetrics(app) {
		return nil
	}
	if util.IsExecutorPod(pod) && !util.ExposeExecutorMetrics(app) {
		return nil
	}

	name := util.GetPrometheusConfigMapName(app)
	volumeName := name + "-vol"
	mountPath := common.PrometheusConfigMapMountPath
	promPort := common.DefaultPrometheusJavaAgentPort
	if app.Spec.Monitoring.Prometheus.Port != nil {
		promPort = *app.Spec.Monitoring.Prometheus.Port
	}
	promProtocol := common.DefaultPrometheusPortProtocol
	promPortName := common.DefaultPrometheusPortName
	if app.Spec.Monitoring.Prometheus.PortName != nil {
		promPortName = *app.Spec.Monitoring.Prometheus.PortName
	}
	addConfigMapVolume(pod, name, volumeName)
	addConfigMapVolumeMount(pod, volumeName, mountPath)
	logger.Info("could not mount volume %s in path %s", volumeName, mountPath)
	addContainerPort(pod, promPort, promProtocol, promPortName)
	logger.Info("could not expose port %d to scrape metrics outside the pod", promPort)
	return nil
}

func addContainerPorts(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	var ports []v1beta2.Port

	if util.IsDriverPod(pod) {
		ports = app.Spec.Driver.Ports
	} else if util.IsExecutorPod(pod) {
		ports = app.Spec.Executor.Ports
	}

	for _, p := range ports {
		addContainerPort(pod, p.ContainerPort, p.Protocol, p.Name)
		{
			logger.Info("could not expose port named %s", p.Name)
			continue
		}
	}
	return nil
}

func addContainerPort(pod *corev1.Pod, port int32, protocol string, portName string) error {
	i := findContainer(pod)
	if i < 0 {
		return fmt.Errorf("not able to add containerPort %d as Spark container was not found in pod", port)
	}

	containerPort := corev1.ContainerPort{
		Name:          portName,
		ContainerPort: port,
		Protocol:      corev1.Protocol(protocol),
	}
	pod.Spec.Containers[i].Ports = append(pod.Spec.Containers[i].Ports, containerPort)
	return nil
}

func addConfigMapVolume(pod *corev1.Pod, configMapName string, configMapVolumeName string) error {
	volume := corev1.Volume{
		Name: configMapVolumeName,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: configMapName,
				},
			},
		},
	}
	return addVolume(pod, volume)
}

func addConfigMapVolumeMount(pod *corev1.Pod, configMapVolumeName string, mountPath string) error {
	mount := corev1.VolumeMount{
		Name:      configMapVolumeName,
		ReadOnly:  true,
		MountPath: mountPath,
	}
	return addVolumeMount(pod, mount)
}

func addAffinity(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	var affinity *corev1.Affinity
	if util.IsDriverPod(pod) {
		affinity = app.Spec.Driver.Affinity
	} else if util.IsExecutorPod(pod) {
		affinity = app.Spec.Executor.Affinity
	}
	if affinity == nil {
		return nil
	}
	pod.Spec.Affinity = affinity.DeepCopy()
	return nil
}

func addTolerations(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	var tolerations []corev1.Toleration
	if util.IsDriverPod(pod) {
		tolerations = app.Spec.Driver.SparkPodSpec.Tolerations
	} else if util.IsExecutorPod(pod) {
		tolerations = app.Spec.Executor.SparkPodSpec.Tolerations
	}

	if pod.Spec.Tolerations == nil {
		pod.Spec.Tolerations = []corev1.Toleration{}
	}

	pod.Spec.Tolerations = append(pod.Spec.Tolerations, tolerations...)
	return nil
}

func addNodeSelectors(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	var nodeSelector map[string]string
	if util.IsDriverPod(pod) {
		nodeSelector = app.Spec.Driver.NodeSelector
	} else if util.IsExecutorPod(pod) {
		nodeSelector = app.Spec.Executor.NodeSelector
	}

	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = make(map[string]string)
	}

	for k, v := range nodeSelector {
		pod.Spec.NodeSelector[k] = v
	}
	return nil
}

func addDNSConfig(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	var dnsConfig *corev1.PodDNSConfig
	if util.IsDriverPod(pod) {
		dnsConfig = app.Spec.Driver.DNSConfig
	} else if util.IsExecutorPod(pod) {
		dnsConfig = app.Spec.Executor.DNSConfig
	}

	if dnsConfig != nil {
		pod.Spec.DNSConfig = dnsConfig
	}
	return nil
}

func addSchedulerName(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	var schedulerName *string
	// NOTE: Preferred to use `BatchScheduler` if application spec has it configured.
	if app.Spec.BatchScheduler != nil {
		schedulerName = app.Spec.BatchScheduler
	} else if util.IsDriverPod(pod) {
		schedulerName = app.Spec.Driver.SchedulerName
	} else if util.IsExecutorPod(pod) {
		schedulerName = app.Spec.Executor.SchedulerName
	}

	if schedulerName == nil || *schedulerName == "" {
		return nil
	}

	pod.Spec.SchedulerName = *schedulerName
	return nil
}

func addPriorityClassName(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	var priorityClassName *string
	if app.Spec.BatchSchedulerOptions != nil {
		priorityClassName = app.Spec.BatchSchedulerOptions.PriorityClassName
	}

	if priorityClassName != nil && *priorityClassName != "" {
		pod.Spec.PriorityClassName = *priorityClassName
		if pod.Spec.Priority != nil {
			pod.Spec.Priority = nil
		}
		if pod.Spec.PreemptionPolicy != nil {
			pod.Spec.PreemptionPolicy = nil
		}
	}
	return nil
}

func addPodSecurityContext(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	var securityContext *corev1.PodSecurityContext
	if util.IsDriverPod(pod) {
		securityContext = app.Spec.Driver.PodSecurityContext
	} else if util.IsExecutorPod(pod) {
		securityContext = app.Spec.Executor.PodSecurityContext
	}

	if securityContext != nil {
		pod.Spec.SecurityContext = securityContext
	}
	return nil
}

func addContainerSecurityContext(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	i := findContainer(pod)
	if util.IsDriverPod(pod) {
		if i < 0 {
			return fmt.Errorf("driver container not found in pod")
		}
		if app.Spec.Driver.SecurityContext == nil {
			return nil
		}
		pod.Spec.Containers[i].SecurityContext = app.Spec.Driver.SecurityContext
	} else if util.IsExecutorPod(pod) {
		if i < 0 {
			return fmt.Errorf("executor container not found in pod")
		}
		if app.Spec.Driver.SecurityContext == nil {
			return nil
		}
		pod.Spec.Containers[i].SecurityContext = app.Spec.Executor.SecurityContext
	}
	return nil
}

func addSidecarContainers(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	var sidecars []corev1.Container
	if util.IsDriverPod(pod) {
		sidecars = app.Spec.Driver.Sidecars
	} else if util.IsExecutorPod(pod) {
		sidecars = app.Spec.Executor.Sidecars
	}

	for _, sidecar := range sidecars {
		if !hasContainer(pod, &sidecar) {
			pod.Spec.Containers = append(pod.Spec.Containers, *sidecar.DeepCopy())
		}
	}
	return nil
}

func addInitContainers(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	var initContainers []corev1.Container
	if util.IsDriverPod(pod) {
		initContainers = app.Spec.Driver.InitContainers
	} else if util.IsExecutorPod(pod) {
		initContainers = app.Spec.Executor.InitContainers
	}

	if pod.Spec.InitContainers == nil {
		pod.Spec.InitContainers = []corev1.Container{}
	}

	for _, container := range initContainers {
		if !hasInitContainer(pod, &container) {
			pod.Spec.InitContainers = append(pod.Spec.InitContainers, *container.DeepCopy())
		}
	}
	return nil
}

func addGPU(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	var gpu *v1beta2.GPUSpec
	if util.IsDriverPod(pod) {
		gpu = app.Spec.Driver.GPU
	}
	if util.IsExecutorPod(pod) {
		gpu = app.Spec.Executor.GPU
	}
	if gpu == nil {
		return nil
	}
	if gpu.Name == "" {
		logger.V(1).Info(fmt.Sprintf("Please specify GPU resource name, such as: nvidia.com/gpu, amd.com/gpu etc. Current gpu spec: %+v", gpu))
		return nil
	}
	if gpu.Quantity <= 0 {
		logger.V(1).Info(fmt.Sprintf("GPU Quantity must be positive. Current gpu spec: %+v", gpu))
		return nil
	}

	i := findContainer(pod)
	if i < 0 {
		return fmt.Errorf("not able to add GPU as Spark container was not found in pod %s", pod.Name)
	}
	if pod.Spec.Containers[i].Resources.Limits == nil {
		pod.Spec.Containers[i].Resources.Limits = make(corev1.ResourceList)
	}
	pod.Spec.Containers[i].Resources.Limits[corev1.ResourceName(gpu.Name)] = *resource.NewQuantity(gpu.Quantity, resource.DecimalSI)
	return nil
}

func addHostNetwork(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	var hostNetwork *bool
	if util.IsDriverPod(pod) {
		hostNetwork = app.Spec.Driver.HostNetwork
	}
	if util.IsExecutorPod(pod) {
		hostNetwork = app.Spec.Executor.HostNetwork
	}

	if hostNetwork == nil || !*hostNetwork {
		return nil
	}

	// For Pods with hostNetwork, explicitly set its DNS policy to “ClusterFirstWithHostNet”
	// Detail: https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/#pod-s-dns-policy
	pod.Spec.HostNetwork = true
	pod.Spec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
	return nil
}

func addTerminationGracePeriodSeconds(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	var gracePeriodSeconds *int64
	if util.IsDriverPod(pod) {
		gracePeriodSeconds = app.Spec.Driver.TerminationGracePeriodSeconds
	} else if util.IsExecutorPod(pod) {
		gracePeriodSeconds = app.Spec.Executor.TerminationGracePeriodSeconds
	}

	if gracePeriodSeconds == nil {
		return nil
	}

	pod.Spec.TerminationGracePeriodSeconds = gracePeriodSeconds
	return nil
}

func addPodLifeCycleConfig(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	var lifeCycle *corev1.Lifecycle
	var containerName string
	if util.IsDriverPod(pod) {
		lifeCycle = app.Spec.Driver.Lifecycle
		containerName = common.SparkDriverContainerName
	} else if util.IsExecutorPod(pod) {
		lifeCycle = app.Spec.Executor.Lifecycle
		containerName = common.SparkExecutorContainerName
	}
	if lifeCycle == nil {
		return nil
	}

	i := 0
	// Find the driver container in the pod.
	for ; i < len(pod.Spec.Containers); i++ {
		if pod.Spec.Containers[i].Name == containerName {
			break
		}
	}
	if i == len(pod.Spec.Containers) {
		logger.Info("Spark container %s not found in pod %s", containerName, pod.Name)
		return nil
	}

	pod.Spec.Containers[i].Lifecycle = lifeCycle
	return nil
}

func addHostAliases(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	var hostAliases []corev1.HostAlias
	if util.IsDriverPod(pod) {
		hostAliases = app.Spec.Driver.HostAliases
	} else if util.IsExecutorPod(pod) {
		hostAliases = app.Spec.Executor.HostAliases
	}

	pod.Spec.HostAliases = append(pod.Spec.HostAliases, hostAliases...)
	return nil
}

func addShareProcessNamespace(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	var shareProcessNamespace *bool
	if util.IsDriverPod(pod) {
		shareProcessNamespace = app.Spec.Driver.ShareProcessNamespace
	} else if util.IsExecutorPod(pod) {
		shareProcessNamespace = app.Spec.Executor.ShareProcessNamespace
	}

	if shareProcessNamespace == nil || !*shareProcessNamespace {
		return nil
	}

	pod.Spec.ShareProcessNamespace = shareProcessNamespace
	return nil
}

func findContainer(pod *corev1.Pod) int {
	var candidateContainerNames []string
	if util.IsDriverPod(pod) {
		candidateContainerNames = append(candidateContainerNames, common.SparkDriverContainerName)
	} else if util.IsExecutorPod(pod) {
		// Spark 3.x changed the default executor container name so we need to include both.
		candidateContainerNames = append(candidateContainerNames, common.SparkExecutorContainerName, common.Spark3DefaultExecutorContainerName)
	}

	if len(candidateContainerNames) == 0 {
		return -1
	}

	for i := 0; i < len(pod.Spec.Containers); i++ {
		for _, name := range candidateContainerNames {
			if pod.Spec.Containers[i].Name == name {
				return i
			}
		}
	}
	return -1
}

func hasContainer(pod *corev1.Pod, container *corev1.Container) bool {
	for _, c := range pod.Spec.Containers {
		if container.Name == c.Name && container.Image == c.Image {
			return true
		}
	}
	return false
}

func hasInitContainer(pod *corev1.Pod, container *corev1.Container) bool {
	for _, c := range pod.Spec.InitContainers {
		if container.Name == c.Name && container.Image == c.Image {
			return true
		}
	}
	return false
}
