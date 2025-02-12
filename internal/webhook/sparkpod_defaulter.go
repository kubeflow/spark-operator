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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
func NewSparkPodDefaulter(client client.Client, namespaces []string) *SparkPodDefaulter {
	nsMap := make(map[string]bool)
	if len(namespaces) == 0 {
		nsMap[metav1.NamespaceAll] = true
	} else {
		for _, ns := range namespaces {
			nsMap[ns] = true
		}
	}

	return &SparkPodDefaulter{
		client:             client,
		sparkJobNamespaces: nsMap,
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
	mutator := newSparkPodMutator(pod, app)
	if err := mutator.mutate(); err != nil {
		logger.Info("Denying Spark pod", "name", pod.Name, "namespace", namespace, "errorMessage", err.Error())
		return fmt.Errorf("failed to mutate Spark pod: %v", err)
	}
	return nil
}

func (d *SparkPodDefaulter) isSparkJobNamespace(ns string) bool {
	return d.sparkJobNamespaces[metav1.NamespaceAll] || d.sparkJobNamespaces[ns]
}

type mutateSparkPodOption func(mutator *SparkPodMutator) error

type SparkPodMutator struct {
	pod       *corev1.Pod
	container *corev1.Container
	app       *v1beta2.SparkApplication
}

func newSparkPodMutator(pod *corev1.Pod, app *v1beta2.SparkApplication) *SparkPodMutator {
	return &SparkPodMutator{
		pod:       pod,
		container: findContainer(pod),
		app:       app,
	}
}

func (m *SparkPodMutator) getSparkPodSpec() *v1beta2.SparkPodSpec {
	if util.IsDriverPod(m.pod) {
		return &m.app.Spec.Driver.SparkPodSpec
	}
	return &m.app.Spec.Executor.SparkPodSpec
}

func (m *SparkPodMutator) mutate() error {
	if err := m.validate(); err != nil {
		return err
	}

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
		if err := option(m); err != nil {
			return err
		}
	}

	return nil
}

func (m *SparkPodMutator) validate() error {
	if util.IsDriverPod(m.pod) || util.IsExecutorPod(m.pod) {
		if m.container == nil {
			return fmt.Errorf("container not found")
		}
		return nil
	}
	return fmt.Errorf("pod is not a driver or executor")

}

func addOwnerReference(m *SparkPodMutator) error {
	if !util.IsDriverPod(m.pod) {
		return nil
	}
	ownerReference := util.GetOwnerReference(m.app)
	m.pod.ObjectMeta.OwnerReferences = append(m.pod.ObjectMeta.OwnerReferences, ownerReference)
	return nil
}

func addVolumes(m *SparkPodMutator) error {
	volumes := m.app.Spec.Volumes

	volumeMap := make(map[string]corev1.Volume)
	for _, v := range volumes {
		volumeMap[v.Name] = v
	}

	volumeMounts := m.getSparkPodSpec().VolumeMounts

	addedVolumeMap := make(map[string]corev1.Volume)
	for _, mount := range volumeMounts {
		// Skip adding localDirVolumes
		if strings.HasPrefix(mount.Name, common.SparkLocalDirVolumePrefix) {
			continue
		}

		if v, ok := volumeMap[mount.Name]; ok {
			if _, ok := addedVolumeMap[mount.Name]; !ok {
				addVolume(m.pod, v)
				addedVolumeMap[mount.Name] = v
			}
			addVolumeMount(m.container, mount)
		}
	}
	return nil
}

func addVolume(pod *corev1.Pod, volume corev1.Volume) {
	pod.Spec.Volumes = append(pod.Spec.Volumes, volume)
}

func addVolumeMount(container *corev1.Container, mount corev1.VolumeMount) {
	container.VolumeMounts = append(container.VolumeMounts, mount)
}

func addEnvVars(m *SparkPodMutator) error {
	envs := m.getSparkPodSpec().Env
	m.container.Env = append(m.container.Env, envs...)
	return nil
}

func addEnvFrom(m *SparkPodMutator) error {
	envFrom := m.getSparkPodSpec().EnvFrom
	m.container.EnvFrom = append(m.container.EnvFrom, envFrom...)
	return nil
}

func addEnvironmentVariable(container *corev1.Container, name, value string) {
	container.Env = append(container.Env, corev1.EnvVar{
		Name:  name,
		Value: value,
	})
}

func addSparkConfigMap(m *SparkPodMutator) error {
	if m.app.Spec.SparkConfigMap == nil {
		return nil
	}

	addConfigMapVolume(m.pod, *m.app.Spec.SparkConfigMap, common.SparkConfigMapVolumeName)
	addConfigMapVolumeMount(m.container, common.SparkConfigMapVolumeName, common.DefaultSparkConfDir)
	addEnvironmentVariable(m.container, common.EnvSparkConfDir, common.DefaultSparkConfDir)
	return nil
}

func addHadoopConfigMap(m *SparkPodMutator) error {
	if m.app.Spec.HadoopConfigMap == nil {
		return nil
	}

	addConfigMapVolume(m.pod, *m.app.Spec.HadoopConfigMap, common.HadoopConfigMapVolumeName)
	addConfigMapVolumeMount(m.container, common.HadoopConfigMapVolumeName, common.DefaultHadoopConfDir)
	addEnvironmentVariable(m.container, common.EnvHadoopConfDir, common.DefaultHadoopConfDir)

	return nil
}

func addGeneralConfigMaps(m *SparkPodMutator) error {
	configMaps := m.getSparkPodSpec().ConfigMaps

	for _, namePath := range configMaps {
		volumeName := namePath.Name + "-vol"
		if len(volumeName) > maxNameLength {
			volumeName = volumeName[0:maxNameLength]
			logger.Info(fmt.Sprintf("ConfigMap volume name is too long. Truncating to length %d. Result: %s.", maxNameLength, volumeName))
		}
		addConfigMapVolume(m.pod, namePath.Name, volumeName)
		addConfigMapVolumeMount(m.container, volumeName, namePath.Path)
	}
	return nil
}

func addPrometheusConfig(m *SparkPodMutator) error {
	// Skip if Prometheus Monitoring is not enabled or an in-container ConfigFile is used,
	// in which cases a Prometheus ConfigMap won't be created.
	if !util.PrometheusMonitoringEnabled(m.app) || (util.HasMetricsPropertiesFile(m.app) && util.HasPrometheusConfigFile(m.app)) {
		return nil
	}

	if util.IsDriverPod(m.pod) && !util.ExposeDriverMetrics(m.app) {
		return nil
	}
	if util.IsExecutorPod(m.pod) && !util.ExposeExecutorMetrics(m.app) {
		return nil
	}

	name := util.GetPrometheusConfigMapName(m.app)
	volumeName := name + "-vol"
	mountPath := common.PrometheusConfigMapMountPath
	promPort := common.DefaultPrometheusJavaAgentPort
	if m.app.Spec.Monitoring.Prometheus.Port != nil {
		promPort = *m.app.Spec.Monitoring.Prometheus.Port
	}
	promProtocol := common.DefaultPrometheusPortProtocol
	promPortName := common.DefaultPrometheusPortName
	if m.app.Spec.Monitoring.Prometheus.PortName != nil {
		promPortName = *m.app.Spec.Monitoring.Prometheus.PortName
	}

	addConfigMapVolume(m.pod, name, volumeName)
	addConfigMapVolumeMount(m.container, volumeName, mountPath)
	addContainerPort(m.container, promPort, promProtocol, promPortName)

	return nil
}

func addContainerPorts(m *SparkPodMutator) error {
	ports := m.getSparkPodSpec().Ports

	for _, p := range ports {
		addContainerPort(m.container, p.ContainerPort, p.Protocol, p.Name)
	}
	return nil
}

func addContainerPort(container *corev1.Container, port int32, protocol string, portName string) {
	containerPort := corev1.ContainerPort{
		Name:          portName,
		ContainerPort: port,
		Protocol:      corev1.Protocol(protocol),
	}
	container.Ports = append(container.Ports, containerPort)
}

func addConfigMapVolume(pod *corev1.Pod, configMapName string, configMapVolumeName string) {
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
	addVolume(pod, volume)
}

func addConfigMapVolumeMount(container *corev1.Container, configMapVolumeName string, mountPath string) {
	mount := corev1.VolumeMount{
		Name:      configMapVolumeName,
		ReadOnly:  true,
		MountPath: mountPath,
	}
	addVolumeMount(container, mount)
}

func addAffinity(m *SparkPodMutator) error {
	affinity := m.getSparkPodSpec().Affinity
	if affinity == nil {
		return nil
	}
	m.pod.Spec.Affinity = affinity.DeepCopy()
	return nil
}

func addTolerations(m *SparkPodMutator) error {
	tolerations := m.getSparkPodSpec().Tolerations

	if m.pod.Spec.Tolerations == nil {
		m.pod.Spec.Tolerations = []corev1.Toleration{}
	}

	m.pod.Spec.Tolerations = append(m.pod.Spec.Tolerations, tolerations...)
	return nil
}

func addNodeSelectors(m *SparkPodMutator) error {
	nodeSelector := m.getSparkPodSpec().NodeSelector

	if m.pod.Spec.NodeSelector == nil {
		m.pod.Spec.NodeSelector = make(map[string]string)
	}

	for k, v := range nodeSelector {
		m.pod.Spec.NodeSelector[k] = v
	}
	return nil
}

func addDNSConfig(m *SparkPodMutator) error {
	dnsConfig := m.getSparkPodSpec().DNSConfig

	if dnsConfig != nil {
		m.pod.Spec.DNSConfig = dnsConfig
	}
	return nil
}

func addSchedulerName(m *SparkPodMutator) error {
	var schedulerName *string
	// NOTE: Preferred to use `BatchScheduler` if application spec has it configured.
	if m.app.Spec.BatchScheduler != nil {
		schedulerName = m.app.Spec.BatchScheduler
	} else {
		schedulerName = m.getSparkPodSpec().SchedulerName
	}

	if schedulerName == nil || *schedulerName == "" {
		return nil
	}

	m.pod.Spec.SchedulerName = *schedulerName
	return nil
}

func addPriorityClassName(m *SparkPodMutator) error {
	priorityClassName := m.getSparkPodSpec().PriorityClassName

	if priorityClassName != nil && *priorityClassName != "" {
		m.pod.Spec.PriorityClassName = *priorityClassName
		m.pod.Spec.Priority = nil
		m.pod.Spec.PreemptionPolicy = nil
	}

	return nil
}

func addPodSecurityContext(m *SparkPodMutator) error {
	securityContext := m.getSparkPodSpec().PodSecurityContext

	if securityContext != nil {
		m.pod.Spec.SecurityContext = securityContext
	}
	return nil
}

func addContainerSecurityContext(m *SparkPodMutator) error {
	securityContext := m.getSparkPodSpec().SecurityContext
	if securityContext != nil {
		m.container.SecurityContext = securityContext
	}
	return nil
}

func addSidecarContainers(m *SparkPodMutator) error {
	sidecars := m.getSparkPodSpec().Sidecars

	for _, sidecar := range sidecars {
		if !hasContainer(m.pod.Spec.Containers, &sidecar) {
			m.pod.Spec.Containers = append(m.pod.Spec.Containers, *sidecar.DeepCopy())
		}
	}
	return nil
}

func addInitContainers(m *SparkPodMutator) error {
	initContainers := m.getSparkPodSpec().InitContainers

	for _, container := range initContainers {
		if !hasContainer(m.pod.Spec.InitContainers, &container) {
			m.pod.Spec.InitContainers = append(m.pod.Spec.InitContainers, *container.DeepCopy())
		}
	}
	return nil
}

func addGPU(m *SparkPodMutator) error {
	gpu := m.getSparkPodSpec().GPU
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

	if m.container.Resources.Limits == nil {
		m.container.Resources.Limits = make(corev1.ResourceList)
	}
	m.container.Resources.Limits[corev1.ResourceName(gpu.Name)] = *resource.NewQuantity(gpu.Quantity, resource.DecimalSI)
	return nil
}

func addHostNetwork(m *SparkPodMutator) error {
	hostNetwork := m.getSparkPodSpec().HostNetwork
	if hostNetwork == nil || !*hostNetwork {
		return nil
	}

	// For Pods with hostNetwork, explicitly set its DNS policy to “ClusterFirstWithHostNet”
	// Detail: https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/#pod-s-dns-policy
	m.pod.Spec.HostNetwork = true
	m.pod.Spec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
	return nil
}

func addTerminationGracePeriodSeconds(m *SparkPodMutator) error {
	gracePeriodSeconds := m.getSparkPodSpec().TerminationGracePeriodSeconds
	if gracePeriodSeconds == nil {
		return nil
	}

	m.pod.Spec.TerminationGracePeriodSeconds = gracePeriodSeconds
	return nil
}

func addPodLifeCycleConfig(m *SparkPodMutator) error {
	lifecycle := m.getSparkPodSpec().Lifecycle
	if lifecycle == nil {
		return nil
	}
	m.container.Lifecycle = lifecycle.DeepCopy()
	return nil
}

func addHostAliases(m *SparkPodMutator) error {
	hostAliases := m.getSparkPodSpec().HostAliases
	m.pod.Spec.HostAliases = append(m.pod.Spec.HostAliases, hostAliases...)
	return nil
}

func addShareProcessNamespace(m *SparkPodMutator) error {
	shareProcessNamespace := m.getSparkPodSpec().ShareProcessNamespace
	if shareProcessNamespace == nil || !*shareProcessNamespace {
		return nil
	}

	m.pod.Spec.ShareProcessNamespace = shareProcessNamespace
	return nil
}

func findContainer(pod *corev1.Pod) *corev1.Container {
	var candidateContainerNames []string
	if util.IsDriverPod(pod) {
		candidateContainerNames = append(candidateContainerNames, common.SparkDriverContainerName)
	} else if util.IsExecutorPod(pod) {
		// Spark 3.x changed the default executor container name so we need to include both.
		candidateContainerNames = append(candidateContainerNames, common.SparkExecutorContainerName, common.Spark3DefaultExecutorContainerName)
	}

	if len(candidateContainerNames) == 0 {
		return nil
	}

	for i := 0; i < len(pod.Spec.Containers); i++ {
		for _, name := range candidateContainerNames {
			if pod.Spec.Containers[i].Name == name {
				return &pod.Spec.Containers[i]
			}
		}
	}
	return nil
}

func hasContainer(containers []corev1.Container, container *corev1.Container) bool {
	for _, c := range containers {
		if container.Name == c.Name && container.Image == c.Image {
			return true
		}
	}
	return false
}
