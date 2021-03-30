/*
Copyright 2018 Google LLC

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
	"fmt"
	"strings"

	"github.com/golang/glog"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/util"
)

const (
	maxNameLength = 63
)

// patchOperation represents a RFC6902 JSON patch operation.
type patchOperation struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value,omitempty"`
}

func patchSparkPod(pod *corev1.Pod, app *v1beta2.SparkApplication) []patchOperation {
	var patchOps []patchOperation

	if util.IsDriverPod(pod) {
		patchOps = append(patchOps, addOwnerReference(pod, app))
	}

	patchOps = append(patchOps, addVolumes(pod, app)...)
	patchOps = append(patchOps, addGeneralConfigMaps(pod, app)...)
	patchOps = append(patchOps, addSparkConfigMap(pod, app)...)
	patchOps = append(patchOps, addHadoopConfigMap(pod, app)...)
	patchOps = append(patchOps, getPrometheusConfigPatches(pod, app)...)
	patchOps = append(patchOps, addTolerations(pod, app)...)
	patchOps = append(patchOps, addSidecarContainers(pod, app)...)
	patchOps = append(patchOps, addInitContainers(pod, app)...)
	patchOps = append(patchOps, addHostNetwork(pod, app)...)
	patchOps = append(patchOps, addNodeSelectors(pod, app)...)
	patchOps = append(patchOps, addDNSConfig(pod, app)...)
	patchOps = append(patchOps, addEnvVars(pod, app)...)
	patchOps = append(patchOps, addEnvFrom(pod, app)...)
	patchOps = append(patchOps, addHostAliases(pod, app)...)
	patchOps = append(patchOps, addContainerPorts(pod, app)...)
	patchOps = append(patchOps, addPriorityClassName(pod, app)...)

	op := addSchedulerName(pod, app)
	if op != nil {
		patchOps = append(patchOps, *op)
	}

	if pod.Spec.Affinity == nil {
		op := addAffinity(pod, app)
		if op != nil {
			patchOps = append(patchOps, *op)
		}
	}

	op = addPodSecurityContext(pod, app)
	if op != nil {
		patchOps = append(patchOps, *op)
	}

	op = addSecurityContext(pod, app)
	if op != nil {
		patchOps = append(patchOps, *op)
	}

	op = addGPU(pod, app)
	if op != nil {
		patchOps = append(patchOps, *op)
	}

	op = addTerminationGracePeriodSeconds(pod, app)
	if op != nil {
		patchOps = append(patchOps, *op)
	}

	op = addPodLifeCycleConfig(pod, app)
	if op != nil {
		patchOps = append(patchOps, *op)
	}

	return patchOps
}

func addOwnerReference(pod *corev1.Pod, app *v1beta2.SparkApplication) patchOperation {
	ownerReference := util.GetOwnerReference(app)

	path := "/metadata/ownerReferences"
	var value interface{}
	if len(pod.OwnerReferences) == 0 {
		value = []metav1.OwnerReference{ownerReference}
	} else {
		path += "/-"
		value = ownerReference
	}

	return patchOperation{Op: "add", Path: path, Value: value}
}

func addVolumes(pod *corev1.Pod, app *v1beta2.SparkApplication) []patchOperation {
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

	var ops []patchOperation
	addedVolumeMap := make(map[string]corev1.Volume)
	for _, m := range volumeMounts {
		// Skip adding localDirVolumes
		if strings.HasPrefix(m.Name, config.SparkLocalDirVolumePrefix) {
			continue
		}

		if v, ok := volumeMap[m.Name]; ok {
			if _, ok := addedVolumeMap[m.Name]; !ok {
				ops = append(ops, addVolume(pod, v))
				addedVolumeMap[m.Name] = v
			}
			vmPatchOp := addVolumeMount(pod, m)
			if vmPatchOp == nil {
				return nil
			}
			ops = append(ops, *vmPatchOp)
		}
	}

	return ops
}

func addVolume(pod *corev1.Pod, volume corev1.Volume) patchOperation {
	path := "/spec/volumes"
	var value interface{}
	if len(pod.Spec.Volumes) == 0 {
		value = []corev1.Volume{volume}
	} else {
		path += "/-"
		value = volume
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, volume)

	return patchOperation{Op: "add", Path: path, Value: value}
}

func addVolumeMount(pod *corev1.Pod, mount corev1.VolumeMount) *patchOperation {
	i := findContainer(pod)
	if i < 0 {
		glog.Warningf("not able to add VolumeMount %s as Spark container was not found in pod %s", mount.Name, pod.Name)
		return nil
	}

	path := fmt.Sprintf("/spec/containers/%d/volumeMounts", i)
	var value interface{}
	if len(pod.Spec.Containers[i].VolumeMounts) == 0 {
		value = []corev1.VolumeMount{mount}
	} else {
		path += "/-"
		value = mount
	}
	pod.Spec.Containers[i].VolumeMounts = append(pod.Spec.Containers[i].VolumeMounts, mount)

	return &patchOperation{Op: "add", Path: path, Value: value}
}

func addEnvVars(pod *corev1.Pod, app *v1beta2.SparkApplication) []patchOperation {
	var envVars []corev1.EnvVar
	if util.IsDriverPod(pod) {
		envVars = app.Spec.Driver.Env
	} else if util.IsExecutorPod(pod) {
		envVars = app.Spec.Executor.Env
	}

	i := findContainer(pod)
	if i < 0 {
		glog.Warningf("not able to add EnvVars as Spark container was not found in pod %s", pod.Name)
		return nil
	}
	basePath := fmt.Sprintf("/spec/containers/%d/env", i)

	var value interface{}
	var patchOps []patchOperation

	first := false
	if len(pod.Spec.Containers[i].Env) == 0 {
		first = true
	}

	for _, envVar := range envVars {
		path := basePath
		if first {
			value = []corev1.EnvVar{envVar}
			first = false
		} else {
			path += "/-"
			value = envVar
		}
		patchOps = append(patchOps, patchOperation{Op: "add", Path: path, Value: value})
	}
	return patchOps
}

func addEnvFrom(pod *corev1.Pod, app *v1beta2.SparkApplication) []patchOperation {
	var envFrom []corev1.EnvFromSource
	if util.IsDriverPod(pod) {
		envFrom = app.Spec.Driver.EnvFrom
	} else if util.IsExecutorPod(pod) {
		envFrom = app.Spec.Executor.EnvFrom
	}

	i := findContainer(pod)
	if i < 0 {
		glog.Warningf("not able to add EnvFrom as Spark container was not found in pod %s", pod.Name)
		return nil
	}
	basePath := fmt.Sprintf("/spec/containers/%d/envFrom", i)

	var value interface{}
	var patchOps []patchOperation

	first := false
	if len(pod.Spec.Containers[i].EnvFrom) == 0 {
		first = true
	}

	for _, ef := range envFrom {
		path := basePath
		if first {
			value = []corev1.EnvFromSource{ef}
			first = false
		} else {
			path += "/-"
			value = ef
		}
		patchOps = append(patchOps, patchOperation{Op: "add", Path: path, Value: value})
	}
	return patchOps
}

func addEnvironmentVariable(pod *corev1.Pod, envName, envValue string) *patchOperation {
	i := findContainer(pod)
	if i < 0 {
		glog.Warningf("not able to add environment variable %s as Spark container was not found in pod %s", envName, pod.Name)
		return nil
	}

	path := fmt.Sprintf("/spec/containers/%d/env", i)
	var value interface{}
	if len(pod.Spec.Containers[i].Env) == 0 {
		value = []corev1.EnvVar{{Name: envName, Value: envValue}}
	} else {
		path += "/-"
		value = corev1.EnvVar{Name: envName, Value: envValue}
	}

	return &patchOperation{Op: "add", Path: path, Value: value}
}

func addSparkConfigMap(pod *corev1.Pod, app *v1beta2.SparkApplication) []patchOperation {
	var patchOps []patchOperation
	sparkConfigMapName := app.Spec.SparkConfigMap
	if sparkConfigMapName != nil {
		patchOps = append(patchOps, addConfigMapVolume(pod, *sparkConfigMapName, config.SparkConfigMapVolumeName))
		vmPatchOp := addConfigMapVolumeMount(pod, config.SparkConfigMapVolumeName, config.DefaultSparkConfDir)
		if vmPatchOp == nil {
			return nil
		}
		patchOps = append(patchOps, *vmPatchOp)
		envPatchOp := addEnvironmentVariable(pod, config.SparkConfDirEnvVar, config.DefaultSparkConfDir)
		if envPatchOp == nil {
			return nil
		}
		patchOps = append(patchOps, *envPatchOp)
	}
	return patchOps
}

func addHadoopConfigMap(pod *corev1.Pod, app *v1beta2.SparkApplication) []patchOperation {
	var patchOps []patchOperation
	hadoopConfigMapName := app.Spec.HadoopConfigMap
	if hadoopConfigMapName != nil {
		patchOps = append(patchOps, addConfigMapVolume(pod, *hadoopConfigMapName, config.HadoopConfigMapVolumeName))
		vmPatchOp := addConfigMapVolumeMount(pod, config.HadoopConfigMapVolumeName, config.DefaultHadoopConfDir)
		if vmPatchOp == nil {
			return nil
		}
		patchOps = append(patchOps, *vmPatchOp)
		envPatchOp := addEnvironmentVariable(pod, config.HadoopConfDirEnvVar, config.DefaultHadoopConfDir)
		if envPatchOp == nil {
			return nil
		}
		patchOps = append(patchOps, *envPatchOp)
	}
	return patchOps
}

func addGeneralConfigMaps(pod *corev1.Pod, app *v1beta2.SparkApplication) []patchOperation {
	var configMaps []v1beta2.NamePath
	if util.IsDriverPod(pod) {
		configMaps = app.Spec.Driver.ConfigMaps
	} else if util.IsExecutorPod(pod) {
		configMaps = app.Spec.Executor.ConfigMaps
	}

	var patchOps []patchOperation
	for _, namePath := range configMaps {
		volumeName := namePath.Name + "-vol"
		if len(volumeName) > maxNameLength {
			volumeName = volumeName[0:maxNameLength]
			glog.V(2).Infof("ConfigMap volume name is too long. Truncating to length %d. Result: %s.", maxNameLength, volumeName)
		}
		patchOps = append(patchOps, addConfigMapVolume(pod, namePath.Name, volumeName))
		vmPatchOp := addConfigMapVolumeMount(pod, volumeName, namePath.Path)
		if vmPatchOp == nil {
			return nil
		}
		patchOps = append(patchOps, *vmPatchOp)
	}
	return patchOps
}

func getPrometheusConfigPatches(pod *corev1.Pod, app *v1beta2.SparkApplication) []patchOperation {
	// Skip if Prometheus Monitoring is not enabled or an in-container ConfigFile is used,
	// in which cases a Prometheus ConfigMap won't be created.
	if !app.PrometheusMonitoringEnabled() || (app.HasMetricsPropertiesFile() && app.HasPrometheusConfigFile()) {
		return nil
	}

	if util.IsDriverPod(pod) && !app.ExposeDriverMetrics() {
		return nil
	}
	if util.IsExecutorPod(pod) && !app.ExposeExecutorMetrics() {
		return nil
	}

	var patchOps []patchOperation
	name := config.GetPrometheusConfigMapName(app)
	volumeName := name + "-vol"
	mountPath := config.PrometheusConfigMapMountPath
	promPort := config.DefaultPrometheusJavaAgentPort
	if app.Spec.Monitoring.Prometheus.Port != nil {
		promPort = *app.Spec.Monitoring.Prometheus.Port
	}
	promProtocol := config.DefaultPrometheusPortProtocol
	promPortName := config.DefaultPrometheusPortName
	if app.Spec.Monitoring.Prometheus.PortName != nil {
		promPortName = *app.Spec.Monitoring.Prometheus.PortName
	}

	patchOps = append(patchOps, addConfigMapVolume(pod, name, volumeName))
	vmPatchOp := addConfigMapVolumeMount(pod, volumeName, mountPath)
	if vmPatchOp == nil {
		glog.Warningf("could not mount volume %s in path %s", volumeName, mountPath)
		return nil
	}
	patchOps = append(patchOps, *vmPatchOp)
	promPortPatchOp := addContainerPort(pod, promPort, promProtocol, promPortName)
	if promPortPatchOp == nil {
		glog.Warningf("could not expose port %d to scrape metrics outside the pod", promPort)
		return nil
	}
	patchOps = append(patchOps, *promPortPatchOp)
	return patchOps
}

func addContainerPorts(pod *corev1.Pod, app *v1beta2.SparkApplication) []patchOperation {
	var ports []v1beta2.Port

	if util.IsDriverPod(pod) {
		ports = app.Spec.Driver.Ports
	} else if util.IsExecutorPod(pod) {
		ports = app.Spec.Executor.Ports
	}

	var patchOps []patchOperation
	for _, p := range ports {
		portPatchOp := addContainerPort(pod, p.ContainerPort, p.Protocol, p.Name)
		if portPatchOp == nil {
			glog.Warningf("could not expose port named %s", p.Name)
			continue
		}
		patchOps = append(patchOps, *portPatchOp)
	}
	return patchOps
}

func addContainerPort(pod *corev1.Pod, port int32, protocol string, portName string) *patchOperation {
	i := findContainer(pod)
	if i < 0 {
		glog.Warningf("not able to add containerPort %d as Spark container was not found in pod %s", port, pod.Name)
		return nil
	}

	path := fmt.Sprintf("/spec/containers/%d/ports", i)
	containerPort := corev1.ContainerPort{
		Name:          portName,
		ContainerPort: port,
		Protocol:      corev1.Protocol(protocol),
	}
	var value interface{}
	if len(pod.Spec.Containers[i].Ports) == 0 {
		value = []corev1.ContainerPort{containerPort}
	} else {
		path += "/-"
		value = containerPort
	}
	pod.Spec.Containers[i].Ports = append(pod.Spec.Containers[i].Ports, containerPort)
	return &patchOperation{Op: "add", Path: path, Value: value}
}

func addConfigMapVolume(pod *corev1.Pod, configMapName string, configMapVolumeName string) patchOperation {
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

func addConfigMapVolumeMount(pod *corev1.Pod, configMapVolumeName string, mountPath string) *patchOperation {
	mount := corev1.VolumeMount{
		Name:      configMapVolumeName,
		ReadOnly:  true,
		MountPath: mountPath,
	}
	return addVolumeMount(pod, mount)
}

func addAffinity(pod *corev1.Pod, app *v1beta2.SparkApplication) *patchOperation {
	var affinity *corev1.Affinity
	if util.IsDriverPod(pod) {
		affinity = app.Spec.Driver.Affinity
	} else if util.IsExecutorPod(pod) {
		affinity = app.Spec.Executor.Affinity
	}

	if affinity == nil {
		return nil
	}
	return &patchOperation{Op: "add", Path: "/spec/affinity", Value: *affinity}
}

func addTolerations(pod *corev1.Pod, app *v1beta2.SparkApplication) []patchOperation {
	var tolerations []corev1.Toleration
	if util.IsDriverPod(pod) {
		tolerations = app.Spec.Driver.Tolerations
	} else if util.IsExecutorPod(pod) {
		tolerations = app.Spec.Executor.Tolerations
	}

	first := false
	if len(pod.Spec.Tolerations) == 0 {
		first = true
	}

	var ops []patchOperation
	for _, v := range tolerations {
		ops = append(ops, addToleration(pod, v, first))
		if first {
			first = false
		}
	}
	return ops
}

func addNodeSelectors(pod *corev1.Pod, app *v1beta2.SparkApplication) []patchOperation {
	var nodeSelector map[string]string
	if util.IsDriverPod(pod) {
		nodeSelector = app.Spec.Driver.NodeSelector
	} else if util.IsExecutorPod(pod) {
		nodeSelector = app.Spec.Executor.NodeSelector
	}

	var ops []patchOperation
	if len(nodeSelector) > 0 {
		ops = append(ops, patchOperation{Op: "add", Path: "/spec/nodeSelector", Value: nodeSelector})
	}
	return ops
}

func addDNSConfig(pod *corev1.Pod, app *v1beta2.SparkApplication) []patchOperation {
	var dnsConfig *corev1.PodDNSConfig

	if util.IsDriverPod(pod) {
		dnsConfig = app.Spec.Driver.DNSConfig
	} else if util.IsExecutorPod(pod) {
		dnsConfig = app.Spec.Executor.DNSConfig
	}

	var ops []patchOperation
	if dnsConfig != nil {
		ops = append(ops, patchOperation{Op: "add", Path: "/spec/dnsConfig", Value: dnsConfig})
	}
	return ops
}

func addSchedulerName(pod *corev1.Pod, app *v1beta2.SparkApplication) *patchOperation {
	var schedulerName *string

	//NOTE: Preferred to use `BatchScheduler` if application spec has it configured.
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
	return &patchOperation{Op: "add", Path: "/spec/schedulerName", Value: *schedulerName}
}

func addPriorityClassName(pod *corev1.Pod, app *v1beta2.SparkApplication) []patchOperation {
	var priorityClassName *string

	if app.Spec.BatchSchedulerOptions != nil {
		priorityClassName = app.Spec.BatchSchedulerOptions.PriorityClassName
	}

	var ops []patchOperation
	if priorityClassName != nil && *priorityClassName != "" {
		ops = append(ops, patchOperation{Op: "add", Path: "/spec/priorityClassName", Value: *priorityClassName})

		if pod.Spec.Priority != nil {
			ops = append(ops, patchOperation{Op: "remove", Path: "/spec/priority"})
		}
	}

	return ops
}

func addToleration(pod *corev1.Pod, toleration corev1.Toleration, first bool) patchOperation {
	path := "/spec/tolerations"
	var value interface{}
	if first {
		value = []corev1.Toleration{toleration}
	} else {
		path += "/-"
		value = toleration
	}

	return patchOperation{Op: "add", Path: path, Value: value}
}

func addPodSecurityContext(pod *corev1.Pod, app *v1beta2.SparkApplication) *patchOperation {
	var secContext *corev1.PodSecurityContext
	if util.IsDriverPod(pod) {
		secContext = app.Spec.Driver.PodSecurityContext
	} else if util.IsExecutorPod(pod) {
		secContext = app.Spec.Executor.PodSecurityContext
	}

	if secContext == nil {
		return nil
	}
	return &patchOperation{Op: "add", Path: "/spec/securityContext", Value: *secContext}
}

func addSecurityContext(pod *corev1.Pod, app *v1beta2.SparkApplication) *patchOperation {
	var secContext *corev1.SecurityContext
	if util.IsDriverPod(pod) {
		secContext = app.Spec.Driver.SecurityContext
	} else if util.IsExecutorPod(pod) {
		secContext = app.Spec.Executor.SecurityContext
	}

	if secContext == nil {
		return nil
	}

	i := 0
	// Find the driver/executor container in the pod.
	for ; i < len(pod.Spec.Containers); i++ {
		if pod.Spec.Containers[i].Name == config.SparkDriverContainerName || pod.Spec.Containers[i].Name == config.SparkExecutorContainerName {
			break
		}
	}
	if i == len(pod.Spec.Containers) {
		glog.Warningf("Spark driver/executor container not found in pod %s", pod.Name)
		return nil
	}

	path := fmt.Sprintf("/spec/containers/%d/securityContext", i)
	return &patchOperation{Op: "add", Path: path, Value: *secContext}
}

func addSidecarContainers(pod *corev1.Pod, app *v1beta2.SparkApplication) []patchOperation {
	var sidecars []corev1.Container
	if util.IsDriverPod(pod) {
		sidecars = app.Spec.Driver.Sidecars
	} else if util.IsExecutorPod(pod) {
		sidecars = app.Spec.Executor.Sidecars
	}

	var ops []patchOperation
	for _, c := range sidecars {
		sd := c
		if !hasContainer(pod, &sd) {
			ops = append(ops, patchOperation{Op: "add", Path: "/spec/containers/-", Value: &sd})
		}
	}
	return ops
}

func addInitContainers(pod *corev1.Pod, app *v1beta2.SparkApplication) []patchOperation {
	var initContainers []corev1.Container
	if util.IsDriverPod(pod) {
		initContainers = app.Spec.Driver.InitContainers
	} else if util.IsExecutorPod(pod) {
		initContainers = app.Spec.Executor.InitContainers
	}

	first := false
	if len(pod.Spec.InitContainers) == 0 {
		first = true
	}

	var ops []patchOperation
	for _, c := range initContainers {
		sd := c
		if first {
			first = false
			value := []corev1.Container{sd}
			ops = append(ops, patchOperation{Op: "add", Path: "/spec/initContainers", Value: value})
		} else if !hasInitContainer(pod, &sd) {
			ops = append(ops, patchOperation{Op: "add", Path: "/spec/initContainers/-", Value: &sd})
		}

	}
	return ops
}

func addGPU(pod *corev1.Pod, app *v1beta2.SparkApplication) *patchOperation {
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
		glog.V(2).Infof("Please specify GPU resource name, such as: nvidia.com/gpu, amd.com/gpu etc. Current gpu spec: %+v", gpu)
		return nil
	}
	if gpu.Quantity <= 0 {
		glog.V(2).Infof("GPU Quantity must be positive. Current gpu spec: %+v", gpu)
		return nil
	}

	i := findContainer(pod)
	if i < 0 {
		glog.Warningf("not able to add GPU as Spark container was not found in pod %s", pod.Name)
		return nil
	}

	path := fmt.Sprintf("/spec/containers/%d/resources/limits", i)
	var value interface{}
	if len(pod.Spec.Containers[i].Resources.Limits) == 0 {
		value = corev1.ResourceList{
			corev1.ResourceName(gpu.Name): *resource.NewQuantity(gpu.Quantity, resource.DecimalSI),
		}
	} else {
		encoder := strings.NewReplacer("~", "~0", "/", "~1")
		path += "/" + encoder.Replace(gpu.Name)
		value = *resource.NewQuantity(gpu.Quantity, resource.DecimalSI)
	}
	return &patchOperation{Op: "add", Path: path, Value: value}
}

func addHostNetwork(pod *corev1.Pod, app *v1beta2.SparkApplication) []patchOperation {
	var hostNetwork *bool
	if util.IsDriverPod(pod) {
		hostNetwork = app.Spec.Driver.HostNetwork
	}
	if util.IsExecutorPod(pod) {
		hostNetwork = app.Spec.Executor.HostNetwork
	}

	if hostNetwork == nil || *hostNetwork == false {
		return nil
	}
	var ops []patchOperation
	ops = append(ops, patchOperation{Op: "add", Path: "/spec/hostNetwork", Value: true})
	// For Pods with hostNetwork, explicitly set its DNS policy  to “ClusterFirstWithHostNet”
	// Detail: https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/#pod-s-dns-policy
	ops = append(ops, patchOperation{Op: "add", Path: "/spec/dnsPolicy", Value: corev1.DNSClusterFirstWithHostNet})
	return ops
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

func addTerminationGracePeriodSeconds(pod *corev1.Pod, app *v1beta2.SparkApplication) *patchOperation {
	path := "/spec/terminationGracePeriodSeconds"
	var gracePeriodSeconds *int64

	if util.IsDriverPod(pod) {
		gracePeriodSeconds = app.Spec.Driver.TerminationGracePeriodSeconds
	} else if util.IsExecutorPod(pod) {
		gracePeriodSeconds = app.Spec.Executor.TerminationGracePeriodSeconds
	}
	if gracePeriodSeconds == nil {
		return nil
	}
	return &patchOperation{Op: "add", Path: path, Value: *gracePeriodSeconds}
}

func addPodLifeCycleConfig(pod *corev1.Pod, app *v1beta2.SparkApplication) *patchOperation {
	var lifeCycle *corev1.Lifecycle
	if util.IsDriverPod(pod) {
		lifeCycle = app.Spec.Driver.Lifecycle
	}
	if lifeCycle == nil {
		return nil
	}

	i := 0
	// Find the driver container in the pod.
	for ; i < len(pod.Spec.Containers); i++ {
		if pod.Spec.Containers[i].Name == config.SparkDriverContainerName {
			break
		}
	}
	if i == len(pod.Spec.Containers) {
		glog.Warningf("Spark driver container not found in pod %s", pod.Name)
		return nil
	}

	path := fmt.Sprintf("/spec/containers/%d/lifecycle", i)
	return &patchOperation{Op: "add", Path: path, Value: *lifeCycle}
}

func findContainer(pod *corev1.Pod) int {
	var candidateContainerNames []string
	if util.IsDriverPod(pod) {
		candidateContainerNames = append(candidateContainerNames, config.SparkDriverContainerName)
	} else if util.IsExecutorPod(pod) {
		// Spark 3.x changed the default executor container name so we need to include both.
		candidateContainerNames = append(candidateContainerNames, config.SparkExecutorContainerName, config.Spark3DefaultExecutorContainerName)
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

func addHostAliases(pod *corev1.Pod, app *v1beta2.SparkApplication) []patchOperation {
	var hostAliases []corev1.HostAlias
	if util.IsDriverPod(pod) {
		hostAliases = app.Spec.Driver.HostAliases
	} else if util.IsExecutorPod(pod) {
		hostAliases = app.Spec.Executor.HostAliases
	}

	first := false
	if len(pod.Spec.HostAliases) == 0 {
		first = true
	}

	var ops []patchOperation
	if len(hostAliases) > 0 {
		if first {
			ops = append(ops, patchOperation{Op: "add", Path: "/spec/hostAliases", Value: hostAliases})
		} else {
			ops = append(ops, patchOperation{Op: "add", Path: "/spec/hostAliases/-", Value: hostAliases})
		}
	}
	return ops
}
