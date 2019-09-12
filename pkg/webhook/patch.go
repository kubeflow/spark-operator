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
	"github.com/golang/glog"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta1"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/util"
	corev1 "k8s.io/api/core/v1"
)

const (
	sparkDriverContainerName   = "spark-kubernetes-driver"
	sparkExecutorContainerName = "executor"
	maxNameLength              = 63
)

// patchOperation represents a RFC6902 JSON patch operation.
type patchOperation struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value,omitempty"`
}

func patchSparkPod(pod *corev1.Pod, app *v1beta1.SparkApplication) {
	if util.IsDriverPod(pod) {
		addOwnerReference(pod, app)
	}
	addVolumes(pod, app)
	addGeneralConfigMaps(pod, app)
	addSparkConfigMap(pod, app)
	addHadoopConfigMap(pod, app)
	addPrometheusConfigMap(pod, app)
	addTolerations(pod, app)
	addSidecarContainers(pod, app)
	addHostNetwork(pod, app)
	addNodeSelectors(pod, app)
	addDNSConfig(pod, app)
	addSchedulerName(pod, app)

	if pod.Spec.Affinity == nil {
		addAffinity(pod, app)
	}
	addSecurityContext(pod, app)
	addGPU(pod, app)
}

func addOwnerReference(pod *corev1.Pod, app *v1beta1.SparkApplication) {
	ownerReference := util.GetOwnerReference(app)
	pod.OwnerReferences = append(pod.OwnerReferences, ownerReference)
}

func addVolumes(pod *corev1.Pod, app *v1beta1.SparkApplication) {
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

	for _, m := range volumeMounts {
		if v, ok := volumeMap[m.Name]; ok {
			addVolume(pod, v)
			addVolumeMount(pod, m)
		}
	}
}

func addVolume(pod *corev1.Pod, volume corev1.Volume) {
	pod.Spec.Volumes = append(pod.Spec.Volumes, volume)
}

func addVolumeMount(pod *corev1.Pod, mount corev1.VolumeMount) {
	i := 0
	// Find the driver or executor container in the pod.
	for ; i < len(pod.Spec.Containers); i++ {
		if pod.Spec.Containers[i].Name == sparkDriverContainerName ||
			pod.Spec.Containers[i].Name == sparkExecutorContainerName {
			break
		}
	}
	pod.Spec.Containers[i].VolumeMounts = append(pod.Spec.Containers[i].VolumeMounts, mount)
}

func addEnvironmentVariable(pod *corev1.Pod, envName, envValue string) {
	i := 0
	// Find the driver or executor container in the pod.
	for ; i < len(pod.Spec.Containers); i++ {
		if pod.Spec.Containers[i].Name == sparkDriverContainerName ||
			pod.Spec.Containers[i].Name == sparkExecutorContainerName {
			break
		}
	}
	value := corev1.EnvVar{Name: envName, Value: envValue}
	pod.Spec.Containers[i].Env = append(pod.Spec.Containers[i].Env, value)
}

func addSparkConfigMap(pod *corev1.Pod, app *v1beta1.SparkApplication) {
	sparkConfigMapName := app.Spec.SparkConfigMap
	if sparkConfigMapName != nil {
		addConfigMapVolume(pod, *sparkConfigMapName, config.SparkConfigMapVolumeName)
		addConfigMapVolumeMount(pod, config.SparkConfigMapVolumeName, config.DefaultSparkConfDir)
		addEnvironmentVariable(pod, config.SparkConfDirEnvVar, config.DefaultSparkConfDir)
	}
}

func addHadoopConfigMap(pod *corev1.Pod, app *v1beta1.SparkApplication) {
	hadoopConfigMapName := app.Spec.HadoopConfigMap
	if hadoopConfigMapName != nil {
		addConfigMapVolume(pod, *hadoopConfigMapName, config.HadoopConfigMapVolumeName)
		addConfigMapVolumeMount(pod, config.HadoopConfigMapVolumeName, config.DefaultHadoopConfDir)
		addEnvironmentVariable(pod, config.HadoopConfDirEnvVar, config.DefaultHadoopConfDir)
	}
}

func addGeneralConfigMaps(pod *corev1.Pod, app *v1beta1.SparkApplication) {
	var configMaps []v1beta1.NamePath
	if util.IsDriverPod(pod) {
		configMaps = app.Spec.Driver.ConfigMaps
	} else if util.IsExecutorPod(pod) {
		configMaps = app.Spec.Executor.ConfigMaps
	}

	for _, namePath := range configMaps {
		volumeName := namePath.Name + "-vol"
		if len(volumeName) > maxNameLength {
			volumeName = volumeName[0:maxNameLength]
			glog.V(2).Infof("ConfigMap volume name is too long. Truncating to length %d. Result: %s.", maxNameLength, volumeName)
		}
		addConfigMapVolume(pod, namePath.Name, volumeName)
		addConfigMapVolumeMount(pod, volumeName, namePath.Path)
	}
}

func addPrometheusConfigMap(pod *corev1.Pod, app *v1beta1.SparkApplication) {
	// Skip if Prometheus Monitoring is not enabled or an in-container ConfigFile is used,
	// in which cases a Prometheus ConfigMap won't be created.
	if !app.PrometheusMonitoringEnabled() || app.HasPrometheusConfigFile() {
		return
	}

	if util.IsDriverPod(pod) && !app.ExposeDriverMetrics() {
		return
	}
	if util.IsExecutorPod(pod) && !app.ExposeExecutorMetrics() {
		return
	}

	name := config.GetPrometheusConfigMapName(app)
	volumeName := name + "-vol"
	mountPath := config.PrometheusConfigMapMountPath
	addConfigMapVolume(pod, name, volumeName)
	addConfigMapVolumeMount(pod, volumeName, mountPath)
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

func addConfigMapVolumeMount(pod *corev1.Pod, configMapVolumeName string, mountPath string) {
	mount := corev1.VolumeMount{
		Name:      configMapVolumeName,
		ReadOnly:  true,
		MountPath: mountPath,
	}
	addVolumeMount(pod, mount)
}

func addAffinity(pod *corev1.Pod, app *v1beta1.SparkApplication) {
	var affinity *corev1.Affinity
	if util.IsDriverPod(pod) {
		affinity = app.Spec.Driver.Affinity
	} else if util.IsExecutorPod(pod) {
		affinity = app.Spec.Executor.Affinity
	}

	if affinity == nil {
		return
	}
	pod.Spec.Affinity = affinity
}

func addTolerations(pod *corev1.Pod, app *v1beta1.SparkApplication) {
	var tolerations []corev1.Toleration
	if util.IsDriverPod(pod) {
		tolerations = app.Spec.Driver.Tolerations
	} else if util.IsExecutorPod(pod) {
		tolerations = app.Spec.Executor.Tolerations
	}

	for _, v := range tolerations {
		addToleration(pod, v)
	}
}

func addNodeSelectors(pod *corev1.Pod, app *v1beta1.SparkApplication) {
	var nodeSelector map[string]string
	if util.IsDriverPod(pod) {
		nodeSelector = app.Spec.Driver.NodeSelector
	} else if util.IsExecutorPod(pod) {
		nodeSelector = app.Spec.Executor.NodeSelector
	}

	if len(nodeSelector) > 0 {
		pod.Spec.NodeSelector = nodeSelector
	}
}

func addDNSConfig(pod *corev1.Pod, app *v1beta1.SparkApplication) {
	var dnsConfig *corev1.PodDNSConfig

	if util.IsDriverPod(pod) {
		dnsConfig = app.Spec.Driver.DNSConfig
	} else if util.IsExecutorPod(pod) {
		dnsConfig = app.Spec.Executor.DNSConfig
	}

	if dnsConfig != nil {
		pod.Spec.DNSConfig = dnsConfig
	}
}

func addSchedulerName(pod *corev1.Pod, app *v1beta1.SparkApplication) {
	var schedulerName *string
	if util.IsDriverPod(pod) {
		schedulerName = app.Spec.Driver.SchedulerName
	}
	if util.IsExecutorPod(pod) {
		schedulerName = app.Spec.Executor.SchedulerName
	}
	if schedulerName == nil || *schedulerName == "" {
		return
	}
	pod.Spec.SchedulerName = *schedulerName
}

func addToleration(pod *corev1.Pod, toleration corev1.Toleration) {
	pod.Spec.Tolerations = append(pod.Spec.Tolerations, toleration)
}

func addSecurityContext(pod *corev1.Pod, app *v1beta1.SparkApplication) {
	var secContext *corev1.PodSecurityContext
	if util.IsDriverPod(pod) {
		secContext = app.Spec.Driver.SecurityContenxt
	} else if util.IsExecutorPod(pod) {
		secContext = app.Spec.Executor.SecurityContenxt
	}

	if secContext == nil {
		return
	}
	pod.Spec.SecurityContext = secContext
}

func addSidecarContainers(pod *corev1.Pod, app *v1beta1.SparkApplication) {
	var sidecars []corev1.Container
	if util.IsDriverPod(pod) {
		sidecars = app.Spec.Driver.Sidecars
	} else if util.IsExecutorPod(pod) {
		sidecars = app.Spec.Executor.Sidecars
	}
	for _, c := range sidecars {
		sd := c
		if !hasContainer(pod, &sd) {
			pod.Spec.Containers = append(pod.Spec.Containers, sd)
		}
	}
}

func addGPU(pod *corev1.Pod, app *v1beta1.SparkApplication) {
	var gpu *v1beta1.GPUSpec
	if util.IsDriverPod(pod) {
		gpu = app.Spec.Driver.GPU
	}
	if util.IsExecutorPod(pod) {
		gpu = app.Spec.Executor.GPU
	}
	if gpu == nil {
		return
	}
	if gpu.Name == "" {
		glog.V(2).Infof("Please specify GPU resource name, such as: nvidia.com/gpu, amd.com/gpu etc. Current gpu spec: %+v", gpu)
		return
	}
	if gpu.Quantity <= 0 {
		glog.V(2).Infof("GPU Quantity must be positive. Current gpu spec: %+v", gpu)
		return
	}
	i := 0
	// Find the driver or executor container in the pod.
	for ; i < len(pod.Spec.Containers); i++ {
		if pod.Spec.Containers[i].Name == sparkDriverContainerName ||
			pod.Spec.Containers[i].Name == sparkExecutorContainerName {
			break
		}
	}
	var value corev1.ResourceList = corev1.ResourceList{
		corev1.ResourceName(gpu.Name): *resource.NewQuantity(gpu.Quantity, resource.DecimalSI),
	}

	if len(pod.Spec.Containers[i].Resources.Limits) != 0 {
		for k, v := range value {
			pod.Spec.Containers[i].Resources.Limits[k] = v
		}
	} else {
		pod.Spec.Containers[i].Resources.Limits = value
	}
}

func addHostNetwork(pod *corev1.Pod, app *v1beta1.SparkApplication) {
	var hostNetwork *bool
	if util.IsDriverPod(pod) {
		hostNetwork = app.Spec.Driver.HostNetwork
	}
	if util.IsExecutorPod(pod) {
		hostNetwork = app.Spec.Executor.HostNetwork
	}

	if hostNetwork == nil || *hostNetwork == false {
		return
	}
	pod.Spec.HostNetwork = true
	// For Pods with hostNetwork, explicitly set its DNS policy  to “ClusterFirstWithHostNet”
	// Detail: https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/#pod-s-dns-policy
	pod.Spec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
}

func hasContainer(pod *corev1.Pod, container *corev1.Container) bool {
	for _, c := range pod.Spec.Containers {
		if container.Name == c.Name && container.Image == c.Image {
			return true
		}
	}
	return false
}
