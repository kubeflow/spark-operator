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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/util"
)

const (
	sparkDriverContainerName   = "spark-kubernetes-driver"
	sparkExecutorContainerName = "executor"
)

// patchOperation represents a RFC6902 JSON patch operation.
type patchOperation struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value,omitempty"`
}

func patchSparkPod(pod *corev1.Pod) ([]*patchOperation, error) {
	var patchOps []*patchOperation

	op, err := addOwnerReference(pod)
	if err != nil {
		return nil, err
	}
	if op != nil {
		patchOps = append(patchOps, op)
	}

	ops, err := addVolumes(pod)
	if err != nil {
		return nil, err
	}
	patchOps = append(patchOps, ops...)

	patchOps = append(patchOps, addGeneralConfigMaps(pod)...)
	patchOps = append(patchOps, addSparkConfigMap(pod)...)
	patchOps = append(patchOps, addHadoopConfigMap(pod)...)

	op, err = addAffinity(pod)
	if err != nil {
		return nil, err
	}
	if op != nil {
		patchOps = append(patchOps, op)
	}

	ops, err = addTolerations(pod)
	if err != nil {
		return nil, err
	}
	patchOps = append(patchOps, ops...)

	return patchOps, nil
}

func addOwnerReference(pod *corev1.Pod) (*patchOperation, error) {
	ownerReferenceStr, ok := pod.Annotations[config.OwnerReferenceAnnotation]
	if !ok {
		return nil, nil
	}

	ownerReference, err := util.UnmarshalOwnerReference(ownerReferenceStr)
	if err != nil {
		return nil, err
	}

	path := "/metadata/ownerReferences"
	var value interface{}
	if len(pod.OwnerReferences) == 0 {
		value = []metav1.OwnerReference{*ownerReference}
	} else {
		path += "/-"
		value = *ownerReference
	}

	return &patchOperation{Op: "add", Path: path, Value: value}, nil
}

func addVolumes(pod *corev1.Pod) ([]*patchOperation, error) {
	volumes, err := config.FindVolumes(pod.Annotations)
	if err != nil {
		return nil, err
	}
	volumeMounts, err := config.FindVolumeMounts(pod.Annotations)
	if err != nil {
		return nil, err
	}

	var ops []*patchOperation
	for name := range volumeMounts {
		if volume, ok := volumes[name]; ok {
			ops = append(ops, addVolume(pod, volume))
			ops = append(ops, addVolumeMount(pod, volumeMounts[name]))
		}
	}

	return ops, nil
}

func addVolume(pod *corev1.Pod, volume *corev1.Volume) *patchOperation {
	path := "/spec/volumes"
	var value interface{}
	if len(pod.Spec.Volumes) == 0 {
		value = []corev1.Volume{*volume}
	} else {
		path += "/-"
		value = *volume
	}

	return &patchOperation{Op: "add", Path: path, Value: value}
}

func addVolumeMount(pod *corev1.Pod, mount *corev1.VolumeMount) *patchOperation {
	i := 0
	// Find the driver or executor container in the pod.
	for ; i < len(pod.Spec.Containers); i++ {
		if pod.Spec.Containers[i].Name == sparkDriverContainerName ||
			pod.Spec.Containers[i].Name == sparkExecutorContainerName {
			break
		}
	}

	path := fmt.Sprintf("/spec/containers/%d/volumeMounts", i)
	var value interface{}
	if len(pod.Spec.Containers[i].VolumeMounts) == 0 {
		value = []corev1.VolumeMount{*mount}
	} else {
		path += "/-"
		value = *mount
	}

	return &patchOperation{Op: "add", Path: path, Value: value}
}

func addEnvironmentVariable(pod *corev1.Pod, envName, envValue string) *patchOperation {
	i := 0
	// Find the driver or executor container in the pod.
	for ; i < len(pod.Spec.Containers); i++ {
		if pod.Spec.Containers[i].Name == sparkDriverContainerName ||
			pod.Spec.Containers[i].Name == sparkExecutorContainerName {
			break
		}
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

func addSparkConfigMap(pod *corev1.Pod) []*patchOperation {
	var patchOps []*patchOperation
	sparkConfigMapName, ok := pod.Annotations[config.SparkConfigMapAnnotation]
	if ok {
		patchOps = append(patchOps, addConfigMapVolume(pod, sparkConfigMapName, config.SparkConfigMapVolumeName))
		patchOps = append(patchOps, addConfigMapVolumeMount(pod, config.SparkConfigMapVolumeName,
			config.DefaultSparkConfDir))
		patchOps = append(patchOps, addEnvironmentVariable(pod, config.SparkConfDirEnvVar, config.DefaultSparkConfDir))
	}
	return patchOps
}

func addHadoopConfigMap(pod *corev1.Pod) []*patchOperation {
	var patchOps []*patchOperation
	hadoopConfigMapName, ok := pod.Annotations[config.HadoopConfigMapAnnotation]
	if ok {
		patchOps = append(patchOps, addConfigMapVolume(pod, hadoopConfigMapName, config.HadoopConfigMapVolumeName))
		patchOps = append(patchOps, addConfigMapVolumeMount(pod, config.HadoopConfigMapVolumeName,
			config.DefaultHadoopConfDir))
		patchOps = append(patchOps, addEnvironmentVariable(pod, config.HadoopConfDirEnvVar, config.DefaultHadoopConfDir))
	}
	return patchOps
}

func addGeneralConfigMaps(pod *corev1.Pod) []*patchOperation {
	var patchOps []*patchOperation
	namesToMountPaths := config.FindGeneralConfigMaps(pod.Annotations)
	for name, mountPath := range namesToMountPaths {
		volumeName := name + "-volume"
		patchOps = append(patchOps, addConfigMapVolume(pod, name, volumeName))
		patchOps = append(patchOps, addConfigMapVolumeMount(pod, volumeName, mountPath))
	}
	return patchOps
}

func addConfigMapVolume(pod *corev1.Pod, configMapName string, configMapVolumeName string) *patchOperation {
	volume := &corev1.Volume{
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
	mount := &corev1.VolumeMount{
		Name:      configMapVolumeName,
		ReadOnly:  true,
		MountPath: mountPath,
	}
	return addVolumeMount(pod, mount)
}

func addAffinity(pod *corev1.Pod) (*patchOperation, error) {
	affinityStr, ok := pod.Annotations[config.AffinityAnnotation]
	if !ok {
		return nil, nil
	}

	affinity, err := util.UnmarshalAffinity(affinityStr)
	if err != nil {
		return nil, err
	}

	return &patchOperation{Op: "add", Path: "/spec/affinity", Value: *affinity}, nil
}

func addTolerations(pod *corev1.Pod) ([]*patchOperation, error) {
	tolerations, err := config.FindTolerations(pod.Annotations)
	if err != nil {
		return nil, err
	}
	var ops []*patchOperation
	for _, v := range tolerations {
		ops = append(ops, addToleration(pod, v))
	}
	return ops, nil
}

func addToleration(pod *corev1.Pod, toleration *corev1.Toleration) *patchOperation {
	path := "/spec/tolerations"
	var value interface{}
	if len(pod.Spec.Tolerations) == 0 {
		value = []corev1.Toleration{*toleration}
	} else {
		path += "/-"
		value = *toleration
	}

	return &patchOperation{Op: "add", Path: path, Value: value}
}
