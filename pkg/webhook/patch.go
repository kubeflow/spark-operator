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

	"k8s.io/spark-on-k8s-operator/pkg/config"
	"k8s.io/spark-on-k8s-operator/pkg/util"
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

	op, err = addAffinity(pod)
	if err != nil {
		return nil, err
	}
	if op != nil {
		patchOps = append(patchOps, op)
	}

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
