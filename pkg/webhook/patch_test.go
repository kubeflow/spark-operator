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
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/evanphx/json-patch"
	"github.com/stretchr/testify/assert"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	"k8s.io/spark-on-k8s-operator/pkg/config"
	"k8s.io/spark-on-k8s-operator/pkg/util"
)

func TestPatchSparkPod(t *testing.T) {
	ownerReference := metav1.OwnerReference{
		APIVersion: v1alpha1.SchemeGroupVersion.String(),
		Kind:       reflect.TypeOf(v1alpha1.SparkApplication{}).Name(),
		Name:       "spark-test",
		UID:        "spark-test-1",
	}
	referenceStr, err := util.MarshalOwnerReference(&ownerReference)
	if err != nil {
		t.Error(err)
	}

	volume := &corev1.Volume{
		Name: "spark",
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: "/spark",
			},
		},
	}
	volumeStr, err := util.MarshalVolume(volume)
	if err != nil {
		t.Error(err)
	}
	volumeMount := &corev1.VolumeMount{
		Name:      "spark",
		MountPath: "/mnt/spark",
	}
	volumeMountStr, err := util.MarshalVolumeMount(volumeMount)
	if err != nil {
		t.Error(err)
	}

	volumeAnnotation := fmt.Sprintf("%s%s", config.VolumesAnnotationPrefix, volume.Name)
	volumeMountAnnotation := fmt.Sprintf("%s%s", config.VolumeMountsAnnotationPrefix, volumeMount.Name)
	// Test patching a pod without existing OwnerReference and Volume.
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "spark-driver",
			Labels: map[string]string{sparkRoleLabel: sparkDriverRole},
			Annotations: map[string]string{
				config.OwnerReferenceAnnotation: referenceStr,
				volumeAnnotation:                volumeStr,
				volumeMountAnnotation:           volumeMountStr},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  sparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	patchOps, err := patchSparkPod(pod1)
	if err != nil {
		t.Error(err)
	}
	patchBytes, err := json.Marshal(patchOps)
	if err != nil {
		t.Error(err)
	}
	patch, err := jsonpatch.DecodePatch(patchBytes)
	if err != nil {
		t.Error(err)
	}

	original, err := json.Marshal(pod1)
	if err != nil {
		t.Error(err)
	}
	modified, err := patch.Apply(original)
	if err != nil {
		t.Error(err)
	}
	modifiedPod := &corev1.Pod{}
	if err := json.Unmarshal(modified, modifiedPod); err != nil {
		t.Error(err)
	}

	assert.Equal(t, 1, len(modifiedPod.OwnerReferences))
	assert.Equal(t, ownerReference, modifiedPod.OwnerReferences[0])
	assert.Equal(t, 1, len(modifiedPod.Spec.Volumes))
	assert.Equal(t, *volume, modifiedPod.Spec.Volumes[0])
	assert.Equal(t, 1, len(modifiedPod.Spec.Containers[0].VolumeMounts))
	assert.Equal(t, *volumeMount, modifiedPod.Spec.Containers[0].VolumeMounts[0])

	// Test patching a pod with existing OwnerReference and Volume.
	pod2 := pod1.DeepCopy()
	pod2.OwnerReferences = append(pod2.OwnerReferences, metav1.OwnerReference{Name: "owner-reference1"})
	pod2.Spec.Volumes = append(pod2.Spec.Volumes, corev1.Volume{Name: "volume1"})
	pod2.Spec.Containers[0].VolumeMounts = append(pod2.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name: "volume1",
	})

	patchOps, err = patchSparkPod(pod2)
	if err != nil {
		t.Error(err)
	}
	patchBytes, err = json.Marshal(patchOps)
	if err != nil {
		t.Error(err)
	}
	patch, err = jsonpatch.DecodePatch(patchBytes)
	if err != nil {
		t.Error(err)
	}

	original, err = json.Marshal(pod2)
	if err != nil {
		t.Error(err)
	}
	modified, err = patch.Apply(original)
	if err != nil {
		t.Error(err)
	}
	modifiedPod = &corev1.Pod{}
	if err := json.Unmarshal(modified, modifiedPod); err != nil {
		t.Error(err)
	}

	assert.Equal(t, 2, len(modifiedPod.OwnerReferences))
	assert.Equal(t, ownerReference, modifiedPod.OwnerReferences[1])
	assert.Equal(t, 2, len(modifiedPod.Spec.Volumes))
	assert.Equal(t, *volume, modifiedPod.Spec.Volumes[1])
	assert.Equal(t, 2, len(modifiedPod.Spec.Containers[0].VolumeMounts))
	assert.Equal(t, *volumeMount, modifiedPod.Spec.Containers[0].VolumeMounts[1])

	// Test patching a pod with a pod Affinity.
	affinity := &corev1.Affinity{
		PodAffinity: &corev1.PodAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
				{
					LabelSelector: &metav1.LabelSelector{
						MatchLabels: map[string]string{sparkRoleLabel: sparkDriverRole},
					},
					TopologyKey: "kubernetes.io/hostname",
				},
			},
		},
	}
	affinityStr, err := util.MarshalAffinity(affinity)
	if err != nil {
		t.Error(err)
	}
	pod3 := pod1.DeepCopy()
	pod3.Annotations[config.AffinityAnnotation] = affinityStr

	patchOps, err = patchSparkPod(pod3)
	if err != nil {
		t.Error(err)
	}
	patchBytes, err = json.Marshal(patchOps)
	if err != nil {
		t.Error(err)
	}
	patch, err = jsonpatch.DecodePatch(patchBytes)
	if err != nil {
		t.Error(err)
	}

	original, err = json.Marshal(pod3)
	if err != nil {
		t.Error(err)
	}
	modified, err = patch.Apply(original)
	if err != nil {
		t.Error(err)
	}
	modifiedPod = &corev1.Pod{}
	if err := json.Unmarshal(modified, modifiedPod); err != nil {
		t.Error(err)
	}

	assert.True(t, modifiedPod.Spec.Affinity != nil)
	assert.Equal(t, 1,
		len(modifiedPod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution))
	assert.Equal(t, "kubernetes.io/hostname",
		modifiedPod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].TopologyKey)
}
