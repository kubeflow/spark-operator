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

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/util"
)

func TestPatchSparkPod_OwnerReference(t *testing.T) {
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

	// Test patching a pod without existing OwnerReference and Volume.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "spark-driver",
			Labels:      map[string]string{sparkRoleLabel: sparkDriverRole},
			Annotations: map[string]string{config.OwnerReferenceAnnotation: referenceStr},
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

	modifiedPod, err := getModifiedPod(pod)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 1, len(modifiedPod.OwnerReferences))
	assert.Equal(t, ownerReference, modifiedPod.OwnerReferences[0])

	// Test patching a pod with existing OwnerReference and Volume.
	pod.OwnerReferences = append(pod.OwnerReferences, metav1.OwnerReference{Name: "owner-reference1"})

	modifiedPod, err = getModifiedPod(pod)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 2, len(modifiedPod.OwnerReferences))
	assert.Equal(t, ownerReference, modifiedPod.OwnerReferences[1])
}

func TestPatchSparkPod_Volumes(t *testing.T) {
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
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "spark-driver",
			Labels: map[string]string{sparkRoleLabel: sparkDriverRole},
			Annotations: map[string]string{
				volumeAnnotation:      volumeStr,
				volumeMountAnnotation: volumeMountStr,
			},
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

	modifiedPod, err := getModifiedPod(pod)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 1, len(modifiedPod.Spec.Volumes))
	assert.Equal(t, *volume, modifiedPod.Spec.Volumes[0])
	assert.Equal(t, 1, len(modifiedPod.Spec.Containers[0].VolumeMounts))
	assert.Equal(t, *volumeMount, modifiedPod.Spec.Containers[0].VolumeMounts[0])

	// Test patching a pod with existing OwnerReference and Volume.
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: "volume1"})
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name: "volume1",
	})

	modifiedPod, err = getModifiedPod(pod)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 2, len(modifiedPod.Spec.Volumes))
	assert.Equal(t, *volume, modifiedPod.Spec.Volumes[1])
	assert.Equal(t, 2, len(modifiedPod.Spec.Containers[0].VolumeMounts))
	assert.Equal(t, *volumeMount, modifiedPod.Spec.Containers[0].VolumeMounts[1])
}

func TestPatchSparkPod_Affinity(t *testing.T) {
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

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "spark-driver",
			Labels:      map[string]string{sparkRoleLabel: sparkDriverRole},
			Annotations: map[string]string{config.AffinityAnnotation: affinityStr},
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

	modifiedPod, err := getModifiedPod(pod)
	if err != nil {
		t.Fatal(err)
	}

	assert.True(t, modifiedPod.Spec.Affinity != nil)
	assert.Equal(t, 1,
		len(modifiedPod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution))
	assert.Equal(t, "kubernetes.io/hostname",
		modifiedPod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].TopologyKey)
}

func TestPatchSparkPod_ConfigMaps(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "spark-driver",
			Labels:      map[string]string{sparkRoleLabel: sparkDriverRole},
			Annotations: map[string]string{config.GeneralConfigMapsAnnotationPrefix + "foo": "/path/to/foo"},
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

	modifiedPod, err := getModifiedPod(pod)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 1, len(modifiedPod.Spec.Volumes))
	assert.Equal(t, "foo-volume", modifiedPod.Spec.Volumes[0].Name)
	assert.True(t, modifiedPod.Spec.Volumes[0].ConfigMap != nil)
	assert.Equal(t, 1, len(modifiedPod.Spec.Containers[0].VolumeMounts))
	assert.Equal(t, "/path/to/foo", modifiedPod.Spec.Containers[0].VolumeMounts[0].MountPath)
}

func TestPatchSparkPod_SparkConfigMap(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "spark-driver",
			Labels:      map[string]string{sparkRoleLabel: sparkDriverRole},
			Annotations: map[string]string{config.SparkConfigMapAnnotation: "spark-conf"},
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

	modifiedPod, err := getModifiedPod(pod)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 1, len(modifiedPod.Spec.Volumes))
	assert.Equal(t, config.SparkConfigMapVolumeName, modifiedPod.Spec.Volumes[0].Name)
	assert.True(t, modifiedPod.Spec.Volumes[0].ConfigMap != nil)
	assert.Equal(t, 1, len(modifiedPod.Spec.Containers[0].VolumeMounts))
	assert.Equal(t, config.DefaultSparkConfDir, modifiedPod.Spec.Containers[0].VolumeMounts[0].MountPath)
	assert.Equal(t, 1, len(modifiedPod.Spec.Containers[0].Env))
	assert.Equal(t, config.DefaultSparkConfDir, modifiedPod.Spec.Containers[0].Env[0].Value)
}

func TestPatchSparkPod_HadoopConfigMap(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "spark-driver",
			Labels:      map[string]string{sparkRoleLabel: sparkDriverRole},
			Annotations: map[string]string{config.HadoopConfigMapAnnotation: "hadoop-conf"},
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

	modifiedPod, err := getModifiedPod(pod)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 1, len(modifiedPod.Spec.Volumes))
	assert.Equal(t, config.HadoopConfigMapVolumeName, modifiedPod.Spec.Volumes[0].Name)
	assert.True(t, modifiedPod.Spec.Volumes[0].ConfigMap != nil)
	assert.Equal(t, 1, len(modifiedPod.Spec.Containers[0].VolumeMounts))
	assert.Equal(t, config.DefaultHadoopConfDir, modifiedPod.Spec.Containers[0].VolumeMounts[0].MountPath)
	assert.Equal(t, 1, len(modifiedPod.Spec.Containers[0].Env))
	assert.Equal(t, config.DefaultHadoopConfDir, modifiedPod.Spec.Containers[0].Env[0].Value)
}

func getModifiedPod(pod *corev1.Pod) (*corev1.Pod, error) {
	patchOps, err := patchSparkPod(pod)
	if err != nil {
		return nil, err
	}
	patchBytes, err := json.Marshal(patchOps)
	if err != nil {
		return nil, err
	}
	patch, err := jsonpatch.DecodePatch(patchBytes)
	if err != nil {
		return nil, err
	}

	original, err := json.Marshal(pod)
	if err != nil {
		return nil, err
	}
	modified, err := patch.Apply(original)
	if err != nil {
		return nil, err
	}
	modifiedPod := &corev1.Pod{}
	if err := json.Unmarshal(modified, modifiedPod); err != nil {
		return nil, err
	}

	return modifiedPod, nil
}

func TestPatchSparkPod_Tolerations(t *testing.T) {
	toleration := &corev1.Toleration{
		Key:      "Key",
		Operator: "Equal",
		Value:    "Value",
		Effect:   "NoEffect",
	}

	tolerationStr, err := util.MarshalToleration(toleration)
	if err != nil {
		t.Error(err)
	}

	tolerationAnnotation := fmt.Sprintf("%s%s", config.TolerationsAnnotationPrefix, "toleration1")

	// Test patching a pod with a Toleration.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "spark-driver",
			Labels:      map[string]string{sparkRoleLabel: sparkDriverRole},
			Annotations: map[string]string{tolerationAnnotation: tolerationStr},
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

	modifiedPod, err := getModifiedPod(pod)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 1, len(modifiedPod.Spec.Tolerations))
	assert.Equal(t, *toleration, modifiedPod.Spec.Tolerations[0])
}
