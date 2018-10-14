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

	"github.com/stretchr/testify/assert"

	"k8s.io/api/admission/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/util"
)

func TestMutatePod(t *testing.T) {
	// Testing processing non-Spark pod.
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-driver",
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

	podBytes, err := serializePod(pod1)
	if err != nil {
		t.Error(err)
	}
	review := &v1beta1.AdmissionReview{
		Request: &v1beta1.AdmissionRequest{
			Resource: metav1.GroupVersionResource{
				Group:    corev1.SchemeGroupVersion.Group,
				Version:  corev1.SchemeGroupVersion.Version,
				Resource: "pods",
			},
			Object: runtime.RawExtension{
				Raw: podBytes,
			},
		},
	}
	response := mutatePods(review)
	assert.True(t, response.Allowed)

	// Test processing Spark pod without any patch.
	pod1.Labels = map[string]string{
		sparkRoleLabel:                      sparkDriverRole,
		config.LaunchedBySparkOperatorLabel: "true",
	}

	podBytes, err = serializePod(pod1)
	if err != nil {
		t.Error(err)
	}
	review.Request.Object.Raw = podBytes
	assert.True(t, response.Allowed)
	assert.True(t, response.PatchType == nil)
	assert.True(t, response.Patch == nil)

	// Test processing Spark pod with patches.
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

	volumeAnnotation := fmt.Sprintf("%s%s", config.VolumesAnnotationPrefix, volume.Name)
	volumeMountAnnotation := fmt.Sprintf("%s%s", config.VolumeMountsAnnotationPrefix, volumeMount.Name)
	tolerationAnnotation := fmt.Sprintf("%s%s", config.TolerationsAnnotationPrefix, "toleration1")

	pod1.Annotations = map[string]string{
		config.OwnerReferenceAnnotation:                  referenceStr,
		volumeAnnotation:                                 volumeStr,
		volumeMountAnnotation:                            volumeMountStr,
		config.AffinityAnnotation:                        affinityStr,
		config.GeneralConfigMapsAnnotationPrefix + "foo": "/path/to/foo",
		config.SparkConfigMapAnnotation:                  "spark-conf",
		config.HadoopConfigMapAnnotation:                 "hadoop-conf",
		tolerationAnnotation:                             tolerationStr,
	}

	podBytes, err = serializePod(pod1)
	if err != nil {
		t.Error(err)
	}
	review.Request.Object.Raw = podBytes
	response = mutatePods(review)
	assert.True(t, response.Allowed)
	assert.Equal(t, v1beta1.PatchTypeJSONPatch, *response.PatchType)
	assert.True(t, len(response.Patch) > 0)
	var patchOps []*patchOperation
	json.Unmarshal(response.Patch, &patchOps)
	assert.Equal(t, 13, len(patchOps))
}

func serializePod(pod *corev1.Pod) ([]byte, error) {
	return json.Marshal(pod)
}
