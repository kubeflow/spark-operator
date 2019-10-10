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
	"context"
	"encoding/json"

	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"

	"github.com/stretchr/testify/assert"

	spov1beta2 "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	"k8s.io/api/admission/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func TestMutatePod(t *testing.T) {

	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-driver",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}
	// 1. Testing processing non-Spark pod.
	podBytes, err := serializePod(pod1)
	if err != nil {
		t.Error(err)
	}

	s := kscheme.Scheme
	app := spov1beta2.SparkApplication{}
	s.AddKnownTypes(spov1beta2.SchemeGroupVersion, &app)
	fakeClient := fake.NewFakeClientWithScheme(s)
	ctx := context.Background()

	spm := SparkPodMutator{
		client:       fakeClient,
		decoder:      nil,
		JobNameSpace: "",
	}

	// 1. Testing processing non-Spark pod.

	breq := v1beta1.AdmissionRequest{
		Resource: metav1.GroupVersionResource{
			Group:    corev1.SchemeGroupVersion.Group,
			Version:  corev1.SchemeGroupVersion.Version,
			Resource: "pods",
		},
		Object: runtime.RawExtension{
			Raw: podBytes,
		},
		Namespace: "default",
	}

	request := admission.Request{AdmissionRequest: breq}

	response := spm.mutatePods(pod1, request)
	assert.True(t, response.Allowed)

	// 2. Test processing Spark pod with only one patch: adding an OwnerReference.
	app1 := &spov1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-app1",
			Namespace: "default",
		},
	}

	err = fakeClient.Create(ctx, app1)
	if err != nil {
		t.Error(err)
	}

	pod1.Labels = map[string]string{
		config.SparkRoleLabel:               config.SparkDriverRole,
		config.LaunchedBySparkOperatorLabel: "true",
		config.SparkAppNameLabel:            app1.Name,
	}
	podBytes, err = serializePod(pod1)
	if err != nil {
		t.Error(err)
	}

	request.Object.Raw = podBytes
	response = spm.mutatePods(pod1, request)
	assert.True(t, response.Allowed)
	assert.Equal(t, v1beta1.PatchTypeJSONPatch, *response.PatchType)
	assert.True(t, len(response.Patches) > 0)

	// 3. Test processing Spark pod with patches.
	var user int64 = 1000
	app2 := &spov1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-app2",
			Namespace: "default",
		},
		Spec: spov1beta2.SparkApplicationSpec{
			Volumes: []corev1.Volume{
				{
					Name: "spark",
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: "/spark",
						},
					},
				},
				{
					Name: "unused", // Expect this to not be added to the driver.
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			},
			Driver: spov1beta2.DriverSpec{
				SparkPodSpec: spov1beta2.SparkPodSpec{
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "spark",
							MountPath: "/mnt/spark",
						},
					},
					Affinity: &corev1.Affinity{
						PodAffinity: &corev1.PodAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchLabels: map[string]string{config.SparkRoleLabel: config.SparkDriverRole},
									},
									TopologyKey: "kubernetes.io/hostname",
								},
							},
						},
					},
					Tolerations: []corev1.Toleration{
						{
							Key:      "Key",
							Operator: "Equal",
							Value:    "Value",
							Effect:   "NoEffect",
						},
					},
					SecurityContenxt: &corev1.PodSecurityContext{
						RunAsUser: &user,
					},
				},
			},
		},
	}

	err = fakeClient.Create(ctx, app2)
	if err != nil {
		t.Error(err)
	}

	pod1.Labels[config.SparkAppNameLabel] = app2.Name
	podBytes, err = serializePod(pod1)
	if err != nil {
		t.Error(err)
	}
	request.Object.Raw = podBytes
	response = spm.mutatePods(pod1, request)
	assert.True(t, response.Allowed)
	assert.Equal(t, v1beta1.PatchTypeJSONPatch, *response.PatchType)
	assert.True(t, len(response.Patches) > 0)
	assert.Equal(t, 6, len(response.Patches))
}

func serializePod(pod *corev1.Pod) ([]byte, error) {
	return json.Marshal(pod)
}

func testSelector(input string, expected *metav1.LabelSelector, t *testing.T) {
	selector, err := parseNamespaceSelector(input)
	if expected == nil {
		if err == nil {
			t.Errorf("Expected error parsing '%s', but got %v", input, selector)
		}
	} else {
		if err != nil {
			t.Errorf("Parsing '%s' failed: %v", input, err)
			return
		}
		if !equality.Semantic.DeepEqual(*selector, *expected) {
			t.Errorf("Parsing '%s' failed: expected %v, got %v", input, expected, selector)
		}
	}
}

func TestNamespaceSelectorParsing(t *testing.T) {
	testSelector("invalid", nil, t)
	testSelector("=invalid", nil, t)
	testSelector("invalid=", nil, t)
	testSelector("in,val,id", nil, t)
	testSelector(",inval=id,inval2=id2", nil, t)
	testSelector("inval=id,inval2=id2,", nil, t)
	testSelector("val=id,invalid", nil, t)
	testSelector("val=id", &metav1.LabelSelector{
		MatchLabels: map[string]string{
			"val": "id",
		},
	}, t)
	testSelector("val=id,val2=id2", &metav1.LabelSelector{
		MatchLabels: map[string]string{
			"val":  "id",
			"val2": "id2",
		},
	}, t)
}
