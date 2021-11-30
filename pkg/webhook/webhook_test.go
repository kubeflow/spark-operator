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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	spov1beta2 "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	crdclientfake "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned/fake"
	crdinformers "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/informers/externalversions"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
)

func TestMutatePod(t *testing.T) {
	crdClient := crdclientfake.NewSimpleClientset()
	informerFactory := crdinformers.NewSharedInformerFactory(crdClient, 0*time.Second)
	informer := informerFactory.Sparkoperator().V1beta2().SparkApplications()
	lister := informer.Lister()

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
	review := &admissionv1.AdmissionReview{
		Request: &admissionv1.AdmissionRequest{
			Resource: metav1.GroupVersionResource{
				Group:    corev1.SchemeGroupVersion.Group,
				Version:  corev1.SchemeGroupVersion.Version,
				Resource: "pods",
			},
			Object: runtime.RawExtension{
				Raw: podBytes,
			},
			Namespace: "default",
		},
	}
	response, _ := mutatePods(review, lister, "default")
	assert.True(t, response.Allowed)

	// 2. Test processing Spark pod with only one patch: adding an OwnerReference.
	app1 := &spov1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-app1",
			Namespace: "default",
		},
	}
	crdClient.SparkoperatorV1beta2().SparkApplications(app1.Namespace).Create(context.TODO(), app1, metav1.CreateOptions{})
	informer.Informer().GetIndexer().Add(app1)
	pod1.Labels = map[string]string{
		config.SparkRoleLabel:               config.SparkDriverRole,
		config.LaunchedBySparkOperatorLabel: "true",
		config.SparkAppNameLabel:            app1.Name,
	}
	podBytes, err = serializePod(pod1)
	if err != nil {
		t.Error(err)
	}
	review.Request.Object.Raw = podBytes
	response, _ = mutatePods(review, lister, "default")
	assert.True(t, response.Allowed)
	assert.Equal(t, admissionv1.PatchTypeJSONPatch, *response.PatchType)
	assert.True(t, len(response.Patch) > 0)

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
					SecurityContext: &corev1.SecurityContext{
						RunAsUser: &user,
					},
				},
			},
		},
	}
	crdClient.SparkoperatorV1beta2().SparkApplications(app2.Namespace).Update(context.TODO(), app2, metav1.UpdateOptions{})
	informer.Informer().GetIndexer().Add(app2)

	pod1.Labels[config.SparkAppNameLabel] = app2.Name
	podBytes, err = serializePod(pod1)
	if err != nil {
		t.Error(err)
	}
	review.Request.Object.Raw = podBytes
	response, _ = mutatePods(review, lister, "default")
	assert.True(t, response.Allowed)
	assert.Equal(t, admissionv1.PatchTypeJSONPatch, *response.PatchType)
	assert.True(t, len(response.Patch) > 0)
	var patchOps []*patchOperation
	json.Unmarshal(response.Patch, &patchOps)
	assert.Equal(t, 6, len(patchOps))
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
