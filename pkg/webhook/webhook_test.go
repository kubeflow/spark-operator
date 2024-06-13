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
	arv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	gotest "k8s.io/client-go/testing"

	spov1beta2 "github.com/kubeflow/spark-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	crdclientfake "github.com/kubeflow/spark-operator/pkg/client/clientset/versioned/fake"
	crdinformers "github.com/kubeflow/spark-operator/pkg/client/informers/externalversions"
	"github.com/kubeflow/spark-operator/pkg/config"
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

func TestSelfRegistrationWithObjectSelector(t *testing.T) {
	clientset := fake.NewSimpleClientset()
	informerFactory := crdinformers.NewSharedInformerFactory(nil, 0)
	coreV1InformerFactory := informers.NewSharedInformerFactory(nil, 0)

	// Setup userConfig with object selector
	userConfig.webhookObjectSelector = "spark-role in (driver,executor)"
	webhookTimeout := 30

	// Create webhook instance
	webhook, err := New(clientset, informerFactory, "default", false, false, coreV1InformerFactory, &webhookTimeout)
	assert.NoError(t, err)

	// Mock the clientset's Create function to capture the MutatingWebhookConfiguration object
	var createdWebhookConfig *arv1.MutatingWebhookConfiguration
	clientset.PrependReactor("create", "mutatingwebhookconfigurations", func(action gotest.Action) (handled bool, ret runtime.Object, err error) {
		createAction := action.(gotest.CreateAction)
		createdWebhookConfig = createAction.GetObject().(*arv1.MutatingWebhookConfiguration)
		return true, createdWebhookConfig, nil
	})

	// Call the selfRegistration method
	err = webhook.selfRegistration("test-webhook-config")
	assert.NoError(t, err)

	// Verify the MutatingWebhookConfiguration was created with the expected object selector
	assert.NotNil(t, createdWebhookConfig, "MutatingWebhookConfiguration should have been created")

	expectedSelector := &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{
				Key:      "spark-role",
				Operator: metav1.LabelSelectorOpIn,
				Values:   []string{"driver", "executor"},
			},
		},
	}
	actualSelector := createdWebhookConfig.Webhooks[0].ObjectSelector

	assert.True(t, labelSelectorsEqual(expectedSelector, actualSelector), "ObjectSelectors should be equal")
}

func labelSelectorsEqual(expected, actual *metav1.LabelSelector) bool {
	if expected == nil || actual == nil {
		return expected == nil && actual == nil
	}

	if len(expected.MatchLabels) != len(actual.MatchLabels) {
		return false
	}

	for k, v := range expected.MatchLabels {
		if actual.MatchLabels[k] != v {
			return false
		}
	}

	if len(expected.MatchExpressions) != len(actual.MatchExpressions) {
		return false
	}

	for i, expr := range expected.MatchExpressions {
		if expr.Key != actual.MatchExpressions[i].Key ||
			expr.Operator != actual.MatchExpressions[i].Operator ||
			!equalStringSlices(expr.Values, actual.MatchExpressions[i].Values) {
			return false
		}
	}

	return true
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func testSelector(input string, expected *metav1.LabelSelector, t *testing.T) {
	selector, err := parseSelector(input)

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
