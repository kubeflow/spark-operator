/*
Copyright 2017 Google LLC

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

package initializer

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"k8s.io/spark-on-k8s-operator/pkg/config"
	"k8s.io/spark-on-k8s-operator/pkg/secret"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestAddAndDeleteInitializationConfig(t *testing.T) {
	controller := New(fake.NewSimpleClientset())
	client := controller.kubeClient.AdmissionregistrationV1alpha1()

	controller.addInitializationConfig()
	ig1, err := client.InitializerConfigurations().Get(sparkPodInitializerConfigName, metav1.GetOptions{})
	if err != nil {
		t.Error(err)
	}
	if ig1.Name != sparkPodInitializerConfigName {
		t.Errorf("mismatched name wanted %s got %s", sparkPodInitializerConfigName, ig1.Name)
	}

	controller.addInitializationConfig()
	ig2, err := client.InitializerConfigurations().Get(sparkPodInitializerConfigName, metav1.GetOptions{})
	if err != nil {
		t.Error(err)
	}
	if ig1.UID != ig2.UID {
		t.Errorf("mismatched UID wanted %s got %s", ig1.UID, ig2.UID)
	}

	list, err := client.InitializerConfigurations().List(metav1.ListOptions{})
	if err != nil {
		t.Error(err)
	}
	if len(list.Items) != 1 {
		t.Errorf("unexpected number of items wanted 1 got %d", len(list.Items))
	}

	controller.deleteInitializationConfig()
	_, err = client.InitializerConfigurations().Get(sparkPodInitializerConfigName, metav1.GetOptions{})
	if err != nil {
		if !errors.IsNotFound(err) {
			t.Error(err)
		}
	}
}

func TestRemoteInitializer(t *testing.T) {
	type testcase struct {
		name    string
		pod     *apiv1.Pod
		removed bool
	}

	testFn := func(test testcase, t *testing.T) {
		removeSelf(test.pod)
		if test.removed && isInitializerPresent(test.pod) {
			t.Errorf("%s: %s was not removed", test.name, sparkPodInitializerName)
		}
	}

	pod0 := &apiv1.Pod{}
	pod1 := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Initializers: &metav1.Initializers{
				Pending: []metav1.Initializer{},
			},
		},
	}
	pod2 := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Initializers: &metav1.Initializers{
				Pending: []metav1.Initializer{
					{
						Name: sparkPodInitializerName,
					},
				},
			},
		},
	}
	pod3 := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Initializers: &metav1.Initializers{
				Pending: []metav1.Initializer{
					{
						Name: sparkPodInitializerName,
					},
					{
						Name: "foo",
					},
				},
			},
		},
	}

	testcases := []testcase{
		{
			name:    "pod with nil Initializers",
			pod:     pod0,
			removed: false,
		},
		{
			name:    "pod with empty Initializers",
			pod:     pod1,
			removed: false,
		},
		{
			name:    "pod with only the initializer",
			pod:     pod2,
			removed: true,
		},
		{
			name:    "pod with the initializer and another initializer",
			pod:     pod3,
			removed: true,
		},
	}

	for _, test := range testcases {
		testFn(test, t)
	}
}

func TestIsSparkPod(t *testing.T) {
	type testcase struct {
		name     string
		pod      *apiv1.Pod
		sparkPod bool
	}
	testFn := func(test testcase, t *testing.T) {
		result := isSparkPod(test.pod)
		if result != test.sparkPod {
			t.Errorf("%s: isSparkPod is %v while expectation is %v", test.name, result, test.sparkPod)
		}
	}

	pod1 := &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1"}}
	pod2 := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "pod1",
			Labels: map[string]string{sparkRoleLabel: sparkDriverRole},
		},
	}
	pod3 := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "pod1",
			Labels: map[string]string{sparkRoleLabel: sparkExecutorRole},
		},
	}
	testcases := []testcase{
		{
			name:     "not a Spark Pod",
			pod:      pod1,
			sparkPod: false,
		},
		{
			name:     "a Spark driver Pod",
			pod:      pod2,
			sparkPod: true,
		},
		{
			name:     "a Spark executor Pod",
			pod:      pod3,
			sparkPod: true,
		},
	}
	for _, test := range testcases {
		testFn(test, t)
	}
}

func TestIsInitializerPresent(t *testing.T) {
	type testcase struct {
		name    string
		pod     *apiv1.Pod
		present bool
	}
	testFn := func(test testcase, t *testing.T) {
		result := isInitializerPresent(test.pod)
		if result != test.present {
			t.Errorf("%s: isInitializerPresent is %v while expectation is %v", test.name, result, test.present)
		}
	}

	pod1 := &apiv1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1"}}
	pod2 := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod1",
			Initializers: &metav1.Initializers{
				Pending: []metav1.Initializer{
					{
						Name: sparkPodInitializerName,
					},
				},
			},
		},
	}
	testcases := []testcase{
		{
			name:    "without the initializer",
			pod:     pod1,
			present: false,
		},
		{
			name:    "with the initializer",
			pod:     pod2,
			present: true,
		},
	}
	for _, test := range testcases {
		testFn(test, t)
	}
}

func TestAddPod(t *testing.T) {
	type testcase struct {
		name   string
		pod    *apiv1.Pod
		queued bool
	}
	controller := New(fake.NewSimpleClientset())
	testFn := func(test testcase, t *testing.T) {
		controller.onPodAdded(test.pod)
		if test.queued {
			key, _ := controller.queue.Get()
			keyString := key.(string)
			expectedQueueKey := getQueueKey(test.pod)
			if keyString != expectedQueueKey {
				t.Errorf("%s: expected queue key %s got %s", test.name, expectedQueueKey, keyString)
			}
			controller.queue.Done(key)
			if controller.queue.Len() > 0 {
				t.Errorf("%s: expected queue to be emptied got %d keys", test.name, controller.queue.Len())
			}
		}
		if !test.queued && controller.queue.Len() > 0 {
			t.Errorf("%s: expected an empty queue got %d keys", test.name, controller.queue.Len())
		}
	}

	pod0 := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod1",
			Namespace: "default",
		},
	}
	pod1 := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod1",
			Namespace: "default",
			Initializers: &metav1.Initializers{
				Pending: []metav1.Initializer{
					{
						Name: sparkPodInitializerName,
					},
				},
			},
		},
	}
	pod2 := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod1",
			Namespace: "default",
			Labels:    map[string]string{sparkRoleLabel: sparkDriverRole},
		},
	}
	pod3 := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod1",
			Namespace: "default",
			Labels:    map[string]string{sparkRoleLabel: sparkDriverRole},
			Initializers: &metav1.Initializers{
				Pending: []metav1.Initializer{
					{
						Name: sparkPodInitializerName,
					},
				},
			},
		},
	}
	testcases := []testcase{
		{
			name:   "non-Spark pod without initializers",
			pod:    pod0,
			queued: false,
		},
		{
			name:   "non-Spark pod with the initializer",
			pod:    pod1,
			queued: false,
		},
		{
			name:   "Spark pod without the initializer",
			pod:    pod2,
			queued: false,
		},
		{
			name:   "Spark pod with the initializer",
			pod:    pod3,
			queued: true,
		},
	}
	for _, test := range testcases {
		testFn(test, t)
	}
}

func TestSyncSparkPod(t *testing.T) {
	type testcase struct {
		name                string
		pod                 *apiv1.Pod
		expectedVolumeNames []string
		expectedObjectNames []string
	}

	clientset := fake.NewSimpleClientset()
	clientset.PrependReactor("patch", "pods", func(action clienttesting.Action) (handled bool, ret runtime.Object, err error) {
		patch := action.(clienttesting.PatchActionImpl).GetPatch()
		obj := &apiv1.Pod{}
		json.Unmarshal(patch, obj)
		return true, obj, nil
	})
	controller := New(clientset)

	testFn := func(test testcase, t *testing.T) {
		_, err := controller.kubeClient.CoreV1().Pods(test.pod.Namespace).Create(test.pod)
		if err != nil {
			t.Fatal(err)
		}

		modifiedPod, err := controller.initializeSparkPod(test.pod)
		if err != nil {
			t.Fatal(err)
		}

		if len(modifiedPod.Spec.Volumes) != len(test.expectedVolumeNames) {
			t.Errorf("%s: wanted %d volumes got %d", test.name, len(test.expectedVolumeNames), len(modifiedPod.Spec.Volumes))
		}
		for i := 0; i < len(modifiedPod.Spec.Volumes); i++ {
			name := modifiedPod.Spec.Volumes[i].Name
			if name != test.expectedVolumeNames[i] {
				t.Errorf("%s: for ConfigMap volume name wanted %s got %s", test.name, test.expectedVolumeNames[i], name)
			}
		}
		for i := 0; i < len(modifiedPod.Spec.Volumes); i++ {
			var name string
			if modifiedPod.Spec.Volumes[i].ConfigMap != nil {
				name = modifiedPod.Spec.Volumes[i].ConfigMap.LocalObjectReference.Name
			} else if modifiedPod.Spec.Volumes[i].Secret != nil {
				name = modifiedPod.Spec.Volumes[i].Secret.SecretName
			}
			if name != test.expectedObjectNames[i] {
				t.Errorf("%s: for ConfigMap name wanted %s got %s", test.name, test.expectedObjectNames[i], name)
			}
		}

		if len(modifiedPod.Spec.Containers) != 1 {
			return
		}
		container := modifiedPod.Spec.Containers[0]
		if len(container.VolumeMounts) != len(test.expectedVolumeNames) {
			t.Errorf("%s: wanted %d volumn mounts got %d", test.name, len(test.expectedVolumeNames), len(container.VolumeMounts))
		}
		for i := 0; i < len(container.VolumeMounts); i++ {
			volumeMount := container.VolumeMounts[i]
			if volumeMount.Name != test.expectedVolumeNames[i] {
				t.Errorf("%s: for volume mount name wanted %s got %s", test.name, test.expectedVolumeNames[i], volumeMount.Name)
			}
		}
		for i := 0; i < len(container.VolumeMounts); i++ {
			volumeMount := container.VolumeMounts[i]
			if volumeMount.Name == config.SparkConfigMapVolumeName {
				if !hasEnvVar(config.SparkConfDirEnvVar, volumeMount.MountPath, container.Env) {
					t.Errorf("expected environment variable %s but not found", config.SparkConfDirEnvVar)
				}
			} else if volumeMount.Name == config.HadoopConfigMapVolumeName {
				if !hasEnvVar(config.HadoopConfDirEnvVar, volumeMount.MountPath, container.Env) {
					t.Errorf("expected environment variable %s but not found", config.HadoopConfDirEnvVar)
				}
			} else if volumeMount.Name == secret.ServiceAccountSecretVolumeName {
				keyFilePath := fmt.Sprintf("%s/%s", volumeMount.MountPath, secret.ServiceAccountJSONKeyFileName)
				if !hasEnvVar(secret.GoogleApplicationCredentialsEnvVar, keyFilePath, container.Env) {
					t.Errorf("expected environment variable %s but not found", secret.GoogleApplicationCredentialsEnvVar)
				}
			}
		}
	}

	pod1 := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "pod1",
			Namespace:   "default",
			Annotations: map[string]string{},
		},
		Spec: apiv1.PodSpec{
			Containers: []apiv1.Container{
				{Name: "foo"},
			},
		},
	}
	pod2 := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod2",
			Namespace: "default",
			Annotations: map[string]string{
				config.SparkConfigMapAnnotation:  "spark-config-map",
				config.HadoopConfigMapAnnotation: "hadoop-config-map",
			},
		},
		Spec: apiv1.PodSpec{
			Containers: []apiv1.Container{
				{Name: "foo"},
			},
		},
	}
	pod3 := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod3",
			Namespace: "default",
			Annotations: map[string]string{
				config.SparkConfigMapAnnotation: "spark-config-map",
			},
		},
		Spec: apiv1.PodSpec{
			Containers: []apiv1.Container{
				{Name: "foo"},
			},
		},
	}
	pod4 := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod4",
			Namespace: "default",
			Annotations: map[string]string{
				config.HadoopConfigMapAnnotation: "hadoop-config-map",
			},
		},
		Spec: apiv1.PodSpec{
			Containers: []apiv1.Container{
				{Name: "foo"},
			},
		},
	}
	pod5 := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod5",
			Namespace: "default",
			Annotations: map[string]string{
				config.GCPServiceAccountSecretAnnotationPrefix + "gcp-service-account": "/etc/secrets",
			},
		},
		Spec: apiv1.PodSpec{
			Containers: []apiv1.Container{
				{Name: "foo"},
			},
		},
	}
	testcases := []testcase{
		{
			name:                "pod without SparkConfigMap or HadoopConfigMap annotation",
			pod:                 pod1,
			expectedVolumeNames: []string{},
			expectedObjectNames: []string{},
		},
		{
			name:                "pod with both SparkConfigMap or HadoopConfigMap annotations",
			pod:                 pod2,
			expectedVolumeNames: []string{config.SparkConfigMapVolumeName, config.HadoopConfigMapVolumeName},
			expectedObjectNames: []string{"spark-config-map", "hadoop-config-map"},
		},
		{
			name:                "pod with SparkConfigMap annotation",
			pod:                 pod3,
			expectedVolumeNames: []string{config.SparkConfigMapVolumeName},
			expectedObjectNames: []string{"spark-config-map"},
		},
		{
			name:                "pod with HadoopConfigMap annotation",
			pod:                 pod4,
			expectedVolumeNames: []string{config.HadoopConfigMapVolumeName},
			expectedObjectNames: []string{"hadoop-config-map"},
		},
		{
			name:                "pod with GCP service account secret annotation",
			pod:                 pod5,
			expectedVolumeNames: []string{secret.ServiceAccountSecretVolumeName},
			expectedObjectNames: []string{"gcp-service-account"},
		},
	}
	for _, test := range testcases {
		testFn(test, t)
	}
}

func TestDeletePod(t *testing.T) {
	type testcase struct {
		name   string
		pod    *apiv1.Pod
		queued bool
	}

	controller := New(fake.NewSimpleClientset())
	testFn := func(test testcase, t *testing.T) {
		controller.onPodAdded(test.pod)
		if test.queued {
			key, _ := controller.queue.Get()
			keyString := key.(string)
			expectedQueueKey := getQueueKey(test.pod)
			if keyString != expectedQueueKey {
				t.Errorf("%s: expected queue key %s got %s", test.name, expectedQueueKey, keyString)
			}
		}
		controller.onPodDeleted(test.pod)
		if controller.queue.Len() > 0 {
			t.Errorf("%s: expected queue to be emptied got %d keys", test.name, controller.queue.Len())
		}
	}

	pod0 := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod1",
			Namespace: "default",
		},
	}
	pod1 := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod1",
			Namespace: "default",
			Initializers: &metav1.Initializers{
				Pending: []metav1.Initializer{
					{
						Name: sparkPodInitializerName,
					},
				},
			},
		},
	}
	pod2 := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod1",
			Namespace: "default",
			Labels:    map[string]string{sparkRoleLabel: sparkDriverRole},
		},
	}
	pod3 := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod1",
			Namespace: "default",
			Labels:    map[string]string{sparkRoleLabel: sparkDriverRole},
			Initializers: &metav1.Initializers{
				Pending: []metav1.Initializer{
					{
						Name: sparkPodInitializerName,
					},
				},
			},
		},
	}
	testcases := []testcase{
		{
			name:   "non-Spark pod without initializers",
			pod:    pod0,
			queued: false,
		},
		{
			name:   "non-Spark pod with the initializer",
			pod:    pod1,
			queued: false,
		},
		{
			name:   "Spark pod without the initializer",
			pod:    pod2,
			queued: false,
		},
		{
			name:   "Spark pod with the initializer",
			pod:    pod3,
			queued: true,
		},
	}
	for _, test := range testcases {
		testFn(test, t)
	}
}

func TestAddOwnerReference(t *testing.T) {
	type testcase struct {
		name                   string
		pod                    *apiv1.Pod
		hasOwnerReference      bool
		expectedOwnerReference *metav1.OwnerReference
	}

	testFn := func(test testcase, t *testing.T) {
		addOwnerReference(test.pod)
		if test.hasOwnerReference {
			if len(test.pod.ObjectMeta.OwnerReferences) != 1 {
				t.Errorf("%s: expected one OwnerReference got %d", test.name, len(test.pod.ObjectMeta.OwnerReferences))
			} else {
				ownerReference := test.pod.ObjectMeta.OwnerReferences[0]
				if !reflect.DeepEqual(ownerReference, *test.expectedOwnerReference) {
					t.Errorf("%s: actual OwnerReference %v and expected one %v are not equal", test.name, ownerReference, *test.expectedOwnerReference)
				}
			}
		}
		if !test.hasOwnerReference && len(test.pod.ObjectMeta.OwnerReferences) > 0 {
			t.Errorf("%s: expected no OwnerReference got %d", test.name, len(test.pod.ObjectMeta.OwnerReferences))
		}
	}

	pod0 := &apiv1.Pod{}
	ownerReference := metav1.OwnerReference{
		APIVersion: "v1alpha1",
		Kind:       "SparkApp",
		Name:       "TestSparkApp",
		Controller: new(bool),
	}
	ownerReferenceBytes, err := ownerReference.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	pod1 := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				config.OwnerReferenceAnnotation: string(ownerReferenceBytes),
			},
		},
	}
	testcases := []testcase{
		{
			name:                   "Pod without OwnerReference annotation",
			pod:                    pod0,
			hasOwnerReference:      false,
			expectedOwnerReference: nil,
		},
		{
			name:                   "Pod with OwnerReference annotation",
			pod:                    pod1,
			hasOwnerReference:      true,
			expectedOwnerReference: &ownerReference,
		},
	}
	for _, test := range testcases {
		testFn(test, t)
	}
}

func hasEnvVar(name, value string, envVars []apiv1.EnvVar) bool {
	for _, envVar := range envVars {
		if envVar.Name == name && envVar.Value == value {
			return true
		}
	}
	return false
}
