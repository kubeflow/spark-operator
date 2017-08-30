package initializer

import (
	"testing"

	"github.com/liyinan926/spark-operator/pkg/config"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestAddInitializationConfig(t *testing.T) {
	controller := newFakeController()
	client := controller.kubeClient.AdmissionregistrationV1alpha1()

	controller.addInitializationConfig()
	ig1, err := client.InitializerConfigurations().Get(initializerConfigName, metav1.GetOptions{})
	if err != nil {
		t.Error(err)
	}
	if ig1.Name != initializerConfigName {
		t.Errorf("mismatched name wanted %s got %s", initializerConfigName, ig1.Name)
	}

	controller.addInitializationConfig()
	ig2, err := client.InitializerConfigurations().Get(initializerConfigName, metav1.GetOptions{})
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
}

func TestRemoteInitializer(t *testing.T) {
	type testcase struct {
		name    string
		pod     *apiv1.Pod
		removed bool
	}

	testFn := func(test *testcase, t *testing.T) {
		remoteInitializer(test.pod)
		if test.removed && isInitializerPresent(test.pod) {
			t.Errorf("%s: %s was not removed", test.name, initializerName)
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
					metav1.Initializer{
						Name: initializerName,
					},
				},
			},
		},
	}
	pod3 := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Initializers: &metav1.Initializers{
				Pending: []metav1.Initializer{
					metav1.Initializer{
						Name: initializerName,
					},
					metav1.Initializer{
						Name: "foo",
					},
				},
			},
		},
	}

	testcases := []*testcase{
		&testcase{
			name:    "pod with nil Initializers",
			pod:     pod0,
			removed: false,
		},
		&testcase{
			name:    "pod with empty Initializers",
			pod:     pod1,
			removed: false,
		},
		&testcase{
			name:    "pod with only the initializer",
			pod:     pod2,
			removed: true,
		},
		&testcase{
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
		testcase{
			name:     "not a Spark Pod",
			pod:      pod1,
			sparkPod: false,
		},
		testcase{
			name:     "a Spark driver Pod",
			pod:      pod2,
			sparkPod: true,
		},
		testcase{
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
					metav1.Initializer{
						Name: initializerName,
					},
				},
			},
		},
	}
	testcases := []testcase{
		testcase{
			name:    "without the initializer",
			pod:     pod1,
			present: false,
		},
		testcase{
			name:    "with the initializer",
			pod:     pod2,
			present: true,
		},
	}
	for _, test := range testcases {
		testFn(test, t)
	}
}

func TestSyncSparkPod(t *testing.T) {
	type testcase struct {
		name                   string
		pod                    *apiv1.Pod
		expectedConfigMapNames []string
		expectedVolumeNames    []string
	}
	controller := newFakeController()
	testFn := func(test testcase, t *testing.T) {
		_, err := controller.kubeClient.CoreV1().Pods(test.pod.Namespace).Create(test.pod)
		if err != nil {
			t.Fatal(err)
		}
		err = controller.syncSparkPod(test.pod)
		if err != nil {
			t.Fatal(err)
		}
		updatedPod, err := controller.kubeClient.CoreV1().Pods(test.pod.Namespace).Get(test.pod.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(updatedPod.Spec.Volumes) != len(test.expectedVolumeNames) {
			t.Errorf("%s: wanted %d volumes got %d", test.name, len(test.expectedVolumeNames), len(updatedPod.Spec.Volumes))
		}
		for i := 0; i < len(updatedPod.Spec.Volumes); i++ {
			name := updatedPod.Spec.Volumes[i].Name
			if name != test.expectedVolumeNames[i] {
				t.Errorf("%s: for ConfigMap volume name wanted %s got %s", test.name, test.expectedVolumeNames[i], name)
			}
		}
		for i := 0; i < len(updatedPod.Spec.Volumes); i++ {
			name := updatedPod.Spec.Volumes[i].ConfigMap.LocalObjectReference.Name
			if name != test.expectedConfigMapNames[i] {
				t.Errorf("%s: for ConfigMap name wanted %s got %s", test.name, test.expectedConfigMapNames[i], name)
			}
		}
		container := updatedPod.Spec.Containers[0]
		if len(container.VolumeMounts) != len(test.expectedVolumeNames) {
			t.Errorf("%s: wanted %d volumn mounts got %d", test.name, len(test.expectedVolumeNames), len(container.VolumeMounts))
		}
		for i := 0; i < len(container.VolumeMounts); i++ {
			volumeMount := container.VolumeMounts[i]
			if volumeMount.Name != test.expectedVolumeNames[i] {
				t.Errorf("%s: for volume mount name wanted %s got %s", test.name, test.expectedVolumeNames[i], volumeMount.Name)
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
				apiv1.Container{},
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
				apiv1.Container{},
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
				apiv1.Container{},
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
				apiv1.Container{},
			},
		},
	}
	testcases := []testcase{
		testcase{
			name:                   "pod without SparkConfigMap or HadoopConfigMap annotation",
			pod:                    pod1,
			expectedVolumeNames:    []string{},
			expectedConfigMapNames: []string{},
		},
		testcase{
			name:                   "pod with both SparkConfigMap or HadoopConfigMap annotations",
			pod:                    pod2,
			expectedVolumeNames:    []string{config.SparkConfigMapVolumeName, config.HadoopConfigMapVolumeName},
			expectedConfigMapNames: []string{"spark-config-map", "hadoop-config-map"},
		},
		testcase{
			name:                   "pod with SparkConfigMap annotation",
			pod:                    pod3,
			expectedVolumeNames:    []string{config.SparkConfigMapVolumeName},
			expectedConfigMapNames: []string{"spark-config-map"},
		},
		testcase{
			name:                   "pod with HadoopConfigMap annotation",
			pod:                    pod4,
			expectedVolumeNames:    []string{config.HadoopConfigMapVolumeName},
			expectedConfigMapNames: []string{"hadoop-config-map"},
		},
	}
	for _, test := range testcases {
		testFn(test, t)
	}
}

func newFakeController() *Controller {
	return NewController(fake.NewSimpleClientset())
}
