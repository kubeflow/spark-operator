package initializer

import (
	"testing"

	initializer "k8s.io/api/admissionregistration/v1alpha1"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestAddInitializationConfig(t *testing.T) {
	controller := newFakeController()
	client := controller.kubeClient.AdmissionregistrationV1alpha1()

	controller.addInitializationConfig()
	ig1, err := client.InitializerConfigurations().Get(InitializerConfigName, metav1.GetOptions{})
	if err != nil {
		t.Error(err)
	}
	if ig1.Name != InitializerConfigName {
		t.Errorf("mismatched name wanted %s got %s", InitializerConfigName, ig1.Name)
	}

	controller.addInitializationConfig()
	ig2, err := client.InitializerConfigurations().Get(InitializerConfigName, metav1.GetOptions{})
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
		pod     *v1.Pod
		removed bool
	}

	testFn := func(test *testcase, t *testing.T) {
		remoteInitializer(test.pod)
		if test.removed && isInitializerPresent(test.pod) {
			t.Errorf("%s: %s was not removed", test.name, InitializerName)
		}
	}

	pod0 := &v1.Pod{}
	pod1 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Initializers: &metav1.Initializers{
				Pending: []metav1.Initializer{},
			},
		},
	}
	pod2 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Initializers: &metav1.Initializers{
				Pending: []metav1.Initializer{
					metav1.Initializer{
						Name: InitializerName,
					},
				},
			},
		},
	}
	pod3 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Initializers: &metav1.Initializers{
				Pending: []metav1.Initializer{
					metav1.Initializer{
						Name: InitializerName,
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

func newFakeController() *Controller {
	return NewController(fake.NewSimpleClientset(), "")
}

func newInitializerConfiguration() *initializer.InitializerConfiguration {
	return nil
}
