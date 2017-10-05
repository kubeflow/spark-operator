package controller

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/liyinan926/spark-operator/pkg/apis/v1alpha1"
	"github.com/liyinan926/spark-operator/pkg/config"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCreateSparkUIService(t *testing.T) {
	type testcase struct {
		name                string
		app                 *v1alpha1.SparkApplication
		expectedServiceName string
		expectedServicePort int32
		expectedSelector    map[string]string
		expectError         bool
	}
	testFn := func(test testcase, t *testing.T) {
		fakeClient := fake.NewSimpleClientset()
		_, err := createSparkUIService(test.app, fakeClient)
		if !test.expectError && err != nil {
			t.Fatal(err)
		}
		if test.expectError {
			if err == nil {
				t.Errorf("%s: expected error got nothing", test.name)
			} else {
				return
			}
		}

		if test.app.Status.UIServiceInfo.Name != test.expectedServiceName {
			t.Errorf("%s: for service name wanted %s got %s", test.name, test.expectedServiceName, test.app.Status.UIServiceInfo.Name)
		}
		service, err := fakeClient.CoreV1().Services(test.app.Namespace).Get(test.app.Status.UIServiceInfo.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(test.expectedSelector, service.Spec.Selector) {
			t.Errorf("%s: for label selector wanted %s got %s", test.name, test.expectedSelector, service.Spec.Selector)
		}
		if service.Spec.Type != apiv1.ServiceTypeNodePort {
			t.Errorf("%s: for service type wanted %s got %s", test.name, apiv1.ServiceTypeNodePort, service.Spec.Type)
		}
		if len(service.Spec.Ports) != 1 {
			t.Errorf("%s: wanted a single port got %d ports", test.name, len(service.Spec.Ports))
		}
		port := service.Spec.Ports[0]
		if port.Port != test.expectedServicePort {
			t.Errorf("%s: unexpected port wanted %d got %d", test.name, test.expectedServicePort, port.Port)
		}
		if port.TargetPort.IntVal != test.expectedServicePort {
			t.Errorf("%s: unexpected target port wanted %d got %d", test.name, test.expectedServicePort, port.TargetPort.IntVal)
		}
		if test.app.Status.UIServiceInfo.Port != test.expectedServicePort {
			t.Errorf("%s: unexpected port wanted %d got %d", test.name, test.expectedServicePort, test.app.Status.UIServiceInfo.Port)
		}
	}

	app1 := &v1alpha1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
			UID:       "foo-123",
		},
		Spec: v1alpha1.SparkApplicationSpec{
			SparkConf: map[string]string{
				sparkUIPortConfigurationKey: "4041",
			},
		},
		Status: v1alpha1.SparkApplicationStatus{
			AppID: "foo-1",
		},
	}
	app2 := &v1alpha1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
			UID:       "foo-123",
		},
		Status: v1alpha1.SparkApplicationStatus{
			AppID: "foo-2",
		},
	}
	defaultPort, err := strconv.Atoi(defaultSparkWebUIPort)
	if err != nil {
		t.Fatal(err)
	}
	app3 := &v1alpha1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
			UID:       "foo-123",
		},
		Spec: v1alpha1.SparkApplicationSpec{
			SparkConf: map[string]string{
				sparkUIPortConfigurationKey: "4041x",
			},
		},
		Status: v1alpha1.SparkApplicationStatus{
			AppID: "foo-3",
		},
	}
	testcases := []testcase{
		testcase{
			name:                "service with custom port",
			app:                 app1,
			expectedServiceName: buildUIServiceName(app1),
			expectedServicePort: 4041,
			expectedSelector: map[string]string{
				config.SparkAppIDLabel: "foo-1",
				sparkRoleLabel:         sparkDriverRole,
			},
			expectError: false,
		},
		testcase{
			name:                "service with default port",
			app:                 app2,
			expectedServiceName: buildUIServiceName(app2),
			expectedServicePort: int32(defaultPort),
			expectedSelector: map[string]string{
				config.SparkAppIDLabel: "foo-2",
				sparkRoleLabel:         sparkDriverRole,
			},
			expectError: false,
		},
		testcase{
			name:        "service with bad port configurations",
			app:         app3,
			expectError: true,
		},
	}
	for _, test := range testcases {
		testFn(test, t)
	}
}
