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

package sparkapplication

import (
	"reflect"
	"strconv"
	"testing"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	"k8s.io/spark-on-k8s-operator/pkg/config"
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
		uiServiceName, uiServicePort, err := createSparkUIService(test.app, test.app.Status.AppID, fakeClient)
		if err != nil {
			if test.expectError {
				return
			}
			t.Fatal(err)
		}
		test.app.Status.DriverInfo.WebUIServiceName = uiServiceName
		test.app.Status.DriverInfo.WebUIPort = uiServicePort

		if test.app.Status.DriverInfo.WebUIServiceName != test.expectedServiceName {
			t.Errorf("%s: for service name wanted %s got %s", test.name, test.expectedServiceName, test.app.Status.DriverInfo.WebUIServiceName)
		}
		service, err := fakeClient.CoreV1().
			Services(test.app.Namespace).
			Get(test.app.Status.DriverInfo.WebUIServiceName, metav1.GetOptions{})
		if err != nil {
			if test.expectError {
				return
			}
			t.Fatal(err)
		}
		if len(service.Labels) != 2 || service.Labels[config.SparkAppIDLabel] != test.app.Status.AppID ||
			service.Labels[config.SparkAppNameLabel] != test.app.Name {
			t.Errorf("%s: service of app %s has the wrong labels", test.name, test.app.Name)
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
	}

	defaultPort, err := strconv.Atoi(defaultSparkWebUIPort)
	if err != nil {
		t.Fatal(err)
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
		{
			name:                "service with custom port",
			app:                 app1,
			expectedServiceName: app1.Status.AppID + "-ui-svc",
			expectedServicePort: 4041,
			expectedSelector: map[string]string{
				config.SparkAppNameLabel: "foo",
				config.SparkAppIDLabel:   "foo-1",
				sparkRoleLabel:           sparkDriverRole,
			},
			expectError: false,
		},
		{
			name:                "service with default port",
			app:                 app2,
			expectedServiceName: app2.Status.AppID + "-ui-svc",
			expectedServicePort: int32(defaultPort),
			expectedSelector: map[string]string{
				config.SparkAppNameLabel: "foo",
				config.SparkAppIDLabel:   "foo-2",
				sparkRoleLabel:           sparkDriverRole,
			},
			expectError: false,
		},
		{
			name:        "service with bad port configurations",
			app:         app3,
			expectError: true,
		},
	}
	for _, test := range testcases {
		testFn(test, t)
	}
}
