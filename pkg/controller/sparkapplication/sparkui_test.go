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
	"fmt"
	"reflect"
	"strconv"
	"testing"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
)

func TestCreateSparkUIService(t *testing.T) {
	type testcase struct {
		name             string
		app              *v1alpha1.SparkApplication
		expectedService  SparkService
		expectedSelector map[string]string
		expectError      bool
	}
	testFn := func(test testcase, t *testing.T) {
		fakeClient := fake.NewSimpleClientset()
		sparkService, err := createSparkUIService(test.app, fakeClient)
		if err != nil {
			if test.expectError {
				return
			}
			t.Fatal(err)
		}

		if sparkService.serviceName != test.expectedService.serviceName {
			t.Errorf("%s: for service name wanted %s got %s", test.name, test.expectedService.serviceName, sparkService.serviceName)
		}

		service, err := fakeClient.CoreV1().
			Services(test.app.Namespace).
			Get(sparkService.serviceName, metav1.GetOptions{})
		if err != nil {
			if test.expectError {
				return
			}
			t.Fatal(err)
		}
		if len(service.Labels) != 1 ||
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
		if port.Port != test.expectedService.servicePort {
			t.Errorf("%s: unexpected port wanted %d got %d", test.name, test.expectedService.servicePort, port.Port)
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
			SparkApplicationID: "foo-1",
			ExecutionAttempts:  1,
		},
	}
	app2 := &v1alpha1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
			UID:       "foo-123",
		},
		Status: v1alpha1.SparkApplicationStatus{
			SparkApplicationID: "foo-2",
			ExecutionAttempts:  2,
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
			SparkApplicationID: "foo-3",
		},
	}
	testcases := []testcase{
		{
			name: "service with custom port",
			app:  app1,
			expectedService: SparkService{
				serviceName: fmt.Sprintf("%s-ui-svc", app1.GetName()),
				servicePort: 4041,
			},
			expectedSelector: map[string]string{
				config.SparkAppNameLabel: "foo",
				config.SparkRoleLabel:    sparkDriverRole,
			},
			expectError: false,
		},
		{
			name: "service with default port",
			app:  app2,
			expectedService: SparkService{
				serviceName: fmt.Sprintf("%s-ui-svc", app2.GetName()),
				servicePort: int32(defaultPort),
			},
			expectedSelector: map[string]string{
				config.SparkAppNameLabel: "foo",
				config.SparkRoleLabel:    sparkDriverRole,
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

func TestCreateSparkUIIngress(t *testing.T) {

	app := &v1alpha1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
			UID:       "foo-123",
		},
		Status: v1alpha1.SparkApplicationStatus{
			SparkApplicationID: "foo-1",
			DriverInfo: v1alpha1.DriverInfo{
				WebUIServiceName: "blah-service",
			},
		},
	}

	service := SparkService{
		serviceName: app.GetName() + "-ui-svc",
		servicePort: 4041,
	}
	ingressFormat := "{{$appName}}.ingress.clusterName.com"

	expectedIngress := SparkIngress{
		ingressName: fmt.Sprintf("%s-ui-ingress", app.GetName()),
		ingressUrl:  app.GetName() + ".ingress.clusterName.com",
	}
	fakeClient := fake.NewSimpleClientset()
	sparkIngress, err := createSparkUIIngress(app, service, ingressFormat, fakeClient)
	if err != nil {
		t.Fatal(err)
	}

	if sparkIngress.ingressName != expectedIngress.ingressName {
		t.Errorf("Ingress name wanted %s got %s", expectedIngress.ingressName, sparkIngress.ingressName)
	}
	if sparkIngress.ingressUrl != expectedIngress.ingressUrl {
		t.Errorf("Ingress name wanted %s got %s", expectedIngress.ingressUrl, sparkIngress.ingressUrl)
	}

	ingress, err := fakeClient.Extensions().Ingresses(app.Namespace).
		Get(sparkIngress.ingressName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if len(ingress.Labels) != 1 ||
		ingress.Labels[config.SparkAppNameLabel] != app.Name {
		t.Errorf("Ingress of app %s has the wrong labels", app.Name)
	}

	if len(ingress.Spec.Rules) != 1 {
		t.Errorf("No Ingress rules found.")
	}
	ingressRule := ingress.Spec.Rules[0]
	if ingressRule.Host != expectedIngress.ingressUrl {
		t.Errorf("Ingress of app %s has the wrong host %s", expectedIngress.ingressUrl, ingressRule.Host)
	}

	if len(ingressRule.IngressRuleValue.HTTP.Paths) != 1 {
		t.Errorf("No Ingress paths found.")
	}
	ingressPath := ingressRule.IngressRuleValue.HTTP.Paths[0]
	if ingressPath.Backend.ServiceName != service.serviceName {
		t.Errorf("Service name wanted %s got %s", service.serviceName, ingressPath.Backend.ServiceName)
	}
	if ingressPath.Backend.ServicePort.IntVal != service.servicePort {
		t.Errorf("Service port wanted %v got %v", service.servicePort, ingressPath.Backend.ServicePort)
	}
}
