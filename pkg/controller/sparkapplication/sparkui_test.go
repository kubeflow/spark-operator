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
	"testing"

	apiv1 "k8s.io/api/core/v1"
	extensions "k8s.io/api/extensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
)

func TestCreateSparkUIService(t *testing.T) {
	type testcase struct {
		name             string
		app              *v1beta2.SparkApplication
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
		if service.Labels[config.SparkAppNameLabel] != test.app.Name {
			t.Errorf("%s: service of app %s has the wrong labels", test.name, test.app.Name)
		}
		if !reflect.DeepEqual(test.expectedSelector, service.Spec.Selector) {
			t.Errorf("%s: for label selector wanted %s got %s", test.name, test.expectedSelector, service.Spec.Selector)
		}
		if service.Spec.Type != apiv1.ServiceTypeClusterIP {
			t.Errorf("%s: for service type wanted %s got %s", test.name, apiv1.ServiceTypeClusterIP, service.Spec.Type)
		}
		if len(service.Spec.Ports) != 1 {
			t.Errorf("%s: wanted a single port got %d ports", test.name, len(service.Spec.Ports))
		}
		port := service.Spec.Ports[0]
		if port.Port != test.expectedService.servicePort {
			t.Errorf("%s: unexpected port wanted %d got %d", test.name, test.expectedService.servicePort, port.Port)
		}
	}
	defaultPort := defaultSparkWebUIPort
	app1 := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
			UID:       "foo-123",
		},
		Spec: v1beta2.SparkApplicationSpec{
			SparkConf: map[string]string{
				sparkUIPortConfigurationKey: "4041",
			},
		},
		Status: v1beta2.SparkApplicationStatus{
			SparkApplicationID: "foo-1",
			ExecutionAttempts:  1,
		},
	}
	app2 := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
			UID:       "foo-123",
		},
		Status: v1beta2.SparkApplicationStatus{
			SparkApplicationID: "foo-2",
			ExecutionAttempts:  2,
		},
	}
	app3 := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
			UID:       "foo-123",
		},
		Spec: v1beta2.SparkApplicationSpec{
			SparkConf: map[string]string{
				sparkUIPortConfigurationKey: "4041x",
			},
		},
		Status: v1beta2.SparkApplicationStatus{
			SparkApplicationID: "foo-3",
		},
	}
	var appPort int32 = 80
	app4 := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
			UID:       "foo-123",
		},
		Spec: v1beta2.SparkApplicationSpec{
			SparkUIOptions: &v1beta2.SparkUIConfiguration{
				ServicePort:        &appPort,
				IngressAnnotations: nil,
				IngressTLS:         nil,
			},
			SparkConf: map[string]string{
				sparkUIPortConfigurationKey: "4041",
			},
		},
		Status: v1beta2.SparkApplicationStatus{
			SparkApplicationID: "foo-3",
		},
	}
	testcases := []testcase{
		{
			name: "service with custom serviceport and serviceport and target port are same",
			app:  app1,
			expectedService: SparkService{
				serviceName: fmt.Sprintf("%s-ui-svc", app1.GetName()),
				servicePort: 4041,
				targetPort: intstr.IntOrString{
					Type:   intstr.Int,
					IntVal: int32(4041),
				},
			},
			expectedSelector: map[string]string{
				config.SparkAppNameLabel: "foo",
				config.SparkRoleLabel:    config.SparkDriverRole,
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
				config.SparkRoleLabel:    config.SparkDriverRole,
			},
			expectError: false,
		},
		{
			name: "service with custom serviceport and serviceport and target port are different",
			app:  app4,
			expectedService: SparkService{
				serviceName: fmt.Sprintf("%s-ui-svc", app4.GetName()),
				servicePort: 80,
				targetPort: intstr.IntOrString{
					Type:   intstr.Int,
					IntVal: int32(4041),
				},
			},
			expectedSelector: map[string]string{
				config.SparkAppNameLabel: "foo",
				config.SparkRoleLabel:    config.SparkDriverRole,
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
	type testcase struct {
		name            string
		app             *v1beta2.SparkApplication
		expectedIngress SparkIngress
		expectError     bool
	}

	testFn := func(test testcase, t *testing.T) {
		fakeClient := fake.NewSimpleClientset()
		ingressFormat := "{{$appName}}.ingress.clusterName.com"
		sparkService, err := createSparkUIService(test.app, fakeClient)
		sparkIngress, err := createSparkUIIngress(test.app, *sparkService, ingressFormat, fakeClient)
		if err != nil {
			if test.expectError {
				return
			}
			t.Fatal(err)
		}
		if sparkIngress.ingressName != test.expectedIngress.ingressName {
			t.Errorf("Ingress name wanted %s got %s", test.expectedIngress.ingressName, sparkIngress.ingressName)
		}
		if sparkIngress.ingressURL != test.expectedIngress.ingressURL {
			t.Errorf("Ingress name wanted %s got %s", test.expectedIngress.ingressURL, sparkIngress.ingressURL)
		}
		ingress, err := fakeClient.ExtensionsV1beta1().Ingresses(test.app.Namespace).
			Get(sparkIngress.ingressName, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(ingress.Annotations) != 0 {
			for key, value := range ingress.Annotations {
				if test.expectedIngress.annotations[key] != ingress.Annotations[key] {
					t.Errorf("Expected annotation: %s=%s but found : %s=%s", key, value, key, ingress.Annotations[key])
				}
			}
		}
		if len(ingress.Spec.TLS) != 0 {
			for _, ingressTls := range ingress.Spec.TLS {
				if ingressTls.Hosts[0] != test.expectedIngress.ingressTLS[0].Hosts[0] {
					t.Errorf("Expected ingressTls host: %s but found : %s", test.expectedIngress.ingressTLS[0].Hosts[0], ingressTls.Hosts[0])
				}
				if ingressTls.SecretName != test.expectedIngress.ingressTLS[0].SecretName {
					t.Errorf("Expected ingressTls secretName: %s but found : %s", test.expectedIngress.ingressTLS[0].SecretName, ingressTls.SecretName)
				}
			}
		}
		if ingress.Labels[config.SparkAppNameLabel] != test.app.Name {
			t.Errorf("Ingress of app %s has the wrong labels", test.app.Name)
		}

		if len(ingress.Spec.Rules) != 1 {
			t.Errorf("No Ingress rules found.")
		}
		ingressRule := ingress.Spec.Rules[0]
		if ingressRule.Host != test.expectedIngress.ingressURL {
			t.Errorf("Ingress of app %s has the wrong host %s", test.expectedIngress.ingressURL, ingressRule.Host)
		}

		if len(ingressRule.IngressRuleValue.HTTP.Paths) != 1 {
			t.Errorf("No Ingress paths found.")
		}
		ingressPath := ingressRule.IngressRuleValue.HTTP.Paths[0]
		if ingressPath.Backend.ServiceName != sparkService.serviceName {
			t.Errorf("Service name wanted %s got %s", sparkService.serviceName, ingressPath.Backend.ServiceName)
		}
		if ingressPath.Backend.ServicePort.IntVal != sparkService.servicePort {
			t.Errorf("Service port wanted %v got %v", sparkService.servicePort, ingressPath.Backend.ServicePort)
		}
	}

	var appPort int32 = 80
	app1 := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
			UID:       "foo-123",
		},
		Status: v1beta2.SparkApplicationStatus{
			SparkApplicationID: "foo-1",
			DriverInfo: v1beta2.DriverInfo{
				WebUIServiceName: "blah-service",
			},
		},
	}
	app2 := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
			UID:       "foo-123",
		},
		Spec: v1beta2.SparkApplicationSpec{
			SparkUIOptions: &v1beta2.SparkUIConfiguration{
				ServicePort: &appPort,
				IngressAnnotations: map[string]string{
					"kubernetes.io/ingress.class":                    "nginx",
					"nginx.ingress.kubernetes.io/force-ssl-redirect": "true",
				},
			},
		},
		Status: v1beta2.SparkApplicationStatus{
			SparkApplicationID: "foo-1",
			DriverInfo: v1beta2.DriverInfo{
				WebUIServiceName: "blah-service",
			},
		},
	}
	app3 := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
			UID:       "foo-123",
		},
		Spec: v1beta2.SparkApplicationSpec{
			SparkUIOptions: &v1beta2.SparkUIConfiguration{
				ServicePort: &appPort,
				IngressAnnotations: map[string]string{
					"kubernetes.io/ingress.class":                    "nginx",
					"nginx.ingress.kubernetes.io/force-ssl-redirect": "true",
				},
				IngressTLS: []extensions.IngressTLS{
					{[]string{"host1", "host2"}, "secret"},
				},
			},
		},
		Status: v1beta2.SparkApplicationStatus{
			SparkApplicationID: "foo-1",
			DriverInfo: v1beta2.DriverInfo{
				WebUIServiceName: "blah-service",
			},
		},
	}
	app4 := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
			UID:       "foo-123",
		},
		Spec: v1beta2.SparkApplicationSpec{
			SparkUIOptions: &v1beta2.SparkUIConfiguration{
				ServicePort: &appPort,
				IngressAnnotations: map[string]string{
					"kubernetes.io/ingress.class": "nginx",
				},
				IngressTLS: []extensions.IngressTLS{
					{[]string{"host1", "host2"}, ""},
				},
			},
		},
		Status: v1beta2.SparkApplicationStatus{
			SparkApplicationID: "foo-1",
			DriverInfo: v1beta2.DriverInfo{
				WebUIServiceName: "blah-service",
			},
		},
	}
	testcases := []testcase{
		{
			name: "simple ingress object",
			app:  app1,
			expectedIngress: SparkIngress{
				ingressName: fmt.Sprintf("%s-ui-ingress", app1.GetName()),
				ingressURL:  app1.GetName() + ".ingress.clusterName.com",
			},
			expectError: false,
		},
		{
			name: "ingress with annotations and without tls configuration",
			app:  app2,
			expectedIngress: SparkIngress{
				ingressName: fmt.Sprintf("%s-ui-ingress", app2.GetName()),
				ingressURL:  app2.GetName() + ".ingress.clusterName.com",
				annotations: map[string]string{
					"kubernetes.io/ingress.class":                    "nginx",
					"nginx.ingress.kubernetes.io/force-ssl-redirect": "true",
				},
			},
			expectError: false,
		},
		{
			name: "ingress with annotations and tls configuration",
			app:  app3,
			expectedIngress: SparkIngress{
				ingressName: fmt.Sprintf("%s-ui-ingress", app3.GetName()),
				ingressURL:  app3.GetName() + ".ingress.clusterName.com",
				annotations: map[string]string{
					"kubernetes.io/ingress.class":                    "nginx",
					"nginx.ingress.kubernetes.io/force-ssl-redirect": "true",
				},
				ingressTLS: []extensions.IngressTLS{
					{[]string{"host1", "host2"}, "secret"},
				},
			},
			expectError: false,
		},
		{
			name: "ingress with incomplete list of annotations",
			app:  app4,
			expectedIngress: SparkIngress{
				ingressName: fmt.Sprintf("%s-ui-ingress", app4.GetName()),
				ingressURL:  app3.GetName() + ".ingress.clusterName.com",
				annotations: map[string]string{
					"kubernetes.io/ingress.class":                    "nginx",
					"nginx.ingress.kubernetes.io/force-ssl-redirect": "true",
				},
				ingressTLS: []extensions.IngressTLS{
					{[]string{"host1", "host2"}, ""},
				},
			},
			expectError: true,
		},
	}
	for _, test := range testcases {
		testFn(test, t)
	}
}
