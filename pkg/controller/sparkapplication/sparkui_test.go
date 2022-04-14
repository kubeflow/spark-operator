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
	"context"
	"fmt"
	"net/url"
	"reflect"
	"testing"

	apiv1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/util"
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
		util.IngressCapabilities = map[string]bool{"networking.k8s.io/v1": true}
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
			Get(context.TODO(), sparkService.serviceName, metav1.GetOptions{})
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
		if service.Spec.Type != test.expectedService.serviceType {
			t.Errorf("%s: for service type wanted %s got %s", test.name, test.expectedService.serviceType, service.Spec.Type)
		}
		if len(service.Spec.Ports) != 1 {
			t.Errorf("%s: wanted a single port got %d ports", test.name, len(service.Spec.Ports))
		}
		port := service.Spec.Ports[0]
		if port.Port != test.expectedService.servicePort {
			t.Errorf("%s: unexpected port wanted %d got %d", test.name, test.expectedService.servicePort, port.Port)
		}
		if port.Name != test.expectedService.servicePortName {
			t.Errorf("%s: unexpected port name wanted %s got %s", test.name, test.expectedService.servicePortName, port.Name)
		}
		serviceAnnotations := service.ObjectMeta.Annotations
		if !reflect.DeepEqual(serviceAnnotations, test.expectedService.serviceAnnotations) {
			t.Errorf("%s: unexpected annotations wanted %s got %s", test.name, test.expectedService.serviceAnnotations, serviceAnnotations)
		}
	}
	defaultPort := defaultSparkWebUIPort
	defaultPortName := defaultSparkWebUIPortName
	app1 := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo1",
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
			Name:      "foo2",
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
			Name:      "foo3",
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
			Name:      "foo4",
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
	var serviceTypeNodePort apiv1.ServiceType = apiv1.ServiceTypeNodePort
	app5 := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo5",
			Namespace: "default",
			UID:       "foo-123",
		},
		Spec: v1beta2.SparkApplicationSpec{
			SparkUIOptions: &v1beta2.SparkUIConfiguration{
				ServiceType: &serviceTypeNodePort,
			},
		},
		Status: v1beta2.SparkApplicationStatus{
			SparkApplicationID: "foo-2",
			ExecutionAttempts:  2,
		},
	}
	appPortName := "http-spark-test"
	app6 := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo6",
			Namespace: "default",
			UID:       "foo-123",
		},
		Spec: v1beta2.SparkApplicationSpec{
			SparkUIOptions: &v1beta2.SparkUIConfiguration{
				ServicePort:     &appPort,
				ServicePortName: &appPortName,
			},
		},
		Status: v1beta2.SparkApplicationStatus{
			SparkApplicationID: "foo-6",
		},
	}
	app7 := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo7",
			Namespace: "default",
			UID:       "foo-123",
		},
		Spec: v1beta2.SparkApplicationSpec{
			SparkUIOptions: &v1beta2.SparkUIConfiguration{
				ServiceAnnotations: map[string]string{
					"key": "value",
				},
			},
		},
		Status: v1beta2.SparkApplicationStatus{
			SparkApplicationID: "foo-7",
			ExecutionAttempts:  1,
		},
	}
	testcases := []testcase{
		{
			name: "service with custom serviceport and serviceport and target port are same",
			app:  app1,
			expectedService: SparkService{
				serviceName:     fmt.Sprintf("%s-ui-svc", app1.GetName()),
				serviceType:     apiv1.ServiceTypeClusterIP,
				servicePortName: defaultPortName,
				servicePort:     4041,
				targetPort: intstr.IntOrString{
					Type:   intstr.Int,
					IntVal: int32(4041),
				},
			},
			expectedSelector: map[string]string{
				config.SparkAppNameLabel: "foo1",
				config.SparkRoleLabel:    config.SparkDriverRole,
			},
			expectError: false,
		},
		{
			name: "service with default port",
			app:  app2,
			expectedService: SparkService{
				serviceName:     fmt.Sprintf("%s-ui-svc", app2.GetName()),
				serviceType:     apiv1.ServiceTypeClusterIP,
				servicePortName: defaultPortName,
				servicePort:     int32(defaultPort),
			},
			expectedSelector: map[string]string{
				config.SparkAppNameLabel: "foo2",
				config.SparkRoleLabel:    config.SparkDriverRole,
			},
			expectError: false,
		},
		{
			name: "service with custom serviceport and serviceport and target port are different",
			app:  app4,
			expectedService: SparkService{
				serviceName:     fmt.Sprintf("%s-ui-svc", app4.GetName()),
				serviceType:     apiv1.ServiceTypeClusterIP,
				servicePortName: defaultPortName,
				servicePort:     80,
				targetPort: intstr.IntOrString{
					Type:   intstr.Int,
					IntVal: int32(4041),
				},
			},
			expectedSelector: map[string]string{
				config.SparkAppNameLabel: "foo4",
				config.SparkRoleLabel:    config.SparkDriverRole,
			},
			expectError: false,
		},
		{
			name: "service with custom servicetype",
			app:  app5,
			expectedService: SparkService{
				serviceName:     fmt.Sprintf("%s-ui-svc", app5.GetName()),
				serviceType:     apiv1.ServiceTypeNodePort,
				servicePortName: defaultPortName,
				servicePort:     int32(defaultPort),
			},
			expectedSelector: map[string]string{
				config.SparkAppNameLabel: "foo5",
				config.SparkRoleLabel:    config.SparkDriverRole,
			},
			expectError: false,
		},
		{
			name: "service with custom serviceportname",
			app:  app6,
			expectedService: SparkService{
				serviceName:     fmt.Sprintf("%s-ui-svc", app6.GetName()),
				serviceType:     apiv1.ServiceTypeClusterIP,
				servicePortName: "http-spark-test",
				servicePort:     int32(80),
			},
			expectedSelector: map[string]string{
				config.SparkAppNameLabel: "foo6",
				config.SparkRoleLabel:    config.SparkDriverRole,
			},
			expectError: false,
		},
		{
			name: "service with annotation",
			app:  app7,
			expectedService: SparkService{
				serviceName:     fmt.Sprintf("%s-ui-svc", app7.GetName()),
				serviceType:     apiv1.ServiceTypeClusterIP,
				servicePortName: defaultPortName,
				servicePort:     defaultPort,
				serviceAnnotations: map[string]string{
					"key": "value",
				},
				targetPort: intstr.IntOrString{
					Type:   intstr.Int,
					IntVal: int32(4041),
				},
			},
			expectedSelector: map[string]string{
				config.SparkAppNameLabel: "foo7",
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

	testFn := func(test testcase, t *testing.T, ingressURLFormat string, ingressClassName string) {
		fakeClient := fake.NewSimpleClientset()
		sparkService, err := createSparkUIService(test.app, fakeClient)
		if err != nil {
			t.Fatal(err)
		}
		ingressURL, err := getSparkUIingressURL(ingressURLFormat, test.app.Name, test.app.Namespace)
		if err != nil {
			t.Fatal(err)
		}
		sparkIngress, err := createSparkUIIngress(test.app, *sparkService, ingressURL, ingressClassName, fakeClient)
		if err != nil {
			if test.expectError {
				return
			}
			t.Fatal(err)
		}
		if sparkIngress.ingressName != test.expectedIngress.ingressName {
			t.Errorf("Ingress name wanted %s got %s", test.expectedIngress.ingressName, sparkIngress.ingressName)
		}
		if sparkIngress.ingressURL.String() != test.expectedIngress.ingressURL.String() {
			t.Errorf("Ingress URL wanted %s got %s", test.expectedIngress.ingressURL, sparkIngress.ingressURL)
		}
		ingress, err := fakeClient.NetworkingV1().Ingresses(test.app.Namespace).
			Get(context.TODO(), sparkIngress.ingressName, metav1.GetOptions{})
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
		// If we have a path, then the ingress adds capture groups
		if ingressRule.IngressRuleValue.HTTP.Paths[0].Path != "" && ingressRule.IngressRuleValue.HTTP.Paths[0].Path != "/" {
			test.expectedIngress.ingressURL.Path = test.expectedIngress.ingressURL.Path + "(/|$)(.*)"
		}
		if ingressRule.Host+ingressRule.IngressRuleValue.HTTP.Paths[0].Path != test.expectedIngress.ingressURL.Host+test.expectedIngress.ingressURL.Path {

			t.Errorf("Ingress of app %s has the wrong host %s", ingressRule.Host+ingressRule.IngressRuleValue.HTTP.Paths[0].Path, test.expectedIngress.ingressURL.Host+test.expectedIngress.ingressURL.Path)
		}

		if len(ingressRule.IngressRuleValue.HTTP.Paths) != 1 {
			t.Errorf("No Ingress paths found.")
		}
		ingressPath := ingressRule.IngressRuleValue.HTTP.Paths[0]
		if ingressPath.Backend.Service.Name != sparkService.serviceName {
			t.Errorf("Service name wanted %s got %s", sparkService.serviceName, ingressPath.Backend.Service.Name)
		}
		if *ingressPath.PathType != networkingv1.PathTypeImplementationSpecific {
			t.Errorf("PathType wanted %s got %s", networkingv1.PathTypeImplementationSpecific, *ingressPath.PathType)
		}
		if ingressPath.Backend.Service.Port.Number != sparkService.servicePort {
			t.Errorf("Service port wanted %v got %v", sparkService.servicePort, ingressPath.Backend.Service.Port.Number)
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
				IngressTLS: []networkingv1.IngressTLS{
					{Hosts: []string{"host1", "host2"}, SecretName: "secret"},
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
				IngressTLS: []networkingv1.IngressTLS{
					{Hosts: []string{"host1", "host2"}, SecretName: ""},
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
				ingressURL:  parseURLAndAssertError(app1.GetName()+".ingress.clusterName.com", t),
			},
			expectError: false,
		},
		{
			name: "ingress with annotations and without tls configuration",
			app:  app2,
			expectedIngress: SparkIngress{
				ingressName: fmt.Sprintf("%s-ui-ingress", app2.GetName()),
				ingressURL:  parseURLAndAssertError(app2.GetName()+".ingress.clusterName.com", t),
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
				ingressURL:  parseURLAndAssertError(app3.GetName()+".ingress.clusterName.com", t),
				annotations: map[string]string{
					"kubernetes.io/ingress.class":                    "nginx",
					"nginx.ingress.kubernetes.io/force-ssl-redirect": "true",
				},
				ingressTLS: []networkingv1.IngressTLS{
					{Hosts: []string{"host1", "host2"}, SecretName: "secret"},
				},
			},
			expectError: false,
		},
		{
			name: "ingress with incomplete list of annotations",
			app:  app4,
			expectedIngress: SparkIngress{
				ingressName: fmt.Sprintf("%s-ui-ingress", app4.GetName()),
				ingressURL:  parseURLAndAssertError(app3.GetName()+".ingress.clusterName.com", t),
				annotations: map[string]string{
					"kubernetes.io/ingress.class":                    "nginx",
					"nginx.ingress.kubernetes.io/force-ssl-redirect": "true",
				},
				ingressTLS: []networkingv1.IngressTLS{
					{Hosts: []string{"host1", "host2"}, SecretName: ""},
				},
			},
			expectError: true,
		},
	}

	for _, test := range testcases {
		testFn(test, t, "{{$appName}}.ingress.clusterName.com", "")
	}

	testcases = []testcase{
		{
			name: "simple ingress object with ingress URL Format with path",
			app:  app1,
			expectedIngress: SparkIngress{
				ingressName: fmt.Sprintf("%s-ui-ingress", app1.GetName()),
				ingressURL:  parseURLAndAssertError("ingress.clusterName.com/"+app1.GetNamespace()+"/"+app1.GetName(), t),
				annotations: map[string]string{
					"nginx.ingress.kubernetes.io/rewrite-target": "/$2",
				},
			},
			expectError: false,
		},
	}

	for _, test := range testcases {
		testFn(test, t, "ingress.clusterName.com/{{$appNamespace}}/{{$appName}}", "")
	}

	testcases = []testcase{
		{
			name: "simple ingress object with ingressClassName set",
			app:  app1,
			expectedIngress: SparkIngress{
				ingressName:      fmt.Sprintf("%s-ui-ingress", app1.GetName()),
				ingressURL:       parseURLAndAssertError(app1.GetName()+".ingress.clusterName.com", t),
				ingressClassName: "nginx",
			},
			expectError: false,
		},
	}
	for _, test := range testcases {
		testFn(test, t, "{{$appName}}.ingress.clusterName.com", "nginx")
	}
}

func parseURLAndAssertError(testURL string, t *testing.T) *url.URL {
	fallbackURL, _ := url.Parse("http://example.com")
	parsedURL, err := url.Parse(testURL)
	if err != nil {
		t.Errorf("failed to parse the url: %s", testURL)
		return fallbackURL
	}
	if parsedURL.Scheme == "" {
		//url does not contain any scheme, adding http:// so url.Parse can function correctly
		parsedURL, err = url.Parse("http://" + testURL)
		if err != nil {
			t.Errorf("failed to parse the url: %s", testURL)
			return fallbackURL
		}
	}
	return parsedURL
}
