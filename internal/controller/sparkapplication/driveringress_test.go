/*
Copyright 2024 spark-operator contributors

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

// func TestCreateDriverIngressService(t *testing.T) {
// 	type testcase struct {
// 		name             string
// 		app              *v1beta2.SparkApplication
// 		expectedServices []SparkService
// 		expectedSelector map[string]string
// 		expectError      bool
// 	}
// 	testFn := func(test testcase, t *testing.T) {
// 		fakeClient := fake.NewSimpleClientset()
// 		util.IngressCapabilities = map[string]bool{"networking.k8s.io/v1": true}
// 		if len(test.expectedServices) != len(test.app.Spec.DriverIngressOptions) {
// 			t.Errorf("%s: size of test.expectedServices (%d) and test.app.Spec.DriverIngressOptions (%d) is different for %s",
// 				test.name, len(test.expectedServices), len(test.app.Spec.DriverIngressOptions), test.app.Name)
// 		}
// 		for i, driverIngressConfiguration := range test.app.Spec.DriverIngressOptions {
// 			sparkService, err := createDriverIngressServiceFromConfiguration(test.app, &driverIngressConfiguration, fakeClient)
// 			if err != nil {
// 				if test.expectError {
// 					return
// 				}
// 				t.Fatal(err)
// 			}
// 			expectedService := test.expectedServices[i]
// 			if sparkService.serviceName != expectedService.serviceName {
// 				t.Errorf("%s: for service name wanted %s got %s", test.name, expectedService.serviceName, sparkService.serviceName)
// 			}
// 			service, err := fakeClient.CoreV1().
// 				Services(test.app.Namespace).
// 				Get(context.TODO(), sparkService.serviceName, metav1.GetOptions{})
// 			if err != nil {
// 				if test.expectError {
// 					return
// 				}
// 				t.Fatal(err)
// 			}
// 			if service.Labels[common.SparkAppNameLabel] != test.app.Name {
// 				t.Errorf("%s: service of app %s has the wrong labels", test.name, test.app.Name)
// 			}
// 			if !reflect.DeepEqual(test.expectedSelector, service.Spec.Selector) {
// 				t.Errorf("%s: for label selector wanted %s got %s", test.name, test.expectedSelector, service.Spec.Selector)
// 			}
// 			if service.Spec.Type != expectedService.serviceType {
// 				t.Errorf("%s: for service type wanted %s got %s", test.name, expectedService.serviceType, service.Spec.Type)
// 			}
// 			if len(service.Spec.Ports) != 1 {
// 				t.Errorf("%s: wanted a single port got %d ports", test.name, len(service.Spec.Ports))
// 			}
// 			port := service.Spec.Ports[0]
// 			if port.Port != expectedService.servicePort {
// 				t.Errorf("%s: unexpected port wanted %d got %d", test.name, expectedService.servicePort, port.Port)
// 			}
// 			if port.Name != expectedService.servicePortName {
// 				t.Errorf("%s: unexpected port name wanted %s got %s", test.name, expectedService.servicePortName, port.Name)
// 			}
// 			serviceAnnotations := service.ObjectMeta.Annotations
// 			if !reflect.DeepEqual(serviceAnnotations, expectedService.serviceAnnotations) {
// 				t.Errorf("%s: unexpected annotations wanted %s got %s", test.name, expectedService.serviceAnnotations, serviceAnnotations)
// 			}
// 			serviceLabels := service.ObjectMeta.Labels
// 			if !reflect.DeepEqual(serviceLabels, expectedService.serviceLabels) {
// 				t.Errorf("%s: unexpected labels wanted %s got %s", test.name, expectedService.serviceLabels, serviceLabels)
// 			}
// 		}
// 	}
// 	serviceNameFormat := "%s-driver-%d"
// 	portNameFormat := "driver-ing-%d"
// 	app1 := &v1beta2.SparkApplication{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Name:      "foo1",
// 			Namespace: "default",
// 			UID:       "foo-123",
// 		},
// 		Spec: v1beta2.SparkApplicationSpec{
// 			DriverIngressOptions: []v1beta2.DriverIngressConfiguration{
// 				{
// 					ServicePort: util.Int32Ptr(8888),
// 				},
// 			},
// 		},
// 		Status: v1beta2.SparkApplicationStatus{
// 			SparkApplicationID: "foo-1",
// 			ExecutionAttempts:  1,
// 		},
// 	}
// 	app2 := &v1beta2.SparkApplication{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Name:      "foo2",
// 			Namespace: "default",
// 			UID:       "foo-123",
// 		},
// 		Spec: v1beta2.SparkApplicationSpec{
// 			DriverIngressOptions: []v1beta2.DriverIngressConfiguration{
// 				{
// 					ServicePort: util.Int32Ptr(8888),
// 				},
// 			},
// 		},
// 		Status: v1beta2.SparkApplicationStatus{
// 			SparkApplicationID: "foo-2",
// 			ExecutionAttempts:  2,
// 		},
// 	}
// 	app3 := &v1beta2.SparkApplication{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Name:      "foo3",
// 			Namespace: "default",
// 			UID:       "foo-123",
// 		},
// 		Spec: v1beta2.SparkApplicationSpec{
// 			DriverIngressOptions: []v1beta2.DriverIngressConfiguration{
// 				{
// 					ServicePort: nil,
// 				},
// 			},
// 		},
// 		Status: v1beta2.SparkApplicationStatus{
// 			SparkApplicationID: "foo-3",
// 		},
// 	}
// 	var appPort int32 = 80
// 	app4 := &v1beta2.SparkApplication{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Name:      "foo4",
// 			Namespace: "default",
// 			UID:       "foo-123",
// 		},
// 		Spec: v1beta2.SparkApplicationSpec{
// 			DriverIngressOptions: []v1beta2.DriverIngressConfiguration{
// 				{
// 					ServicePort: &appPort,
// 				},
// 			},
// 			SparkConf: map[string]string{
// 				sparkUIPortConfigurationKey: "4041",
// 			},
// 		},
// 		Status: v1beta2.SparkApplicationStatus{
// 			SparkApplicationID: "foo-3",
// 		},
// 	}
// 	var serviceTypeNodePort apiv1.ServiceType = apiv1.ServiceTypeNodePort
// 	app5 := &v1beta2.SparkApplication{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Name:      "foo5",
// 			Namespace: "default",
// 			UID:       "foo-123",
// 		},
// 		Spec: v1beta2.SparkApplicationSpec{
// 			DriverIngressOptions: []v1beta2.DriverIngressConfiguration{
// 				{
// 					ServicePort: util.Int32Ptr(8888),
// 					ServiceType: &serviceTypeNodePort,
// 				},
// 			},
// 		},
// 		Status: v1beta2.SparkApplicationStatus{
// 			SparkApplicationID: "foo-2",
// 			ExecutionAttempts:  2,
// 		},
// 	}
// 	appPortName := "http-spark-test"
// 	app6 := &v1beta2.SparkApplication{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Name:      "foo6",
// 			Namespace: "default",
// 			UID:       "foo-123",
// 		},
// 		Spec: v1beta2.SparkApplicationSpec{
// 			DriverIngressOptions: []v1beta2.DriverIngressConfiguration{
// 				{
// 					ServicePort:     &appPort,
// 					ServicePortName: &appPortName,
// 				},
// 			},
// 		},
// 		Status: v1beta2.SparkApplicationStatus{
// 			SparkApplicationID: "foo-6",
// 		},
// 	}
// 	app7 := &v1beta2.SparkApplication{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Name:      "foo7",
// 			Namespace: "default",
// 			UID:       "foo-123",
// 		},
// 		Spec: v1beta2.SparkApplicationSpec{
// 			DriverIngressOptions: []v1beta2.DriverIngressConfiguration{
// 				{
// 					ServicePort: util.Int32Ptr(8888),
// 					ServiceAnnotations: map[string]string{
// 						"key": "value",
// 					},
// 				},
// 			},
// 		},
// 		Status: v1beta2.SparkApplicationStatus{
// 			SparkApplicationID: "foo-7",
// 			ExecutionAttempts:  1,
// 		},
// 	}
// 	app8 := &v1beta2.SparkApplication{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Name:      "foo8",
// 			Namespace: "default",
// 			UID:       "foo-123",
// 		},
// 		Spec: v1beta2.SparkApplicationSpec{
// 			DriverIngressOptions: []v1beta2.DriverIngressConfiguration{
// 				{
// 					ServicePort: util.Int32Ptr(8888),
// 					ServiceLabels: map[string]string{
// 						"sparkoperator.k8s.io/app-name": "foo8",
// 						"key":                           "value",
// 					},
// 				},
// 			},
// 		},
// 		Status: v1beta2.SparkApplicationStatus{
// 			SparkApplicationID: "foo-8",
// 			ExecutionAttempts:  1,
// 		},
// 	}
// 	testcases := []testcase{
// 		{
// 			name: "service with custom serviceport and serviceport and target port are same",
// 			app:  app1,
// 			expectedServices: []SparkService{
// 				{
// 					serviceName:     fmt.Sprintf(serviceNameFormat, app1.GetName(), *app1.Spec.DriverIngressOptions[0].ServicePort),
// 					serviceType:     apiv1.ServiceTypeClusterIP,
// 					servicePortName: fmt.Sprintf(portNameFormat, *app1.Spec.DriverIngressOptions[0].ServicePort),
// 					servicePort:     *app1.Spec.DriverIngressOptions[0].ServicePort,
// 					serviceLabels: map[string]string{
// 						"sparkoperator.k8s.io/app-name": "foo1",
// 					},
// 					targetPort: intstr.IntOrString{
// 						Type:   intstr.Int,
// 						IntVal: int32(*app1.Spec.DriverIngressOptions[0].ServicePort),
// 					},
// 				},
// 			},
// 			expectedSelector: map[string]string{
// 				common.SparkAppNameLabel: "foo1",
// 				common.SparkRoleLabel:    common.SparkDriverRole,
// 			},
// 			expectError: false,
// 		},
// 		{
// 			name: "service with default port",
// 			app:  app2,
// 			expectedServices: []SparkService{
// 				{
// 					serviceName:     fmt.Sprintf(serviceNameFormat, app2.GetName(), *app2.Spec.DriverIngressOptions[0].ServicePort),
// 					serviceType:     apiv1.ServiceTypeClusterIP,
// 					servicePortName: fmt.Sprintf(portNameFormat, *app2.Spec.DriverIngressOptions[0].ServicePort),
// 					servicePort:     int32(*app2.Spec.DriverIngressOptions[0].ServicePort),
// 					serviceLabels: map[string]string{
// 						"sparkoperator.k8s.io/app-name": "foo2",
// 					},
// 				},
// 			},
// 			expectedSelector: map[string]string{
// 				common.SparkAppNameLabel: "foo2",
// 				common.SparkRoleLabel:    common.SparkDriverRole,
// 			},
// 			expectError: false,
// 		},
// 		{
// 			name: "service with custom serviceport and serviceport and target port are different",
// 			app:  app4,
// 			expectedServices: []SparkService{
// 				{
// 					serviceName:     fmt.Sprintf(serviceNameFormat, app4.GetName(), *app4.Spec.DriverIngressOptions[0].ServicePort),
// 					serviceType:     apiv1.ServiceTypeClusterIP,
// 					servicePortName: fmt.Sprintf(portNameFormat, *app4.Spec.DriverIngressOptions[0].ServicePort),
// 					servicePort:     80,
// 					serviceLabels: map[string]string{
// 						"sparkoperator.k8s.io/app-name": "foo4",
// 					},
// 					targetPort: intstr.IntOrString{
// 						Type:   intstr.Int,
// 						IntVal: int32(4041),
// 					},
// 				},
// 			},
// 			expectedSelector: map[string]string{
// 				common.SparkAppNameLabel: "foo4",
// 				common.SparkRoleLabel:    common.SparkDriverRole,
// 			},
// 			expectError: false,
// 		},
// 		{
// 			name: "service with custom servicetype",
// 			app:  app5,
// 			expectedServices: []SparkService{
// 				{
// 					serviceName:     fmt.Sprintf(serviceNameFormat, app5.GetName(), *app5.Spec.DriverIngressOptions[0].ServicePort),
// 					serviceType:     apiv1.ServiceTypeNodePort,
// 					servicePortName: fmt.Sprintf(portNameFormat, *app5.Spec.DriverIngressOptions[0].ServicePort),
// 					servicePort:     *app5.Spec.DriverIngressOptions[0].ServicePort,
// 					serviceLabels: map[string]string{
// 						"sparkoperator.k8s.io/app-name": "foo5",
// 					},
// 				},
// 			},
// 			expectedSelector: map[string]string{
// 				common.SparkAppNameLabel: "foo5",
// 				common.SparkRoleLabel:    common.SparkDriverRole,
// 			},
// 			expectError: false,
// 		},
// 		{
// 			name: "service with custom serviceportname",
// 			app:  app6,
// 			expectedServices: []SparkService{
// 				{
// 					serviceName:     fmt.Sprintf(serviceNameFormat, app6.GetName(), *app6.Spec.DriverIngressOptions[0].ServicePort),
// 					serviceType:     apiv1.ServiceTypeClusterIP,
// 					servicePortName: "http-spark-test",
// 					servicePort:     int32(80),
// 					serviceLabels: map[string]string{
// 						"sparkoperator.k8s.io/app-name": "foo6",
// 					},
// 				},
// 			},
// 			expectedSelector: map[string]string{
// 				common.SparkAppNameLabel: "foo6",
// 				common.SparkRoleLabel:    common.SparkDriverRole,
// 			},
// 			expectError: false,
// 		},
// 		{
// 			name: "service with annotation",
// 			app:  app7,
// 			expectedServices: []SparkService{
// 				{
// 					serviceName:     fmt.Sprintf(serviceNameFormat, app7.GetName(), *app7.Spec.DriverIngressOptions[0].ServicePort),
// 					serviceType:     apiv1.ServiceTypeClusterIP,
// 					servicePortName: fmt.Sprintf(portNameFormat, *app7.Spec.DriverIngressOptions[0].ServicePort),
// 					servicePort:     *app7.Spec.DriverIngressOptions[0].ServicePort,
// 					serviceAnnotations: map[string]string{
// 						"key": "value",
// 					},
// 					serviceLabels: map[string]string{
// 						"sparkoperator.k8s.io/app-name": "foo7",
// 					},
// 					targetPort: intstr.IntOrString{
// 						Type:   intstr.Int,
// 						IntVal: int32(4041),
// 					},
// 				},
// 			},
// 			expectedSelector: map[string]string{
// 				common.SparkAppNameLabel: "foo7",
// 				common.SparkRoleLabel:    common.SparkDriverRole,
// 			},
// 			expectError: false,
// 		},
// 		{
// 			name: "service with custom labels",
// 			app:  app8,
// 			expectedServices: []SparkService{
// 				{
// 					serviceName:     fmt.Sprintf(serviceNameFormat, app8.GetName(), *app8.Spec.DriverIngressOptions[0].ServicePort),
// 					serviceType:     apiv1.ServiceTypeClusterIP,
// 					servicePortName: fmt.Sprintf(portNameFormat, *app8.Spec.DriverIngressOptions[0].ServicePort),
// 					servicePort:     *app8.Spec.DriverIngressOptions[0].ServicePort,
// 					serviceLabels: map[string]string{
// 						"sparkoperator.k8s.io/app-name": "foo8",
// 						"key":                           "value",
// 					},
// 					targetPort: intstr.IntOrString{
// 						Type:   intstr.Int,
// 						IntVal: int32(4041),
// 					},
// 				},
// 			},
// 			expectedSelector: map[string]string{
// 				common.SparkAppNameLabel: "foo8",
// 				common.SparkRoleLabel:    common.SparkDriverRole,
// 			},
// 			expectError: false,
// 		},
// 		{
// 			name:             "service with bad port configurations",
// 			app:              app3,
// 			expectError:      true,
// 			expectedServices: []SparkService{{}},
// 		},
// 	}
// 	for _, test := range testcases {
// 		testFn(test, t)
// 	}
// }

// func TestCreateDriverIngress(t *testing.T) {
// 	type testcase struct {
// 		name              string
// 		app               *v1beta2.SparkApplication
// 		expectedIngresses []SparkIngress
// 		expectError       bool
// 	}

// 	testFn := func(test testcase, t *testing.T, ingressURLFormat string, ingressClassName string) {
// 		fakeClient := fake.NewSimpleClientset()
// 		if len(test.expectedIngresses) != len(test.app.Spec.DriverIngressOptions) {
// 			t.Errorf("%s: size of test.expectedIngresses (%d) and test.app.Spec.DriverIngressOptions (%d) is different for %s",
// 				test.name, len(test.expectedIngresses), len(test.app.Spec.DriverIngressOptions), test.app.Name)
// 		}
// 		for i, driverIngressConfiguration := range test.app.Spec.DriverIngressOptions {
// 			sparkService, err := createDriverIngressServiceFromConfiguration(test.app, &driverIngressConfiguration, fakeClient)
// 			if err != nil {
// 				t.Fatal(err)
// 			}
// 			ingressURL, err := getDriverIngressURL(ingressURLFormat, test.app.Name, test.app.Namespace)
// 			if err != nil {
// 				t.Fatal(err)
// 			}
// 			sparkIngress, err := createDriverIngress(test.app, &driverIngressConfiguration, *sparkService, ingressURL, ingressClassName, fakeClient)
// 			if err != nil {
// 				if test.expectError {
// 					return
// 				}
// 				t.Fatal(err)
// 			}
// 			expectedIngress := test.expectedIngresses[i]
// 			if sparkIngress.ingressName != expectedIngress.ingressName {
// 				t.Errorf("Ingress name wanted %s got %s", expectedIngress.ingressName, sparkIngress.ingressName)
// 			}
// 			if sparkIngress.ingressURL.String() != expectedIngress.ingressURL.String() {
// 				t.Errorf("Ingress URL wanted %s got %s", expectedIngress.ingressURL, sparkIngress.ingressURL)
// 			}
// 			ingress, err := fakeClient.NetworkingV1().Ingresses(test.app.Namespace).
// 				Get(context.TODO(), sparkIngress.ingressName, metav1.GetOptions{})
// 			if err != nil {
// 				t.Fatal(err)
// 			}
// 			if len(ingress.Annotations) != 0 {
// 				for key, value := range ingress.Annotations {
// 					if expectedIngress.annotations[key] != ingress.Annotations[key] {
// 						t.Errorf("Expected annotation: %s=%s but found : %s=%s", key, value, key, ingress.Annotations[key])
// 					}
// 				}
// 			}
// 			if len(ingress.Spec.TLS) != 0 {
// 				for _, ingressTls := range ingress.Spec.TLS {
// 					if ingressTls.Hosts[0] != expectedIngress.ingressTLS[0].Hosts[0] {
// 						t.Errorf("Expected ingressTls host: %s but found : %s", expectedIngress.ingressTLS[0].Hosts[0], ingressTls.Hosts[0])
// 					}
// 					if ingressTls.SecretName != expectedIngress.ingressTLS[0].SecretName {
// 						t.Errorf("Expected ingressTls secretName: %s but found : %s", expectedIngress.ingressTLS[0].SecretName, ingressTls.SecretName)
// 					}
// 				}
// 			}
// 			if ingress.Labels[common.SparkAppNameLabel] != test.app.Name {
// 				t.Errorf("Ingress of app %s has the wrong labels", test.app.Name)
// 			}

// 			if len(ingress.Spec.Rules) != 1 {
// 				t.Errorf("No Ingress rules found.")
// 			}
// 			ingressRule := ingress.Spec.Rules[0]
// 			// If we have a path, then the ingress adds capture groups
// 			if ingressRule.IngressRuleValue.HTTP.Paths[0].Path != "" && ingressRule.IngressRuleValue.HTTP.Paths[0].Path != "/" {
// 				expectedIngress.ingressURL.Path = expectedIngress.ingressURL.Path + "(/|$)(.*)"
// 			}
// 			if ingressRule.Host+ingressRule.IngressRuleValue.HTTP.Paths[0].Path != expectedIngress.ingressURL.Host+expectedIngress.ingressURL.Path {
// 				t.Errorf("Ingress of app %s has the wrong host %s", ingressRule.Host+ingressRule.IngressRuleValue.HTTP.Paths[0].Path, expectedIngress.ingressURL.Host+expectedIngress.ingressURL.Path)
// 			}

// 			if len(ingressRule.IngressRuleValue.HTTP.Paths) != 1 {
// 				t.Errorf("No Ingress paths found.")
// 			}
// 			ingressPath := ingressRule.IngressRuleValue.HTTP.Paths[0]
// 			if ingressPath.Backend.Service.Name != sparkService.serviceName {
// 				t.Errorf("Service name wanted %s got %s", sparkService.serviceName, ingressPath.Backend.Service.Name)
// 			}
// 			if *ingressPath.PathType != networkingv1.PathTypeImplementationSpecific {
// 				t.Errorf("PathType wanted %s got %s", networkingv1.PathTypeImplementationSpecific, *ingressPath.PathType)
// 			}
// 			if ingressPath.Backend.Service.Port.Number != sparkService.servicePort {
// 				t.Errorf("Service port wanted %v got %v", sparkService.servicePort, ingressPath.Backend.Service.Port.Number)
// 			}
// 		}
// 	}

// 	ingressNameFormat := "%s-ing-%d"
// 	var appPort int32 = 80
// 	app1 := &v1beta2.SparkApplication{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Name:      "foo",
// 			Namespace: "default",
// 			UID:       "foo-123",
// 		},
// 		Spec: v1beta2.SparkApplicationSpec{
// 			DriverIngressOptions: []v1beta2.DriverIngressConfiguration{
// 				{
// 					ServicePort: &appPort,
// 				},
// 			},
// 		},
// 		Status: v1beta2.SparkApplicationStatus{
// 			SparkApplicationID: "foo-1",
// 			DriverInfo: v1beta2.DriverInfo{
// 				WebUIServiceName: "blah-service",
// 			},
// 		},
// 	}
// 	app2 := &v1beta2.SparkApplication{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Name:      "foo",
// 			Namespace: "default",
// 			UID:       "foo-123",
// 		},
// 		Spec: v1beta2.SparkApplicationSpec{
// 			DriverIngressOptions: []v1beta2.DriverIngressConfiguration{
// 				{
// 					ServicePort: &appPort,
// 					IngressAnnotations: map[string]string{
// 						"kubernetes.io/ingress.class":                    "nginx",
// 						"nginx.ingress.kubernetes.io/force-ssl-redirect": "true",
// 					},
// 				},
// 			},
// 		},
// 		Status: v1beta2.SparkApplicationStatus{
// 			SparkApplicationID: "foo-1",
// 			DriverInfo: v1beta2.DriverInfo{
// 				WebUIServiceName: "blah-service",
// 			},
// 		},
// 	}
// 	app3 := &v1beta2.SparkApplication{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Name:      "foo",
// 			Namespace: "default",
// 			UID:       "foo-123",
// 		},
// 		Spec: v1beta2.SparkApplicationSpec{
// 			DriverIngressOptions: []v1beta2.DriverIngressConfiguration{
// 				{
// 					ServicePort: &appPort,
// 					IngressAnnotations: map[string]string{
// 						"kubernetes.io/ingress.class":                    "nginx",
// 						"nginx.ingress.kubernetes.io/force-ssl-redirect": "true",
// 					},
// 					IngressTLS: []networkingv1.IngressTLS{
// 						{Hosts: []string{"host1", "host2"}, SecretName: "secret"},
// 					},
// 				},
// 			},
// 		},
// 		Status: v1beta2.SparkApplicationStatus{
// 			SparkApplicationID: "foo-1",
// 			DriverInfo: v1beta2.DriverInfo{
// 				WebUIServiceName: "blah-service",
// 			},
// 		},
// 	}
// 	app4 := &v1beta2.SparkApplication{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Name:      "foo",
// 			Namespace: "default",
// 			UID:       "foo-123",
// 		},
// 		Spec: v1beta2.SparkApplicationSpec{
// 			DriverIngressOptions: []v1beta2.DriverIngressConfiguration{
// 				{
// 					ServicePort: &appPort,
// 					IngressAnnotations: map[string]string{
// 						"kubernetes.io/ingress.class": "nginx",
// 					},
// 					IngressTLS: []networkingv1.IngressTLS{
// 						{Hosts: []string{"host1", "host2"}, SecretName: ""},
// 					},
// 				},
// 			},
// 		},
// 		Status: v1beta2.SparkApplicationStatus{
// 			SparkApplicationID: "foo-1",
// 			DriverInfo: v1beta2.DriverInfo{
// 				WebUIServiceName: "blah-service",
// 			},
// 		},
// 	}

// 	testcases := []testcase{
// 		{
// 			name: "simple ingress object",
// 			app:  app1,
// 			expectedIngresses: []SparkIngress{
// 				{
// 					ingressName: fmt.Sprintf(ingressNameFormat, app1.GetName(), *app1.Spec.DriverIngressOptions[0].ServicePort),
// 					ingressURL:  parseURLAndAssertError(app1.GetName()+".ingress.clusterName.com", t),
// 				},
// 			},
// 			expectError: false,
// 		},
// 		{
// 			name: "ingress with annotations and without tls configuration",
// 			app:  app2,
// 			expectedIngresses: []SparkIngress{
// 				{
// 					ingressName: fmt.Sprintf(ingressNameFormat, app2.GetName(), *app2.Spec.DriverIngressOptions[0].ServicePort),
// 					ingressURL:  parseURLAndAssertError(app2.GetName()+".ingress.clusterName.com", t),
// 					annotations: map[string]string{
// 						"kubernetes.io/ingress.class":                    "nginx",
// 						"nginx.ingress.kubernetes.io/force-ssl-redirect": "true",
// 					},
// 				},
// 			},
// 			expectError: false,
// 		},
// 		{
// 			name: "ingress with annotations and tls configuration",
// 			app:  app3,
// 			expectedIngresses: []SparkIngress{
// 				{
// 					ingressName: fmt.Sprintf(ingressNameFormat, app3.GetName(), *app3.Spec.DriverIngressOptions[0].ServicePort),
// 					ingressURL:  parseURLAndAssertError(app3.GetName()+".ingress.clusterName.com", t),
// 					annotations: map[string]string{
// 						"kubernetes.io/ingress.class":                    "nginx",
// 						"nginx.ingress.kubernetes.io/force-ssl-redirect": "true",
// 					},
// 					ingressTLS: []networkingv1.IngressTLS{
// 						{Hosts: []string{"host1", "host2"}, SecretName: "secret"},
// 					},
// 				},
// 			},
// 			expectError: false,
// 		},
// 		{
// 			name: "ingress with incomplete list of annotations",
// 			app:  app4,
// 			expectedIngresses: []SparkIngress{
// 				{
// 					ingressName: fmt.Sprintf(ingressNameFormat, app4.GetName(), *app4.Spec.DriverIngressOptions[0].ServicePort),
// 					ingressURL:  parseURLAndAssertError(app3.GetName()+".ingress.clusterName.com", t),
// 					annotations: map[string]string{
// 						"kubernetes.io/ingress.class":                    "nginx",
// 						"nginx.ingress.kubernetes.io/force-ssl-redirect": "true",
// 					},
// 					ingressTLS: []networkingv1.IngressTLS{
// 						{Hosts: []string{"host1", "host2"}, SecretName: ""},
// 					},
// 				},
// 			},
// 			expectError: true,
// 		},
// 	}

// 	for _, test := range testcases {
// 		testFn(test, t, "{{$appName}}.ingress.clusterName.com", "")
// 	}

// 	testcases = []testcase{
// 		{
// 			name: "simple ingress object with ingress URL Format with path",
// 			app:  app1,
// 			expectedIngresses: []SparkIngress{
// 				{
// 					ingressName: fmt.Sprintf(ingressNameFormat, app1.GetName(), *app1.Spec.DriverIngressOptions[0].ServicePort),
// 					ingressURL:  parseURLAndAssertError("ingress.clusterName.com/"+app1.GetNamespace()+"/"+app1.GetName(), t),
// 					annotations: map[string]string{
// 						"nginx.ingress.kubernetes.io/rewrite-target": "/$2",
// 					},
// 				},
// 			},
// 			expectError: false,
// 		},
// 	}

// 	for _, test := range testcases {
// 		testFn(test, t, "ingress.clusterName.com/{{$appNamespace}}/{{$appName}}", "")
// 	}

// 	testcases = []testcase{
// 		{
// 			name: "simple ingress object with ingressClassName set",
// 			app:  app1,
// 			expectedIngresses: []SparkIngress{
// 				{
// 					ingressName:      fmt.Sprintf(ingressNameFormat, app1.GetName(), *app1.Spec.DriverIngressOptions[0].ServicePort),
// 					ingressURL:       parseURLAndAssertError(app1.GetName()+".ingress.clusterName.com", t),
// 					ingressClassName: "nginx",
// 				},
// 			},
// 			expectError: false,
// 		},
// 	}
// 	for _, test := range testcases {
// 		testFn(test, t, "{{$appName}}.ingress.clusterName.com", "nginx")
// 	}
// }
