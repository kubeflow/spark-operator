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

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	extensionsv1beta1 "k8s.io/api/extensions/v1beta1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

func TestCreateDriverIngressService(t *testing.T) {
	type testcase struct {
		name             string
		app              *v1beta2.SparkApplication
		expectedServices []SparkService
		expectedSelector map[string]string
		expectError      bool
	}
	testFn := func(tc testcase, t *testing.T) {
		fakeClient := fake.NewFakeClient()
		reconciler := Reconciler{
			client: fakeClient,
		}
		t.Logf("test case: %s", tc.name)

		if len(tc.app.Spec.DriverIngressOptions) == 0 {
			t.Fatalf("Test case %s has no DriverIngressOptions", tc.name)
		}

		ingressOptions := tc.app.Spec.DriverIngressOptions[0]
		ingressConfig, err := reconciler.createDriverIngressServiceFromConfiguration(context.TODO(), tc.app, &ingressOptions)

		if tc.expectError {
			assert.Error(t, err, "Expected an error but got none")
			return
		}

		assert.NoError(t, err, "Failed to create driver ingress service")

		// Verify the service matches expected values
		if len(tc.expectedServices) > 0 {
			expectedService := tc.expectedServices[0]

			assert.Equal(t, expectedService.serviceName, ingressConfig.serviceName, "Service name mismatch")
			assert.Equal(t, expectedService.serviceType, ingressConfig.serviceType, "Service type mismatch")
			assert.Equal(t, expectedService.servicePort, ingressConfig.servicePort, "Service port mismatch")
			assert.Equal(t, expectedService.servicePortName, ingressConfig.servicePortName, "Service port name mismatch")

			if expectedService.targetPort.IntVal != 0 {
				assert.Equal(t, expectedService.targetPort.IntVal, ingressConfig.targetPort.IntVal, "Target port mismatch")
			}

			// Check service annotations if specified
			if len(expectedService.serviceAnnotations) > 0 {
				assert.Equal(t, expectedService.serviceAnnotations, ingressConfig.serviceAnnotations, "Service annotations mismatch")
			}

			// Check service labels if specified
			if len(expectedService.serviceLabels) > 0 {
				for k, v := range expectedService.serviceLabels {
					assert.Equal(t, v, ingressConfig.serviceLabels[k], "Service label %s mismatch", k)
				}
			}
		}

		// Test ingress creation
		driverUrl, err := url.Parse("http://localhost")
		assert.NoError(t, err, "Failed to parse driver ingress url")
		_, err = reconciler.createDriverIngress(context.TODO(), tc.app, &ingressOptions, *ingressConfig, driverUrl, "ingressClass", []networkingv1.IngressTLS{}, map[string]string{})

		if tc.expectError {
			assert.Error(t, err, "Expected an error but got none")
		} else {
			assert.NoError(t, err, "Failed to create driver ingress")
		}
	}

	serviceNameFormat := "%s-driver-%d"
	portNameFormat := "driver-ing-%d"
	app1 := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo1",
			Namespace: "default",
			UID:       "foo-123",
		},
		Spec: v1beta2.SparkApplicationSpec{
			DriverIngressOptions: []v1beta2.DriverIngressConfiguration{
				{
					ServicePort:   ptr.To[int32](8888),
					ServiceLabels: map[string]string{"foo": "bar"},
				},
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
		Spec: v1beta2.SparkApplicationSpec{
			DriverIngressOptions: []v1beta2.DriverIngressConfiguration{
				{
					ServicePort: ptr.To[int32](8888),
				},
			},
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
			DriverIngressOptions: []v1beta2.DriverIngressConfiguration{
				{
					ServicePort: nil,
				},
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
			DriverIngressOptions: []v1beta2.DriverIngressConfiguration{
				{
					ServicePort: ptr.To[int32](4041),
				},
			},
		},
		Status: v1beta2.SparkApplicationStatus{
			SparkApplicationID: "foo-3",
		},
	}
	var serviceTypeNodePort = corev1.ServiceTypeNodePort
	app5 := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo5",
			Namespace: "default",
			UID:       "foo-123",
		},
		Spec: v1beta2.SparkApplicationSpec{
			DriverIngressOptions: []v1beta2.DriverIngressConfiguration{
				{
					ServicePort: ptr.To[int32](8888),
					ServiceType: &serviceTypeNodePort,
				},
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
			DriverIngressOptions: []v1beta2.DriverIngressConfiguration{
				{
					ServicePort:     &appPort,
					ServicePortName: &appPortName,
				},
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
			DriverIngressOptions: []v1beta2.DriverIngressConfiguration{
				{
					ServicePort: ptr.To[int32](8888),
					ServiceAnnotations: map[string]string{
						"key": "value",
					},
				},
			},
		},
		Status: v1beta2.SparkApplicationStatus{
			SparkApplicationID: "foo-7",
			ExecutionAttempts:  1,
		},
	}
	app8 := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo8",
			Namespace: "default",
			UID:       "foo-123",
		},
		Spec: v1beta2.SparkApplicationSpec{
			DriverIngressOptions: []v1beta2.DriverIngressConfiguration{
				{
					ServicePort: ptr.To[int32](8888),
					ServiceLabels: map[string]string{
						"foo": "bar",
					},
				},
			},
		},
		Status: v1beta2.SparkApplicationStatus{
			SparkApplicationID: "foo-8",
			ExecutionAttempts:  1,
		},
	}
	testcases := []testcase{
		{
			name: "service with custom serviceport and serviceport and target port are same",
			app:  app1,
			expectedServices: []SparkService{
				{
					serviceName:     fmt.Sprintf(serviceNameFormat, app1.GetName(), *app1.Spec.DriverIngressOptions[0].ServicePort),
					serviceType:     corev1.ServiceTypeClusterIP,
					servicePortName: fmt.Sprintf(portNameFormat, *app1.Spec.DriverIngressOptions[0].ServicePort),
					servicePort:     *app1.Spec.DriverIngressOptions[0].ServicePort,
					serviceLabels: map[string]string{
						"foo": "bar",
					},
					targetPort: intstr.IntOrString{
						Type:   intstr.Int,
						IntVal: *app1.Spec.DriverIngressOptions[0].ServicePort,
					},
				},
			},
			expectedSelector: map[string]string{
				common.LabelSparkAppName: "foo1",
				common.LabelSparkRole:    common.SparkRoleDriver,
			},
			expectError: false,
		},
		{
			name: "service with default port",
			app:  app2,
			expectedServices: []SparkService{
				{
					serviceName:     fmt.Sprintf(serviceNameFormat, app2.GetName(), *app2.Spec.DriverIngressOptions[0].ServicePort),
					serviceType:     corev1.ServiceTypeClusterIP,
					servicePortName: fmt.Sprintf(portNameFormat, *app2.Spec.DriverIngressOptions[0].ServicePort),
					servicePort:     *app2.Spec.DriverIngressOptions[0].ServicePort,
				},
			},
			expectedSelector: map[string]string{
				common.LabelSparkAppName: "foo2",
				common.LabelSparkRole:    common.SparkRoleDriver,
			},
			expectError: false,
		},
		{
			name: "service with custom serviceport and serviceport and target port are different",
			app:  app4,
			expectedServices: []SparkService{
				{
					serviceName:     fmt.Sprintf(serviceNameFormat, app4.GetName(), *app4.Spec.DriverIngressOptions[0].ServicePort),
					serviceType:     corev1.ServiceTypeClusterIP,
					servicePortName: fmt.Sprintf(portNameFormat, *app4.Spec.DriverIngressOptions[0].ServicePort),
					servicePort:     4041,
					targetPort: intstr.IntOrString{
						Type:   intstr.Int,
						IntVal: int32(4041),
					},
				},
			},
			expectedSelector: map[string]string{
				common.LabelSparkAppName: "foo4",
				common.LabelSparkRole:    common.SparkRoleDriver,
			},
			expectError: false,
		},
		{
			name: "service with custom servicetype",
			app:  app5,
			expectedServices: []SparkService{
				{
					serviceName:     fmt.Sprintf(serviceNameFormat, app5.GetName(), *app5.Spec.DriverIngressOptions[0].ServicePort),
					serviceType:     corev1.ServiceTypeNodePort,
					servicePortName: fmt.Sprintf(portNameFormat, *app5.Spec.DriverIngressOptions[0].ServicePort),
					servicePort:     *app5.Spec.DriverIngressOptions[0].ServicePort,
				},
			},
			expectedSelector: map[string]string{
				common.LabelSparkAppName: "foo5",
				common.LabelSparkRole:    common.SparkRoleDriver,
			},
			expectError: false,
		},
		{
			name: "service with custom serviceportname",
			app:  app6,
			expectedServices: []SparkService{
				{
					serviceName:     fmt.Sprintf(serviceNameFormat, app6.GetName(), *app6.Spec.DriverIngressOptions[0].ServicePort),
					serviceType:     corev1.ServiceTypeClusterIP,
					servicePortName: "http-spark-test",
					servicePort:     int32(80),
				},
			},
			expectedSelector: map[string]string{
				common.LabelSparkAppName: "foo6",
				common.LabelSparkRole:    common.SparkRoleDriver,
			},
			expectError: false,
		},
		{
			name: "service with annotation",
			app:  app7,
			expectedServices: []SparkService{
				{
					serviceName:     fmt.Sprintf(serviceNameFormat, app7.GetName(), *app7.Spec.DriverIngressOptions[0].ServicePort),
					serviceType:     corev1.ServiceTypeClusterIP,
					servicePortName: fmt.Sprintf(portNameFormat, *app7.Spec.DriverIngressOptions[0].ServicePort),
					servicePort:     *app7.Spec.DriverIngressOptions[0].ServicePort,
					serviceAnnotations: map[string]string{
						"key": "value",
					},
					targetPort: intstr.IntOrString{
						Type:   intstr.Int,
						IntVal: int32(8888),
					},
				},
			},
			expectedSelector: map[string]string{
				common.LabelSparkAppName: "foo7",
				common.LabelSparkRole:    common.SparkRoleDriver,
			},
			expectError: false,
		},
		{
			name: "service with custom labels",
			app:  app8,
			expectedServices: []SparkService{
				{
					serviceName:     fmt.Sprintf(serviceNameFormat, app8.GetName(), *app8.Spec.DriverIngressOptions[0].ServicePort),
					serviceType:     corev1.ServiceTypeClusterIP,
					servicePortName: fmt.Sprintf(portNameFormat, *app8.Spec.DriverIngressOptions[0].ServicePort),
					servicePort:     *app8.Spec.DriverIngressOptions[0].ServicePort,
					serviceLabels: map[string]string{
						"foo": "bar",
					},
					targetPort: intstr.IntOrString{
						Type:   intstr.Int,
						IntVal: int32(8888),
					},
				},
			},
			expectedSelector: map[string]string{
				common.LabelSparkAppName: "foo8",
				common.LabelSparkRole:    common.SparkRoleDriver,
			},
			expectError: false,
		},
		{
			name:             "service with bad port configurations",
			app:              app3,
			expectError:      true,
			expectedServices: []SparkService{{}},
		},
	}
	for _, test := range testcases {
		testFn(test, t)
	}
}

// TestDriverIngressAnnotationPrecedence tests that user annotations take precedence over operator defaults
func TestDriverIngressAnnotationPrecedence(t *testing.T) {
	// Set up Ingress capabilities to use networking.k8s.io/v1
	util.IngressCapabilities = util.Capabilities{"networking.k8s.io/v1": true}

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, networkingv1.AddToScheme(scheme))
	require.NoError(t, extensionsv1beta1.AddToScheme(scheme))
	require.NoError(t, v1beta2.AddToScheme(scheme))

	testCases := []struct {
		name                string
		userAnnotations     map[string]string
		userTLS             []networkingv1.IngressTLS
		defaultAnnotations  map[string]string
		defaultTLS          []networkingv1.IngressTLS
		expectHasUserKey    bool
		expectHasDefaultKey bool
		expectUserTLSHost   bool
	}{
		{
			name: "user annotations take precedence over defaults",
			userAnnotations: map[string]string{
				"user-key": "user-value",
			},
			defaultAnnotations: map[string]string{
				"default-key": "default-value",
			},
			expectHasUserKey:    true,
			expectHasDefaultKey: false,
		},
		{
			name:            "fallback to defaults when user annotations empty",
			userAnnotations: nil,
			defaultAnnotations: map[string]string{
				"default-key": "default-value",
			},
			expectHasUserKey:    false,
			expectHasDefaultKey: true,
		},
		{
			name:                "both empty results in no annotations",
			userAnnotations:     nil,
			defaultAnnotations:  nil,
			expectHasUserKey:    false,
			expectHasDefaultKey: false,
		},
		{
			name:              "user TLS takes precedence over defaults",
			userTLS:           []networkingv1.IngressTLS{{Hosts: []string{"user.example.com"}}},
			defaultTLS:        []networkingv1.IngressTLS{{Hosts: []string{"default.example.com"}}},
			expectUserTLSHost: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
			reconciler := Reconciler{
				client: fakeClient,
			}

			driverURL, _ := url.Parse("http://test.example.com")
			service := SparkService{
				serviceName: "test-service",
				servicePort: 4040,
			}

			app := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: "default",
					UID:       "test-uid",
				},
				Spec: v1beta2.SparkApplicationSpec{
					DriverIngressOptions: []v1beta2.DriverIngressConfiguration{
						{
							ServicePort:        ptr.To[int32](4040),
							IngressAnnotations: tc.userAnnotations,
							IngressTLS:         tc.userTLS,
						},
					},
				},
			}

			_, err := reconciler.createDriverIngress(
				context.TODO(),
				app,
				&app.Spec.DriverIngressOptions[0],
				service,
				driverURL,
				"",
				tc.defaultTLS,
				tc.defaultAnnotations,
			)

			if err != nil {
				t.Fatalf("createDriverIngress failed: %v", err)
			}

			// Verify by checking the created Ingress resource in the fake client
			createdIngress := &networkingv1.Ingress{}
			err = fakeClient.Get(context.TODO(), client.ObjectKey{
				Namespace: "default",
				Name:      "test-app-ing-4040",
			}, createdIngress)

			assert.NoError(t, err, "Ingress should be created")

			// Verify annotations based on precedence
			if tc.expectHasUserKey {
				assert.Contains(t, createdIngress.Annotations, "user-key")
				assert.Equal(t, "user-value", createdIngress.Annotations["user-key"])
			}
			if tc.expectHasDefaultKey {
				assert.Contains(t, createdIngress.Annotations, "default-key")
				assert.Equal(t, "default-value", createdIngress.Annotations["default-key"])
			}

			// Verify TLS based on precedence
			if tc.expectUserTLSHost {
				assert.Len(t, createdIngress.Spec.TLS, 1)
				assert.Contains(t, createdIngress.Spec.TLS[0].Hosts, "user.example.com")
			}
		})
	}
}

// TestDriverIngressAnnotationMutationPrevention tests that modifying annotations doesn't corrupt source
func TestDriverIngressAnnotationMutationPrevention(t *testing.T) {
	// Set up Ingress capabilities to use networking.k8s.io/v1
	util.IngressCapabilities = util.Capabilities{"networking.k8s.io/v1": true}

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, networkingv1.AddToScheme(scheme))
	require.NoError(t, extensionsv1beta1.AddToScheme(scheme))
	require.NoError(t, v1beta2.AddToScheme(scheme))

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := Reconciler{
		client: fakeClient,
	}

	driverURL, _ := url.Parse("http://test.example.com/path")
	service := SparkService{
		serviceName: "test-service",
		servicePort: 4040,
	}

	// Create source maps that we'll verify aren't mutated
	userAnnotations := map[string]string{
		"user-key": "user-value",
	}
	defaultAnnotations := map[string]string{
		"default-key": "default-value",
	}

	// Store original values
	originalUserKey := userAnnotations["user-key"]
	originalDefaultKey := defaultAnnotations["default-key"]

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "default",
			UID:       "test-uid",
		},
		Spec: v1beta2.SparkApplicationSpec{
			DriverIngressOptions: []v1beta2.DriverIngressConfiguration{
				{
					ServicePort:        ptr.To[int32](4040),
					IngressAnnotations: userAnnotations,
				},
			},
		},
	}

	_, err := reconciler.createDriverIngress(
		context.TODO(),
		app,
		&app.Spec.DriverIngressOptions[0],
		service,
		driverURL,
		"",
		nil,
		defaultAnnotations,
	)

	assert.NoError(t, err)

	// Verify source maps weren't mutated
	assert.Equal(t, originalUserKey, userAnnotations["user-key"], "User annotations map was mutated")
	assert.Equal(t, originalDefaultKey, defaultAnnotations["default-key"], "Default annotations map was mutated")

	// Verify rewrite-target annotation wasn't added to source (it's added internally for subpath)
	_, userHasRewrite := userAnnotations["nginx.ingress.kubernetes.io/rewrite-target"]
	assert.False(t, userHasRewrite, "Rewrite-target annotation leaked into user's source map")

	_, defaultHasRewrite := defaultAnnotations["nginx.ingress.kubernetes.io/rewrite-target"]
	assert.False(t, defaultHasRewrite, "Rewrite-target annotation leaked into default source map")
}
