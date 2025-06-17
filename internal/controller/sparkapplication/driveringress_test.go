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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
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
		_, err = reconciler.createDriverIngress(context.TODO(), tc.app, &ingressOptions, *ingressConfig, driverUrl, "ingressClass")

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
					ServicePort:   util.Int32Ptr(8888),
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
					ServicePort: util.Int32Ptr(8888),
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
					ServicePort: util.Int32Ptr(4041),
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
					ServicePort: util.Int32Ptr(8888),
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
					ServicePort: util.Int32Ptr(8888),
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
					ServicePort: util.Int32Ptr(8888),
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
