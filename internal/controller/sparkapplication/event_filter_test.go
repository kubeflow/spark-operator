/*
Copyright 2025 The Kubeflow authors.

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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

func TestIsServiceIngressFieldsOnlyChange(t *testing.T) {
	baseApp := func() *v1beta2.SparkApplication {
		return &v1beta2.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "default",
			},
			Spec: v1beta2.SparkApplicationSpec{
				Type:         v1beta2.SparkApplicationTypeScala,
				SparkVersion: "3.5.0",
				Driver: v1beta2.DriverSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Cores: func() *int32 { v := int32(1); return &v }(),
					},
				},
				Executor: v1beta2.ExecutorSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Cores: func() *int32 { v := int32(2); return &v }(),
					},
					Instances: func() *int32 { v := int32(2); return &v }(),
				},
			},
		}
	}

	tests := []struct {
		name     string
		oldApp   *v1beta2.SparkApplication
		newApp   *v1beta2.SparkApplication
		expected bool
	}{
		{
			name:     "no changes",
			oldApp:   baseApp(),
			newApp:   baseApp(),
			expected: false, // No changes at all
		},
		{
			name:   "only SparkUIOptions changed",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.SparkUIOptions = &v1beta2.SparkUIConfiguration{
					ServiceAnnotations: map[string]string{"key": "value"},
				}
				return app
			}(),
			expected: true,
		},
		{
			name:   "only Driver.ServiceAnnotations changed",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.Driver.ServiceAnnotations = map[string]string{"key": "value"}
				return app
			}(),
			expected: true,
		},
		{
			name:   "only Driver.ServiceLabels changed",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.Driver.ServiceLabels = map[string]string{"key": "value"}
				return app
			}(),
			expected: true,
		},
		{
			name:   "only DriverIngressOptions changed",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				port := int32(8080)
				app.Spec.DriverIngressOptions = []v1beta2.DriverIngressConfiguration{
					{
						ServicePort:        &port,
						ServiceAnnotations: map[string]string{"key": "value"},
					},
				}
				return app
			}(),
			expected: true,
		},
		{
			name:   "multiple service/ingress fields changed",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.SparkUIOptions = &v1beta2.SparkUIConfiguration{
					ServiceAnnotations: map[string]string{"key": "value"},
				}
				app.Spec.Driver.ServiceAnnotations = map[string]string{"key": "value"}
				return app
			}(),
			expected: true,
		},
		{
			name:   "driver cores changed - requires full restart",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.Driver.Cores = func() *int32 { v := int32(2); return &v }()
				return app
			}(),
			expected: false,
		},
		{
			name:   "executor instances changed - requires full restart",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.Executor.Instances = func() *int32 { v := int32(4); return &v }()
				return app
			}(),
			expected: false,
		},
		{
			name:   "sparkVersion changed - requires full restart",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.SparkVersion = "3.5.1"
				return app
			}(),
			expected: false,
		},
		{
			name:   "service field and non-service field both changed",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.Driver.ServiceAnnotations = map[string]string{"key": "value"}
				app.Spec.Executor.Instances = func() *int32 { v := int32(4); return &v }()
				return app
			}(),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isServiceIngressFieldsOnlyChange(tt.oldApp, tt.newApp)
			if result != tt.expected {
				t.Errorf("isServiceIngressFieldsOnlyChange() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestHasServiceIngressFieldChanges(t *testing.T) {
	baseApp := func() *v1beta2.SparkApplication {
		return &v1beta2.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "default",
			},
			Spec: v1beta2.SparkApplicationSpec{
				Type:         v1beta2.SparkApplicationTypeScala,
				SparkVersion: "3.5.0",
				Driver: v1beta2.DriverSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Cores: func() *int32 { v := int32(1); return &v }(),
					},
				},
				Executor: v1beta2.ExecutorSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Cores: func() *int32 { v := int32(2); return &v }(),
					},
					Instances: func() *int32 { v := int32(2); return &v }(),
				},
			},
		}
	}

	tests := []struct {
		name     string
		oldApp   *v1beta2.SparkApplication
		newApp   *v1beta2.SparkApplication
		expected bool
	}{
		{
			name:     "no changes",
			oldApp:   baseApp(),
			newApp:   baseApp(),
			expected: false,
		},
		{
			name:   "SparkUIOptions changed",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.SparkUIOptions = &v1beta2.SparkUIConfiguration{
					ServicePort: func() *int32 { v := int32(4040); return &v }(),
				}
				return app
			}(),
			expected: true,
		},
		{
			name:   "DriverIngressOptions changed",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				port := int32(8080)
				app.Spec.DriverIngressOptions = []v1beta2.DriverIngressConfiguration{
					{ServicePort: &port},
				}
				return app
			}(),
			expected: true,
		},
		{
			name:   "Driver.ServiceAnnotations changed",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.Driver.ServiceAnnotations = map[string]string{"key": "value"}
				return app
			}(),
			expected: true,
		},
		{
			name:   "Driver.ServiceLabels changed",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.Driver.ServiceLabels = map[string]string{"key": "value"}
				return app
			}(),
			expected: true,
		},
		{
			name:   "executor instances changed - not a service/ingress field",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.Executor.Instances = func() *int32 { v := int32(4); return &v }()
				return app
			}(),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasServiceIngressFieldChanges(tt.oldApp, tt.newApp)
			if result != tt.expected {
				t.Errorf("hasServiceIngressFieldChanges() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestIsWebhookPatchedFieldsOnlyChange(t *testing.T) {
	logger := log.Log.WithName("test")
	filter := &EventFilter{
		logger: logger,
	}

	baseApp := func() *v1beta2.SparkApplication {
		return &v1beta2.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "default",
			},
			Spec: v1beta2.SparkApplicationSpec{
				Type:         v1beta2.SparkApplicationTypeScala,
				SparkVersion: "3.5.0",
				Driver: v1beta2.DriverSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Cores: func() *int32 { v := int32(1); return &v }(),
					},
				},
				Executor: v1beta2.ExecutorSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Cores: func() *int32 { v := int32(2); return &v }(),
					},
					Instances: func() *int32 { v := int32(2); return &v }(),
				},
			},
		}
	}

	tests := []struct {
		name     string
		oldApp   *v1beta2.SparkApplication
		newApp   *v1beta2.SparkApplication
		expected bool
	}{
		{
			name:     "no changes",
			oldApp:   baseApp(),
			newApp:   baseApp(),
			expected: false, // No changes at all, so not "only webhook fields changed"
		},
		{
			name:   "only executor priorityClassName changed",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.Executor.PriorityClassName = func() *string { v := "high-priority"; return &v }()
				return app
			}(),
			expected: true,
		},
		{
			name:   "only executor nodeSelector changed",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.Executor.NodeSelector = map[string]string{"node-type": "spark"}
				return app
			}(),
			expected: true,
		},
		{
			name:   "only executor tolerations changed",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.Executor.Tolerations = []corev1.Toleration{
					{Key: "spark", Operator: corev1.TolerationOpExists},
				}
				return app
			}(),
			expected: true,
		},
		{
			name:   "only executor affinity changed",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.Executor.Affinity = &corev1.Affinity{
					NodeAffinity: &corev1.NodeAffinity{
						RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
							NodeSelectorTerms: []corev1.NodeSelectorTerm{
								{
									MatchExpressions: []corev1.NodeSelectorRequirement{
										{Key: "type", Operator: corev1.NodeSelectorOpIn, Values: []string{"spark"}},
									},
								},
							},
						},
					},
				}
				return app
			}(),
			expected: true,
		},
		{
			name:   "only executor schedulerName changed",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.Executor.SchedulerName = func() *string { v := "volcano"; return &v }()
				return app
			}(),
			expected: true,
		},
		{
			name:   "multiple executor webhook fields changed",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.Executor.PriorityClassName = func() *string { v := "high-priority"; return &v }()
				app.Spec.Executor.NodeSelector = map[string]string{"node-type": "spark"}
				app.Spec.Executor.Tolerations = []corev1.Toleration{
					{Key: "spark", Operator: corev1.TolerationOpExists},
				}
				return app
			}(),
			expected: true,
		},
		{
			name:   "driver cores changed - requires full restart",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.Driver.Cores = func() *int32 { v := int32(2); return &v }()
				return app
			}(),
			expected: false,
		},
		{
			name:   "executor instances changed - requires full restart",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.Executor.Instances = func() *int32 { v := int32(4); return &v }()
				return app
			}(),
			expected: false,
		},
		{
			name:   "sparkVersion changed - requires full restart",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.SparkVersion = "3.5.1"
				return app
			}(),
			expected: false,
		},
		{
			name:   "executor webhook field and non-webhook field both changed",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.Executor.PriorityClassName = func() *string { v := "high-priority"; return &v }()
				app.Spec.Executor.Instances = func() *int32 { v := int32(4); return &v }()
				return app
			}(),
			expected: false,
		},
		{
			name:   "driver priorityClassName changed - requires full restart",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.Driver.PriorityClassName = func() *string { v := "high-priority"; return &v }()
				return app
			}(),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filter.isWebhookPatchedFieldsOnlyChange(tt.oldApp, tt.newApp)
			if result != tt.expected {
				t.Errorf("isWebhookPatchedFieldsOnlyChange() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestHasExecutorWebhookFieldChanges(t *testing.T) {
	baseApp := func() *v1beta2.SparkApplication {
		return &v1beta2.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "default",
			},
			Spec: v1beta2.SparkApplicationSpec{
				Type:         v1beta2.SparkApplicationTypeScala,
				SparkVersion: "3.5.0",
				Executor: v1beta2.ExecutorSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Cores: func() *int32 { v := int32(2); return &v }(),
					},
					Instances: func() *int32 { v := int32(2); return &v }(),
				},
			},
		}
	}

	tests := []struct {
		name     string
		oldApp   *v1beta2.SparkApplication
		newApp   *v1beta2.SparkApplication
		expected bool
	}{
		{
			name:     "no changes",
			oldApp:   baseApp(),
			newApp:   baseApp(),
			expected: false,
		},
		{
			name:   "priorityClassName changed",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.Executor.PriorityClassName = func() *string { v := "high"; return &v }()
				return app
			}(),
			expected: true,
		},
		{
			name:   "nodeSelector changed",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.Executor.NodeSelector = map[string]string{"key": "value"}
				return app
			}(),
			expected: true,
		},
		{
			name:   "tolerations changed",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.Executor.Tolerations = []corev1.Toleration{{Key: "key"}}
				return app
			}(),
			expected: true,
		},
		{
			name:   "instances changed - not a webhook field",
			oldApp: baseApp(),
			newApp: func() *v1beta2.SparkApplication {
				app := baseApp()
				app.Spec.Executor.Instances = func() *int32 { v := int32(4); return &v }()
				return app
			}(),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasExecutorWebhookFieldChanges(tt.oldApp, tt.newApp)
			if result != tt.expected {
				t.Errorf("hasExecutorWebhookFieldChanges() = %v, expected %v", result, tt.expected)
			}
		})
	}
}
