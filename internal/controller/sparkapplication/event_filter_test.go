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
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

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

// =============================================================================
// Namespace Filtering Tests for sparkPodEventFilter
// =============================================================================

func TestNewSparkPodEventFilter(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1beta2.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	tests := []struct {
		name              string
		namespaces        []string
		namespaceSelector string
		expectError       bool
	}{
		{
			name:              "explicit namespaces only",
			namespaces:        []string{"default", "prod"},
			namespaceSelector: "",
			expectError:       false,
		},
		{
			name:              "selector only",
			namespaces:        []string{},
			namespaceSelector: "spark=enabled",
			expectError:       false,
		},
		{
			name:              "both explicit namespaces and selector",
			namespaces:        []string{"default"},
			namespaceSelector: "spark=enabled",
			expectError:       false,
		},
		{
			name:              "invalid selector syntax",
			namespaces:        []string{},
			namespaceSelector: "=value",
			expectError:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := newSparkPodEventFilter(fakeClient, tt.namespaces, tt.namespaceSelector)
			if tt.expectError {
				if err == nil {
					t.Errorf("newSparkPodEventFilter() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("newSparkPodEventFilter() unexpected error: %v", err)
				}
				if filter == nil {
					t.Errorf("newSparkPodEventFilter() returned nil filter")
				}
			}
		})
	}
}

func TestSparkPodEventFilter_Create(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1beta2.AddToScheme(scheme)

	tests := []struct {
		name              string
		namespaces        []string
		namespaceSelector string
		existingNs        *corev1.Namespace
		pod               *corev1.Pod
		expected          bool
	}{
		{
			name:              "spark pod in explicit namespace",
			namespaces:        []string{"default"},
			namespaceSelector: "",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "spark-driver",
					Namespace: "default",
					Labels:    map[string]string{common.LabelLaunchedBySparkOperator: "true"},
				},
			},
			expected: true,
		},
		{
			name:              "spark pod in non-matching namespace",
			namespaces:        []string{"default"},
			namespaceSelector: "",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "spark-driver",
					Namespace: "other",
					Labels:    map[string]string{common.LabelLaunchedBySparkOperator: "true"},
				},
			},
			expected: false,
		},
		{
			name:              "non-spark pod in matching namespace",
			namespaces:        []string{"default"},
			namespaceSelector: "",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "regular-pod",
					Namespace: "default",
					Labels:    map[string]string{},
				},
			},
			expected: false,
		},
		{
			name:              "spark pod with selector match",
			namespaces:        []string{},
			namespaceSelector: "spark=enabled",
			existingNs: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "team-a",
					Labels: map[string]string{"spark": "enabled"},
				},
			},
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "spark-driver",
					Namespace: "team-a",
					Labels:    map[string]string{common.LabelLaunchedBySparkOperator: "true"},
				},
			},
			expected: true,
		},
		{
			name:              "spark pod with selector no match",
			namespaces:        []string{},
			namespaceSelector: "spark=enabled",
			existingNs: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "team-b",
					Labels: map[string]string{"env": "dev"},
				},
			},
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "spark-driver",
					Namespace: "team-b",
					Labels:    map[string]string{common.LabelLaunchedBySparkOperator: "true"},
				},
			},
			expected: false,
		},
		{
			name:              "spark pod with combined explicit and selector - explicit match",
			namespaces:        []string{"default"},
			namespaceSelector: "spark=enabled",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "spark-driver",
					Namespace: "default",
					Labels:    map[string]string{common.LabelLaunchedBySparkOperator: "true"},
				},
			},
			expected: true,
		},
		{
			name:              "spark pod with combined explicit and selector - selector match",
			namespaces:        []string{"default"},
			namespaceSelector: "spark=enabled",
			existingNs: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "team-a",
					Labels: map[string]string{"spark": "enabled"},
				},
			},
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "spark-driver",
					Namespace: "team-a",
					Labels:    map[string]string{common.LabelLaunchedBySparkOperator: "true"},
				},
			},
			expected: true,
		},
		{
			name:              "pod without spark operator label",
			namespaces:        []string{"default"},
			namespaceSelector: "",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "spark-driver",
					Namespace: "default",
					Labels:    map[string]string{common.LabelLaunchedBySparkOperator: "false"},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tt.existingNs != nil {
				builder = builder.WithObjects(tt.existingNs)
			}
			fakeClient := builder.Build()

			filter, err := newSparkPodEventFilter(fakeClient, tt.namespaces, tt.namespaceSelector)
			if err != nil {
				t.Fatalf("newSparkPodEventFilter() unexpected error: %v", err)
			}

			e := event.CreateEvent{Object: tt.pod}
			result := filter.Create(e)

			if result != tt.expected {
				t.Errorf("Create() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestSparkPodEventFilter_Create_NonPodObject(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1beta2.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	filter, _ := newSparkPodEventFilter(fakeClient, []string{"default"}, "")

	// Test with a SparkApplication instead of Pod
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "default",
		},
	}
	e := event.CreateEvent{Object: app}
	result := filter.Create(e)

	if result != false {
		t.Errorf("Create() with non-Pod object = %v, expected false", result)
	}
}

func TestSparkPodEventFilter_Update(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1beta2.AddToScheme(scheme)

	tests := []struct {
		name              string
		namespaces        []string
		namespaceSelector string
		existingNs        *corev1.Namespace
		oldPod            *corev1.Pod
		newPod            *corev1.Pod
		expected          bool
	}{
		{
			name:              "phase changed - spark pod in matching namespace",
			namespaces:        []string{"default"},
			namespaceSelector: "",
			oldPod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "spark-driver",
					Namespace: "default",
					Labels:    map[string]string{common.LabelLaunchedBySparkOperator: "true"},
				},
				Status: corev1.PodStatus{Phase: corev1.PodPending},
			},
			newPod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "spark-driver",
					Namespace: "default",
					Labels:    map[string]string{common.LabelLaunchedBySparkOperator: "true"},
				},
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			},
			expected: true,
		},
		{
			name:              "phase not changed - should return false",
			namespaces:        []string{"default"},
			namespaceSelector: "",
			oldPod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "spark-driver",
					Namespace: "default",
					Labels:    map[string]string{common.LabelLaunchedBySparkOperator: "true"},
				},
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			},
			newPod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "spark-driver",
					Namespace: "default",
					Labels:    map[string]string{common.LabelLaunchedBySparkOperator: "true"},
				},
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			},
			expected: false,
		},
		{
			name:              "phase changed - spark pod in non-matching namespace",
			namespaces:        []string{"default"},
			namespaceSelector: "",
			oldPod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "spark-driver",
					Namespace: "other",
					Labels:    map[string]string{common.LabelLaunchedBySparkOperator: "true"},
				},
				Status: corev1.PodStatus{Phase: corev1.PodPending},
			},
			newPod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "spark-driver",
					Namespace: "other",
					Labels:    map[string]string{common.LabelLaunchedBySparkOperator: "true"},
				},
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			},
			expected: false,
		},
		{
			name:              "phase changed - non-spark pod",
			namespaces:        []string{"default"},
			namespaceSelector: "",
			oldPod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "regular-pod",
					Namespace: "default",
					Labels:    map[string]string{},
				},
				Status: corev1.PodStatus{Phase: corev1.PodPending},
			},
			newPod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "regular-pod",
					Namespace: "default",
					Labels:    map[string]string{},
				},
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			},
			expected: false,
		},
		{
			name:              "phase changed - spark pod with selector match",
			namespaces:        []string{},
			namespaceSelector: "spark=enabled",
			existingNs: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "team-a",
					Labels: map[string]string{"spark": "enabled"},
				},
			},
			oldPod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "spark-driver",
					Namespace: "team-a",
					Labels:    map[string]string{common.LabelLaunchedBySparkOperator: "true"},
				},
				Status: corev1.PodStatus{Phase: corev1.PodPending},
			},
			newPod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "spark-driver",
					Namespace: "team-a",
					Labels:    map[string]string{common.LabelLaunchedBySparkOperator: "true"},
				},
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tt.existingNs != nil {
				builder = builder.WithObjects(tt.existingNs)
			}
			fakeClient := builder.Build()

			filter, err := newSparkPodEventFilter(fakeClient, tt.namespaces, tt.namespaceSelector)
			if err != nil {
				t.Fatalf("newSparkPodEventFilter() unexpected error: %v", err)
			}

			e := event.UpdateEvent{ObjectOld: tt.oldPod, ObjectNew: tt.newPod}
			result := filter.Update(e)

			if result != tt.expected {
				t.Errorf("Update() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestSparkPodEventFilter_Update_NonPodObject(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1beta2.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	filter, _ := newSparkPodEventFilter(fakeClient, []string{"default"}, "")

	// Test with SparkApplication instead of Pod
	oldApp := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "default",
		},
	}
	newApp := oldApp.DeepCopy()
	e := event.UpdateEvent{ObjectOld: oldApp, ObjectNew: newApp}
	result := filter.Update(e)

	if result != false {
		t.Errorf("Update() with non-Pod object = %v, expected false", result)
	}
}

func TestSparkPodEventFilter_Delete(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1beta2.AddToScheme(scheme)

	tests := []struct {
		name              string
		namespaces        []string
		namespaceSelector string
		existingNs        *corev1.Namespace
		pod               *corev1.Pod
		expected          bool
	}{
		{
			name:              "spark pod in explicit namespace",
			namespaces:        []string{"default"},
			namespaceSelector: "",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "spark-driver",
					Namespace: "default",
					Labels:    map[string]string{common.LabelLaunchedBySparkOperator: "true"},
				},
			},
			expected: true,
		},
		{
			name:              "spark pod in non-matching namespace",
			namespaces:        []string{"default"},
			namespaceSelector: "",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "spark-driver",
					Namespace: "other",
					Labels:    map[string]string{common.LabelLaunchedBySparkOperator: "true"},
				},
			},
			expected: false,
		},
		{
			name:              "non-spark pod",
			namespaces:        []string{"default"},
			namespaceSelector: "",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "regular-pod",
					Namespace: "default",
					Labels:    map[string]string{},
				},
			},
			expected: false,
		},
		{
			name:              "spark pod with selector match",
			namespaces:        []string{},
			namespaceSelector: "spark=enabled",
			existingNs: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "team-a",
					Labels: map[string]string{"spark": "enabled"},
				},
			},
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "spark-driver",
					Namespace: "team-a",
					Labels:    map[string]string{common.LabelLaunchedBySparkOperator: "true"},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tt.existingNs != nil {
				builder = builder.WithObjects(tt.existingNs)
			}
			fakeClient := builder.Build()

			filter, err := newSparkPodEventFilter(fakeClient, tt.namespaces, tt.namespaceSelector)
			if err != nil {
				t.Fatalf("newSparkPodEventFilter() unexpected error: %v", err)
			}

			e := event.DeleteEvent{Object: tt.pod}
			result := filter.Delete(e)

			if result != tt.expected {
				t.Errorf("Delete() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestSparkPodEventFilter_Generic(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1beta2.AddToScheme(scheme)

	tests := []struct {
		name              string
		namespaces        []string
		namespaceSelector string
		existingNs        *corev1.Namespace
		pod               *corev1.Pod
		expected          bool
	}{
		{
			name:              "spark pod in explicit namespace",
			namespaces:        []string{"default"},
			namespaceSelector: "",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "spark-driver",
					Namespace: "default",
					Labels:    map[string]string{common.LabelLaunchedBySparkOperator: "true"},
				},
			},
			expected: true,
		},
		{
			name:              "spark pod in non-matching namespace",
			namespaces:        []string{"default"},
			namespaceSelector: "",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "spark-driver",
					Namespace: "other",
					Labels:    map[string]string{common.LabelLaunchedBySparkOperator: "true"},
				},
			},
			expected: false,
		},
		{
			name:              "non-spark pod",
			namespaces:        []string{"default"},
			namespaceSelector: "",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "regular-pod",
					Namespace: "default",
					Labels:    map[string]string{},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tt.existingNs != nil {
				builder = builder.WithObjects(tt.existingNs)
			}
			fakeClient := builder.Build()

			filter, err := newSparkPodEventFilter(fakeClient, tt.namespaces, tt.namespaceSelector)
			if err != nil {
				t.Fatalf("newSparkPodEventFilter() unexpected error: %v", err)
			}

			e := event.GenericEvent{Object: tt.pod}
			result := filter.Generic(e)

			if result != tt.expected {
				t.Errorf("Generic() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

// =============================================================================
// Namespace Filtering Tests for SparkApplicationEventFilter (EventFilter)
// =============================================================================

func TestNewSparkApplicationEventFilter(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1beta2.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	tests := []struct {
		name              string
		namespaces        []string
		namespaceSelector string
		expectError       bool
	}{
		{
			name:              "explicit namespaces only",
			namespaces:        []string{"default", "prod"},
			namespaceSelector: "",
			expectError:       false,
		},
		{
			name:              "selector only",
			namespaces:        []string{},
			namespaceSelector: "spark=enabled",
			expectError:       false,
		},
		{
			name:              "both explicit namespaces and selector",
			namespaces:        []string{"default"},
			namespaceSelector: "spark=enabled",
			expectError:       false,
		},
		{
			name:              "invalid selector syntax",
			namespaces:        []string{},
			namespaceSelector: "=value",
			expectError:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := NewSparkApplicationEventFilter(fakeClient, nil, tt.namespaces, tt.namespaceSelector)
			if tt.expectError {
				if err == nil {
					t.Errorf("NewSparkApplicationEventFilter() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("NewSparkApplicationEventFilter() unexpected error: %v", err)
				}
				if filter == nil {
					t.Errorf("NewSparkApplicationEventFilter() returned nil filter")
				}
			}
		})
	}
}

func TestSparkApplicationEventFilter_Create(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1beta2.AddToScheme(scheme)

	tests := []struct {
		name              string
		namespaces        []string
		namespaceSelector string
		existingNs        *corev1.Namespace
		appNamespace      string
		expected          bool
	}{
		{
			name:              "explicit namespace match",
			namespaces:        []string{"default", "prod"},
			namespaceSelector: "",
			appNamespace:      "default",
			expected:          true,
		},
		{
			name:              "explicit namespace no match",
			namespaces:        []string{"default", "prod"},
			namespaceSelector: "",
			appNamespace:      "other",
			expected:          false,
		},
		{
			name:              "selector match",
			namespaces:        []string{},
			namespaceSelector: "spark=enabled",
			existingNs: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "team-a",
					Labels: map[string]string{"spark": "enabled"},
				},
			},
			appNamespace: "team-a",
			expected:     true,
		},
		{
			name:              "selector no match",
			namespaces:        []string{},
			namespaceSelector: "spark=enabled",
			existingNs: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "team-b",
					Labels: map[string]string{"env": "dev"},
				},
			},
			appNamespace: "team-b",
			expected:     false,
		},
		{
			name:              "combined - explicit namespace match",
			namespaces:        []string{"default"},
			namespaceSelector: "spark=enabled",
			appNamespace:      "default",
			expected:          true,
		},
		{
			name:              "combined - selector match",
			namespaces:        []string{"default"},
			namespaceSelector: "spark=enabled",
			existingNs: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "team-a",
					Labels: map[string]string{"spark": "enabled"},
				},
			},
			appNamespace: "team-a",
			expected:     true,
		},
		{
			name:              "namespace not found with selector",
			namespaces:        []string{},
			namespaceSelector: "spark=enabled",
			appNamespace:      "non-existent",
			expected:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tt.existingNs != nil {
				builder = builder.WithObjects(tt.existingNs)
			}
			fakeClient := builder.Build()

			filter, err := NewSparkApplicationEventFilter(fakeClient, nil, tt.namespaces, tt.namespaceSelector)
			if err != nil {
				t.Fatalf("NewSparkApplicationEventFilter() unexpected error: %v", err)
			}

			app := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: tt.appNamespace,
				},
			}
			e := event.CreateEvent{Object: app}
			result := filter.Create(e)

			if result != tt.expected {
				t.Errorf("Create() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestSparkApplicationEventFilter_Create_NonSparkApplication(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1beta2.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	filter, _ := NewSparkApplicationEventFilter(fakeClient, nil, []string{"default"}, "")

	// Test with a Pod instead of SparkApplication
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
	}
	e := event.CreateEvent{Object: pod}
	result := filter.Create(e)

	if result != false {
		t.Errorf("Create() with non-SparkApplication = %v, expected false", result)
	}
}

func TestSparkApplicationEventFilter_Delete(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1beta2.AddToScheme(scheme)

	tests := []struct {
		name              string
		namespaces        []string
		namespaceSelector string
		existingNs        *corev1.Namespace
		appNamespace      string
		expected          bool
	}{
		{
			name:              "explicit namespace match",
			namespaces:        []string{"default"},
			namespaceSelector: "",
			appNamespace:      "default",
			expected:          true,
		},
		{
			name:              "explicit namespace no match",
			namespaces:        []string{"default"},
			namespaceSelector: "",
			appNamespace:      "other",
			expected:          false,
		},
		{
			name:              "selector match",
			namespaces:        []string{},
			namespaceSelector: "spark=enabled",
			existingNs: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "team-a",
					Labels: map[string]string{"spark": "enabled"},
				},
			},
			appNamespace: "team-a",
			expected:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tt.existingNs != nil {
				builder = builder.WithObjects(tt.existingNs)
			}
			fakeClient := builder.Build()

			filter, err := NewSparkApplicationEventFilter(fakeClient, nil, tt.namespaces, tt.namespaceSelector)
			if err != nil {
				t.Fatalf("NewSparkApplicationEventFilter() unexpected error: %v", err)
			}

			app := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: tt.appNamespace,
				},
			}
			e := event.DeleteEvent{Object: app}
			result := filter.Delete(e)

			if result != tt.expected {
				t.Errorf("Delete() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestSparkApplicationEventFilter_Generic(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1beta2.AddToScheme(scheme)

	tests := []struct {
		name              string
		namespaces        []string
		namespaceSelector string
		existingNs        *corev1.Namespace
		appNamespace      string
		expected          bool
	}{
		{
			name:              "explicit namespace match",
			namespaces:        []string{"default"},
			namespaceSelector: "",
			appNamespace:      "default",
			expected:          true,
		},
		{
			name:              "explicit namespace no match",
			namespaces:        []string{"default"},
			namespaceSelector: "",
			appNamespace:      "other",
			expected:          false,
		},
		{
			name:              "selector match",
			namespaces:        []string{},
			namespaceSelector: "spark=enabled",
			existingNs: &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "team-a",
					Labels: map[string]string{"spark": "enabled"},
				},
			},
			appNamespace: "team-a",
			expected:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tt.existingNs != nil {
				builder = builder.WithObjects(tt.existingNs)
			}
			fakeClient := builder.Build()

			filter, err := NewSparkApplicationEventFilter(fakeClient, nil, tt.namespaces, tt.namespaceSelector)
			if err != nil {
				t.Fatalf("NewSparkApplicationEventFilter() unexpected error: %v", err)
			}

			app := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: tt.appNamespace,
				},
			}
			e := event.GenericEvent{Object: app}
			result := filter.Generic(e)

			if result != tt.expected {
				t.Errorf("Generic() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestSparkApplicationEventFilter_MultipleLabelsSelector(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1beta2.AddToScheme(scheme)

	tests := []struct {
		name     string
		selector string
		nsLabels map[string]string
		expected bool
	}{
		{
			name:     "all labels match (AND logic)",
			selector: "spark=enabled,env=prod",
			nsLabels: map[string]string{"spark": "enabled", "env": "prod"},
			expected: true,
		},
		{
			name:     "only one label matches",
			selector: "spark=enabled,env=prod",
			nsLabels: map[string]string{"spark": "enabled"},
			expected: false,
		},
		{
			name:     "in operator match",
			selector: "env in (prod,staging)",
			nsLabels: map[string]string{"env": "prod"},
			expected: true,
		},
		{
			name:     "in operator no match",
			selector: "env in (prod,staging)",
			nsLabels: map[string]string{"env": "dev"},
			expected: false,
		},
		{
			name:     "notin operator match",
			selector: "env notin (dev,test)",
			nsLabels: map[string]string{"env": "prod"},
			expected: true,
		},
		{
			name:     "notin operator no match",
			selector: "env notin (dev,test)",
			nsLabels: map[string]string{"env": "dev"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "test-ns",
					Labels: tt.nsLabels,
				},
			}
			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(ns).
				Build()

			filter, err := NewSparkApplicationEventFilter(fakeClient, nil, []string{}, tt.selector)
			if err != nil {
				t.Fatalf("NewSparkApplicationEventFilter() unexpected error: %v", err)
			}

			app := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: "test-ns",
				},
			}
			e := event.CreateEvent{Object: app}
			result := filter.Create(e)

			if result != tt.expected {
				t.Errorf("Create() with selector %q = %v, expected %v", tt.selector, result, tt.expected)
			}
		})
	}
}

func TestSparkApplicationEventFilter_NamespaceAll(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1beta2.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	// Empty namespaces list means watch all namespaces
	filter, err := NewSparkApplicationEventFilter(fakeClient, nil, []string{}, "")
	if err != nil {
		t.Fatalf("NewSparkApplicationEventFilter() unexpected error: %v", err)
	}

	tests := []string{"default", "prod", "team-a", "any-namespace"}

	for _, ns := range tests {
		t.Run("namespace-"+ns, func(t *testing.T) {
			app := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: ns,
				},
			}
			e := event.CreateEvent{Object: app}
			result := filter.Create(e)

			if result != true {
				t.Errorf("Create() with NamespaceAll for namespace %q = %v, expected true", ns, result)
			}
		})
	}
}
