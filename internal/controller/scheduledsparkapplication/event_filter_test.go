/*
Copyright 2026 The Kubeflow Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduledsparkapplication

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

func TestNewEventFilter(t *testing.T) {
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
		{
			name:              "invalid selector with unbalanced parentheses",
			namespaces:        []string{},
			namespaceSelector: "env in (prod,staging",
			expectError:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := NewEventFilter(fakeClient, tt.namespaces, tt.namespaceSelector)
			if tt.expectError {
				if err == nil {
					t.Errorf("NewEventFilter() expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("NewEventFilter() unexpected error: %v", err)
				}
				if filter == nil {
					t.Errorf("NewEventFilter() returned nil filter")
				}
			}
		})
	}
}

func TestEventFilter_Create(t *testing.T) {
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
		{
			name:              "non-ScheduledSparkApplication object returns false",
			namespaces:        []string{"default"},
			namespaceSelector: "",
			appNamespace:      "default",
			expected:          true, // Will test non-app object separately
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tt.existingNs != nil {
				builder = builder.WithObjects(tt.existingNs)
			}
			fakeClient := builder.Build()

			filter, err := NewEventFilter(fakeClient, tt.namespaces, tt.namespaceSelector)
			if err != nil {
				t.Fatalf("NewEventFilter() unexpected error: %v", err)
			}

			app := &v1beta2.ScheduledSparkApplication{
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

func TestEventFilter_Create_NonScheduledSparkApplication(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1beta2.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	filter, _ := NewEventFilter(fakeClient, []string{"default"}, "")

	// Test with a Pod instead of ScheduledSparkApplication
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
	}
	e := event.CreateEvent{Object: pod}
	result := filter.Create(e)

	if result != false {
		t.Errorf("Create() with non-ScheduledSparkApplication = %v, expected false", result)
	}
}

func TestEventFilter_Update(t *testing.T) {
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tt.existingNs != nil {
				builder = builder.WithObjects(tt.existingNs)
			}
			fakeClient := builder.Build()

			filter, err := NewEventFilter(fakeClient, tt.namespaces, tt.namespaceSelector)
			if err != nil {
				t.Fatalf("NewEventFilter() unexpected error: %v", err)
			}

			oldApp := &v1beta2.ScheduledSparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: tt.appNamespace,
				},
			}
			newApp := oldApp.DeepCopy()
			e := event.UpdateEvent{ObjectOld: oldApp, ObjectNew: newApp}
			result := filter.Update(e)

			if result != tt.expected {
				t.Errorf("Update() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestEventFilter_Update_ScheduleChange(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1beta2.AddToScheme(scheme)

	nextRun := metav1.NewTime(time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC))

	tests := []struct {
		name          string
		state         v1beta2.ScheduleState
		mutate        func(app *v1beta2.ScheduledSparkApplication)
		expectedState v1beta2.ScheduleState
		expectedNext  metav1.Time
	}{
		{
			name:          "schedule changed while scheduled",
			state:         v1beta2.ScheduleStateScheduled,
			mutate:        func(app *v1beta2.ScheduledSparkApplication) { app.Spec.Schedule = "* * * * *" },
			expectedState: v1beta2.ScheduleStateNew,
			expectedNext:  metav1.Time{},
		},
		{
			name:          "time zone changed while scheduled",
			state:         v1beta2.ScheduleStateScheduled,
			mutate:        func(app *v1beta2.ScheduledSparkApplication) { app.Spec.TimeZone = "Asia/Kolkata" },
			expectedState: v1beta2.ScheduleStateNew,
			expectedNext:  metav1.Time{},
		},
		{
			name:          "template changed while scheduled",
			state:         v1beta2.ScheduleStateScheduled,
			mutate:        func(app *v1beta2.ScheduledSparkApplication) { app.Spec.Template.Image = ptr.To("spark:new") },
			expectedState: v1beta2.ScheduleStateScheduled,
			expectedNext:  nextRun,
		},
		{
			name:          "schedule changed while failed validation",
			state:         v1beta2.ScheduleStateFailedValidation,
			mutate:        func(app *v1beta2.ScheduledSparkApplication) { app.Spec.Schedule = "* * * * *" },
			expectedState: v1beta2.ScheduleStateFailedValidation,
			expectedNext:  nextRun,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldApp := &v1beta2.ScheduledSparkApplication{
				ObjectMeta: metav1.ObjectMeta{Name: "test-app", Namespace: "default"},
				Spec:       v1beta2.ScheduledSparkApplicationSpec{Schedule: "0 0 1 1 *"},
				Status:     v1beta2.ScheduledSparkApplicationStatus{ScheduleState: tt.state, NextRun: nextRun},
			}
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).
				WithObjects(oldApp.DeepCopy()).
				WithStatusSubresource(&v1beta2.ScheduledSparkApplication{}).
				Build()
			filter, err := NewEventFilter(fakeClient, []string{"default"}, "")
			if err != nil {
				t.Fatalf("NewEventFilter() unexpected error: %v", err)
			}

			stored := &v1beta2.ScheduledSparkApplication{}
			if err := fakeClient.Get(context.TODO(), client.ObjectKeyFromObject(oldApp), stored); err != nil {
				t.Fatalf("Get() unexpected error: %v", err)
			}
			newApp := stored.DeepCopy()
			tt.mutate(newApp)
			if err := fakeClient.Update(context.TODO(), newApp); err != nil {
				t.Fatalf("Update() unexpected error: %v", err)
			}

			if !filter.Update(event.UpdateEvent{ObjectOld: stored, ObjectNew: newApp}) {
				t.Fatalf("Update() = false, expected true")
			}

			got := &v1beta2.ScheduledSparkApplication{}
			if err := fakeClient.Get(context.TODO(), client.ObjectKeyFromObject(oldApp), got); err != nil {
				t.Fatalf("Get() unexpected error: %v", err)
			}
			if got.Status.ScheduleState != tt.expectedState {
				t.Errorf("ScheduleState = %q, expected %q", got.Status.ScheduleState, tt.expectedState)
			}
			if !got.Status.NextRun.Equal(&tt.expectedNext) {
				t.Errorf("NextRun = %v, expected %v", got.Status.NextRun, tt.expectedNext)
			}
		})
	}
}

func TestEventFilter_Update_ScheduleChangeRetriesOnConflict(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1beta2.AddToScheme(scheme)

	oldApp := &v1beta2.ScheduledSparkApplication{
		ObjectMeta: metav1.ObjectMeta{Name: "test-app", Namespace: "default"},
		Spec:       v1beta2.ScheduledSparkApplicationSpec{Schedule: "0 0 1 1 *"},
		Status: v1beta2.ScheduledSparkApplicationStatus{
			ScheduleState: v1beta2.ScheduleStateScheduled,
			NextRun:       metav1.NewTime(time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)),
		},
	}
	statusUpdates := 0
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(oldApp.DeepCopy()).
		WithStatusSubresource(&v1beta2.ScheduledSparkApplication{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				statusUpdates++
				if statusUpdates == 1 {
					// Another status write lands first, so this write uses a stale resourceVersion.
					concurrent := &v1beta2.ScheduledSparkApplication{}
					if err := c.Get(ctx, client.ObjectKeyFromObject(obj), concurrent); err != nil {
						return err
					}
					concurrent.Status.LastRunName = "test-app-concurrent"
					if err := c.SubResource(subResourceName).Update(ctx, concurrent); err != nil {
						return err
					}
				}
				return c.SubResource(subResourceName).Update(ctx, obj, opts...)
			},
		}).
		Build()
	filter, err := NewEventFilter(fakeClient, []string{"default"}, "")
	if err != nil {
		t.Fatalf("NewEventFilter() unexpected error: %v", err)
	}

	stored := &v1beta2.ScheduledSparkApplication{}
	if err := fakeClient.Get(context.TODO(), client.ObjectKeyFromObject(oldApp), stored); err != nil {
		t.Fatalf("Get() unexpected error: %v", err)
	}
	newApp := stored.DeepCopy()
	newApp.Spec.Schedule = "* * * * *"
	if err := fakeClient.Update(context.TODO(), newApp); err != nil {
		t.Fatalf("Update() unexpected error: %v", err)
	}

	if !filter.Update(event.UpdateEvent{ObjectOld: stored, ObjectNew: newApp}) {
		t.Fatalf("Update() = false, expected true")
	}

	got := &v1beta2.ScheduledSparkApplication{}
	if err := fakeClient.Get(context.TODO(), client.ObjectKeyFromObject(oldApp), got); err != nil {
		t.Fatalf("Get() unexpected error: %v", err)
	}
	if got.Status.ScheduleState != v1beta2.ScheduleStateNew {
		t.Errorf("ScheduleState = %q, expected %q", got.Status.ScheduleState, v1beta2.ScheduleStateNew)
	}
	if !got.Status.NextRun.IsZero() {
		t.Errorf("NextRun = %v, expected zero", got.Status.NextRun)
	}
	if got.Status.LastRunName != "test-app-concurrent" {
		t.Errorf("LastRunName = %q, expected the concurrent write to be kept", got.Status.LastRunName)
	}
	if statusUpdates != 2 {
		t.Errorf("status updates = %d, expected 2 (one conflict, one retry)", statusUpdates)
	}
}

func TestEventFilter_Update_NonScheduledSparkApplication(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1beta2.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	filter, _ := NewEventFilter(fakeClient, []string{"default"}, "")

	// Test with a Pod instead of ScheduledSparkApplication
	oldPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
	}
	newPod := oldPod.DeepCopy()
	e := event.UpdateEvent{ObjectOld: oldPod, ObjectNew: newPod}
	result := filter.Update(e)

	if result != false {
		t.Errorf("Update() with non-ScheduledSparkApplication = %v, expected false", result)
	}
}

func TestEventFilter_Delete(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1beta2.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	tests := []struct {
		name              string
		namespaces        []string
		namespaceSelector string
		appNamespace      string
	}{
		{
			name:              "delete in watched namespace",
			namespaces:        []string{"default"},
			namespaceSelector: "",
			appNamespace:      "default",
		},
		{
			name:              "delete in unwatched namespace",
			namespaces:        []string{"default"},
			namespaceSelector: "",
			appNamespace:      "other",
		},
		{
			name:              "delete with selector",
			namespaces:        []string{},
			namespaceSelector: "spark=enabled",
			appNamespace:      "team-a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := NewEventFilter(fakeClient, tt.namespaces, tt.namespaceSelector)
			if err != nil {
				t.Fatalf("NewEventFilter() unexpected error: %v", err)
			}

			app := &v1beta2.ScheduledSparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: tt.appNamespace,
				},
			}
			e := event.DeleteEvent{Object: app}
			result := filter.Delete(e)

			// Delete always returns false for ScheduledSparkApplication
			if result != false {
				t.Errorf("Delete() = %v, expected false (Delete always returns false)", result)
			}
		})
	}
}

func TestEventFilter_Generic(t *testing.T) {
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tt.existingNs != nil {
				builder = builder.WithObjects(tt.existingNs)
			}
			fakeClient := builder.Build()

			filter, err := NewEventFilter(fakeClient, tt.namespaces, tt.namespaceSelector)
			if err != nil {
				t.Fatalf("NewEventFilter() unexpected error: %v", err)
			}

			app := &v1beta2.ScheduledSparkApplication{
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

func TestEventFilter_Generic_NonScheduledSparkApplication(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1beta2.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	filter, _ := NewEventFilter(fakeClient, []string{"default"}, "")

	// Test with a Pod instead of ScheduledSparkApplication
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
	}
	e := event.GenericEvent{Object: pod}
	result := filter.Generic(e)

	if result != false {
		t.Errorf("Generic() with non-ScheduledSparkApplication = %v, expected false", result)
	}
}

func TestEventFilter_MultipleLabelsSelector(t *testing.T) {
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

			filter, err := NewEventFilter(fakeClient, []string{}, tt.selector)
			if err != nil {
				t.Fatalf("NewEventFilter() unexpected error: %v", err)
			}

			app := &v1beta2.ScheduledSparkApplication{
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

func TestEventFilter_NamespaceAll(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = v1beta2.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	// Empty namespaces list means watch all namespaces
	filter, err := NewEventFilter(fakeClient, []string{}, "")
	if err != nil {
		t.Fatalf("NewEventFilter() unexpected error: %v", err)
	}

	tests := []string{"default", "prod", "team-a", "any-namespace"}

	for _, ns := range tests {
		t.Run("namespace-"+ns, func(t *testing.T) {
			app := &v1beta2.ScheduledSparkApplication{
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
