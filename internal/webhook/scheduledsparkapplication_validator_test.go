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

package webhook

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

func TestScheduledSparkApplicationValidatorValidateCreate(t *testing.T) {
	validator := NewScheduledSparkApplicationValidator()

	t.Run("accepts ScheduledSparkApplication instances", func(t *testing.T) {
		app := &v1beta2.ScheduledSparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "default",
			},
		}
		warnings, err := validator.ValidateCreate(context.Background(), app)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(warnings) != 0 {
			t.Fatalf("expected no warnings, got %v", warnings)
		}
	})
}

func TestScheduledSparkApplicationValidatorValidateUpdate(t *testing.T) {
	validator := NewScheduledSparkApplicationValidator()

	t.Run("accepts ScheduledSparkApplication instances", func(t *testing.T) {
		oldApp := &v1beta2.ScheduledSparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "default",
			},
		}
		newApp := &v1beta2.ScheduledSparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "default",
			},
		}
		warnings, err := validator.ValidateUpdate(context.Background(), oldApp, newApp)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(warnings) != 0 {
			t.Fatalf("expected no warnings, got %v", warnings)
		}
	})
}

func TestScheduledSparkApplicationValidatorValidateDelete(t *testing.T) {
	validator := NewScheduledSparkApplicationValidator()

	t.Run("accepts ScheduledSparkApplication instances", func(t *testing.T) {
		warnings, err := validator.ValidateDelete(context.Background(), &v1beta2.ScheduledSparkApplication{})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(warnings) != 0 {
			t.Fatalf("expected no warnings, got %v", warnings)
		}
	})
}

func newScheduledSparkApplication() *v1beta2.ScheduledSparkApplication {
	return &v1beta2.ScheduledSparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-scheduled-app",
			Namespace: "default",
		},
		Spec: v1beta2.ScheduledSparkApplicationSpec{
			Schedule: "@every 1h",
			Template: newSparkApplication().Spec,
		},
	}
}

func TestScheduledSparkApplicationValidatorSparkConf_SecurityVectorsRejected(t *testing.T) {
	validator := NewScheduledSparkApplicationValidator()

	for _, tt := range sparkConfSecurityVectors {
		t.Run(tt.name, func(t *testing.T) {
			app := newScheduledSparkApplication()
			app.Spec.Template.SparkConf = tt.sparkConf

			if _, err := validator.ValidateCreate(context.Background(), app); err == nil {
				t.Fatalf("expected sparkConf to be rejected, but it was allowed")
			}
		})
	}
}

func TestScheduledSparkApplicationValidatorSparkConf_UpdateRejected(t *testing.T) {
	validator := NewScheduledSparkApplicationValidator()

	oldApp := newScheduledSparkApplication()
	newApp := newScheduledSparkApplication()
	newApp.Spec.Template.SparkConf = map[string]string{common.SparkMaster: "k8s://https://attacker-cluster:443"}

	if _, err := validator.ValidateUpdate(context.Background(), oldApp, newApp); err == nil {
		t.Fatalf("expected sparkConf to be rejected on update, but it was allowed")
	}
}

func TestScheduledSparkApplicationValidatorValidateName(t *testing.T) {
	validator := NewScheduledSparkApplicationValidator()

	tests := []struct {
		name      string
		appName   string
		wantError bool
	}{
		// Valid names
		{"valid simple name", "test-app", false},
		{"valid name with numbers", "test-app-123", false},
		{"valid single letter", "a", false},
		{"valid name ending with number", "my-app-1", false},
		{"valid name with multiple hyphens", "my-test-app-123", false},
		{"valid 63 char name", strings.Repeat("a", 63), false},
		{"valid name with hyphens in middle", "a-b-c-d-e", false},

		// Invalid names
		{"name starting with number", "123test-app", true},
		{"name with uppercase", "Test-App", true},
		{"name with uppercase at start", "TestApp", true},
		{"name with uppercase in middle", "test-App", true},
		{"name starting with hyphen", "-test-app", true},
		{"name ending with hyphen", "test-app-", true},
		{"name with consecutive hyphens", "test--app", false}, // Kubernetes validation allows consecutive hyphens
		{"empty name", "", true},
		{"name too long", strings.Repeat("a", 64), true},
		{"name with special characters", "test@app", true},
		{"name with underscore", "test_app", true},
		{"name with spaces", "test app", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &v1beta2.ScheduledSparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      tt.appName,
					Namespace: "default",
				},
			}

			_, err := validator.ValidateCreate(context.Background(), app)
			hasError := err != nil

			if hasError != tt.wantError {
				t.Errorf("validateName(%q) = error %v, wantError %v, got error: %v", tt.appName, hasError, tt.wantError, err)
			}

			if hasError && err.Error() == "" {
				t.Errorf("validateName(%q) should return a non-empty error message, got: %v", tt.appName, err)
			}
		})
	}
}

func TestScheduledSparkApplicationValidatorValidateCreate_ConfigMapNames(t *testing.T) {
	validator := NewScheduledSparkApplicationValidator()

	app := newScheduledSparkApplication()
	app.Spec.Template.Driver.ConfigMaps = configMapRefs("MY_CONFIG")

	_, err := validator.ValidateCreate(context.Background(), app)
	if err == nil || !strings.Contains(err.Error(), `spec.template.driver.configMaps[0].name has invalid ConfigMap name "MY_CONFIG"`) {
		t.Fatalf("expected an invalid ConfigMap name error, got %v", err)
	}
}

func TestScheduledSparkApplicationValidatorValidateWorkloadScheduler(t *testing.T) {
	validator := NewScheduledSparkApplicationValidator()

	tests := []struct {
		name         string
		modifyApp    func(app *v1beta2.ScheduledSparkApplication)
		wantErr      bool
		errContains  string
		wantWarns    int
		warnContains string
	}{
		{
			name: "workload + queue (warn, not reject)",
			modifyApp: func(app *v1beta2.ScheduledSparkApplication) {
				workload := "workload"
				app.Spec.Template.BatchScheduler = &workload
				queue := "my-queue"
				app.Spec.Template.BatchSchedulerOptions = &v1beta2.BatchSchedulerConfiguration{
					Queue: &queue,
				}
			},
			wantErr:      false,
			wantWarns:    1,
			warnContains: "batchSchedulerOptions.queue has no effect when batchScheduler is \"workload\"",
		},
		{
			name: "volcano + queue (no warning)",
			modifyApp: func(app *v1beta2.ScheduledSparkApplication) {
				volcano := "volcano"
				app.Spec.Template.BatchScheduler = &volcano
				queue := "my-queue"
				app.Spec.Template.BatchSchedulerOptions = &v1beta2.BatchSchedulerConfiguration{
					Queue: &queue,
				}
			},
			wantErr:   false,
			wantWarns: 0,
		},
		{
			name: "zero minMember (reject)",
			modifyApp: func(app *v1beta2.ScheduledSparkApplication) {
				app.Spec.Template.BatchSchedulerOptions = &v1beta2.BatchSchedulerConfiguration{
					MinMember: ptr.To[int32](0),
				}
			},
			wantErr:     true,
			errContains: "minMember must be greater than or equal to 1",
		},
		{
			name: "negative minMember (reject)",
			modifyApp: func(app *v1beta2.ScheduledSparkApplication) {
				app.Spec.Template.BatchSchedulerOptions = &v1beta2.BatchSchedulerConfiguration{
					MinMember: ptr.To[int32](-1),
				}
			},
			wantErr:     true,
			errContains: "minMember must be greater than or equal to 1",
		},
		{
			name: "positive minMember (pass)",
			modifyApp: func(app *v1beta2.ScheduledSparkApplication) {
				app.Spec.Template.BatchScheduler = ptr.To("workload")
				app.Spec.Template.BatchSchedulerOptions = &v1beta2.BatchSchedulerConfiguration{
					MinMember: ptr.To[int32](5),
				}
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := newScheduledSparkApplication()
			tt.modifyApp(app)

			warnings, err := validator.ValidateCreate(context.Background(), app)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.errContains)
				}
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("expected error containing %q, got %q", tt.errContains, err.Error())
				}
			} else if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if len(warnings) != tt.wantWarns {
				t.Errorf("expected %d warnings, got %d: %v", tt.wantWarns, len(warnings), warnings)
			}
			if tt.wantWarns > 0 && tt.warnContains != "" {
				found := false
				for _, w := range warnings {
					if strings.Contains(w, tt.warnContains) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected warning containing %q, got warnings: %v", tt.warnContains, warnings)
				}
			}
		})
	}
}

func TestScheduledSparkApplicationValidatorValidateUpdate_WorkloadScheduler(t *testing.T) {
	validator := NewScheduledSparkApplicationValidator()

	t.Run("changed template receives workload validation", func(t *testing.T) {
		oldApp := newScheduledSparkApplication()
		newApp := newScheduledSparkApplication()
		// Change template to trigger validation
		newApp.Spec.Template.BatchSchedulerOptions = &v1beta2.BatchSchedulerConfiguration{
			MinMember: ptr.To[int32](-1),
		}

		_, err := validator.ValidateUpdate(context.Background(), oldApp, newApp)
		if err == nil || !strings.Contains(err.Error(), "minMember must be greater than or equal to 1") {
			t.Fatalf("expected minMember validation error, got %v", err)
		}
	})

	t.Run("unchanged template skips validation", func(t *testing.T) {
		oldApp := newScheduledSparkApplication()
		oldApp.Spec.Template.BatchSchedulerOptions = &v1beta2.BatchSchedulerConfiguration{
			MinMember: ptr.To[int32](-1), // Invalid, but unchanged
		}
		newApp := oldApp.DeepCopy()
		// Only metadata changed
		newApp.Labels = map[string]string{"team": "data"}

		_, err := validator.ValidateUpdate(context.Background(), oldApp, newApp)
		if err != nil {
			t.Fatalf("expected unchanged spec to skip validation, got %v", err)
		}
	})
}
