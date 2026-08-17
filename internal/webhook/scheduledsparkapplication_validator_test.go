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
	validator := NewScheduledSparkApplicationValidator(false, nil, nil, nil, nil)

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
	validator := NewScheduledSparkApplicationValidator(false, nil, nil, nil, nil)

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
	validator := NewScheduledSparkApplicationValidator(false, nil, nil, nil, nil)

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
	validator := NewScheduledSparkApplicationValidator(false, nil, nil, nil, nil)

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
	validator := NewScheduledSparkApplicationValidator(false, nil, nil, nil, nil)

	oldApp := newScheduledSparkApplication()
	newApp := newScheduledSparkApplication()
	newApp.Spec.Template.SparkConf = map[string]string{common.SparkMaster: "k8s://https://attacker-cluster:443"}

	if _, err := validator.ValidateUpdate(context.Background(), oldApp, newApp); err == nil {
		t.Fatalf("expected sparkConf to be rejected on update, but it was allowed")
	}
}

func TestScheduledSparkApplicationValidatorValidateName(t *testing.T) {
	validator := NewScheduledSparkApplicationValidator(false, nil, nil, nil, nil)

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

func TestScheduledSparkApplicationValidatorURLSchemes(t *testing.T) {
	app := newScheduledSparkApplication()
	app.Spec.Template.MainApplicationFile = ptr.To("http://attacker.example.com/app.py")
	app.Spec.Template.Deps.Repositories = []string{"http://attacker.example.com/maven"}
	app.Spec.Template.SparkConf = map[string]string{
		"spark.jars": "http://attacker.example.com/evil.jar",
	}

	disabled := NewScheduledSparkApplicationValidator(false, nil, nil, nil, nil)
	if _, err := disabled.ValidateCreate(context.Background(), app); err != nil {
		t.Fatalf("expected no error when URL-scheme validation is disabled, got %v", err)
	}

	enabled := NewScheduledSparkApplicationValidator(true, nil, nil, nil, nil)
	_, err := enabled.ValidateCreate(context.Background(), app)
	requireURLValidationErrors(t, err,
		urlValidationErrorExpectation{
			field:  "spec.template.mainApplicationFile",
			value:  "http://attacker.example.com/app.py",
			scheme: "http",
			kind:   URLValidationSchemeNotAllowed,
		},
		urlValidationErrorExpectation{
			field:  "spec.template.deps.repositories",
			value:  "http://attacker.example.com/maven",
			scheme: "http",
			kind:   URLValidationSchemeNotAllowed,
		},
		urlValidationErrorExpectation{
			field:  `spec.template.sparkConf["spark.jars"]`,
			value:  "http://attacker.example.com/evil.jar",
			scheme: "http",
			kind:   URLValidationSchemeNotAllowed,
		},
	)

	oldApp := newScheduledSparkApplication()
	newApp := oldApp.DeepCopy()
	newApp.Spec.Template.MainApplicationFile = ptr.To("http://attacker.example.com/app.py")
	_, err = enabled.ValidateUpdate(context.Background(), oldApp, newApp)
	requireURLValidationErrors(t, err, urlValidationErrorExpectation{
		field:  "spec.template.mainApplicationFile",
		value:  "http://attacker.example.com/app.py",
		scheme: "http",
		kind:   URLValidationSchemeNotAllowed,
	})

	allowed := NewScheduledSparkApplicationValidator(true, []string{"https"}, nil, []string{"https://test1.example.com"}, nil)
	allowedApp := newScheduledSparkApplication()
	allowedApp.Spec.Template.MainApplicationFile = ptr.To("https://test1.example.com/app.py")
	if _, err := allowed.ValidateCreate(context.Background(), allowedApp); err != nil {
		t.Fatalf("expected allowed scheduled URL to pass validation, got %v", err)
	}
}
