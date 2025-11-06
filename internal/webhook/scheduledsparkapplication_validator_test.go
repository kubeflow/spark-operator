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

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

func TestScheduledSparkApplicationValidatorValidateCreate(t *testing.T) {
	validator := NewScheduledSparkApplicationValidator()

	t.Run("returns nil for unrelated object types", func(t *testing.T) {
		warnings, err := validator.ValidateCreate(context.Background(), &v1beta2.SparkApplication{})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(warnings) != 0 {
			t.Fatalf("expected no warnings, got %v", warnings)
		}
	})

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

	t.Run("returns nil for unrelated object types", func(t *testing.T) {
		warnings, err := validator.ValidateUpdate(
			context.Background(),
			&v1beta2.ScheduledSparkApplication{},
			&v1beta2.SparkApplication{},
		)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(warnings) != 0 {
			t.Fatalf("expected no warnings, got %v", warnings)
		}
	})

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

	t.Run("returns nil for unrelated object types", func(t *testing.T) {
		warnings, err := validator.ValidateDelete(context.Background(), &v1beta2.SparkApplication{})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(warnings) != 0 {
			t.Fatalf("expected no warnings, got %v", warnings)
		}
	})

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
