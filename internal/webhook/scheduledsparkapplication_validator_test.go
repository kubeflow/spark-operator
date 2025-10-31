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
	"testing"

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
		app := &v1beta2.ScheduledSparkApplication{}
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
		oldApp := &v1beta2.ScheduledSparkApplication{}
		newApp := &v1beta2.ScheduledSparkApplication{}
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
