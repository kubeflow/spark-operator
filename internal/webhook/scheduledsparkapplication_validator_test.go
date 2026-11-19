/*
Copyright 2025 The Kubeflow Authors.

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

package webhook

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

func newTestScheduledValidator(t *testing.T) *ScheduledSparkApplicationValidator {
	t.Helper()
	return NewScheduledSparkApplicationValidator(newTestValidator(t, false))
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

func TestScheduledSparkApplicationValidatorValidateCreate(t *testing.T) {
	validator := newTestScheduledValidator(t)

	t.Run("accepts ScheduledSparkApplication instances", func(t *testing.T) {
		app := newScheduledSparkApplication()
		warnings, err := validator.ValidateCreate(context.Background(), app)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(warnings) != 0 {
			t.Fatalf("expected no warnings, got %v", warnings)
		}
	})

	t.Run("propagates ErrInvalidConcurrencyPolicy through ValidateCreate", func(t *testing.T) {
		app := newScheduledSparkApplication()
		app.Spec.ConcurrencyPolicy = "MaybeSometimes"
		_, err := validator.ValidateCreate(context.Background(), app)
		if !errors.Is(err, ErrInvalidConcurrencyPolicy) {
			t.Fatalf("expected ErrInvalidConcurrencyPolicy, got %v", err)
		}
	})

	t.Run("propagates ErrInvalidHistoryLimit through ValidateCreate", func(t *testing.T) {
		app := newScheduledSparkApplication()
		app.Spec.SuccessfulRunHistoryLimit = ptr.To(int32(-1))
		_, err := validator.ValidateCreate(context.Background(), app)
		if !errors.Is(err, ErrInvalidHistoryLimit) {
			t.Fatalf("expected ErrInvalidHistoryLimit, got %v", err)
		}
	})
}

func TestScheduledSparkApplicationValidatorValidateUpdate(t *testing.T) {
	validator := newTestScheduledValidator(t)

	t.Run("accepts ScheduledSparkApplication instances", func(t *testing.T) {
		oldApp := newScheduledSparkApplication()
		newApp := newScheduledSparkApplication()
		warnings, err := validator.ValidateUpdate(context.Background(), oldApp, newApp)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(warnings) != 0 {
			t.Fatalf("expected no warnings, got %v", warnings)
		}
	})

	t.Run("skips validation when spec did not change even if it is invalid", func(t *testing.T) {
		// Anti-tautology guard: prove the stale spec would be rejected, then that
		// the unchanged-spec short-circuit returns nil.
		invalid := newScheduledSparkApplication()
		invalid.Spec.Schedule = "not a cron expression"
		if err := validator.validate(context.Background(), invalid); err == nil {
			t.Fatalf("precondition failed: invalid spec should not validate cleanly")
		}

		oldApp := invalid.DeepCopy()
		newApp := invalid.DeepCopy()
		if _, err := validator.ValidateUpdate(context.Background(), oldApp, newApp); err != nil {
			t.Fatalf("expected no error for unchanged spec, got %v", err)
		}
	})

	t.Run("rejects updates that introduce an invalid schedule", func(t *testing.T) {
		oldApp := newScheduledSparkApplication()
		newApp := newScheduledSparkApplication()
		newApp.Spec.Schedule = "not a cron expression"
		_, err := validator.ValidateUpdate(context.Background(), oldApp, newApp)
		if !errors.Is(err, ErrInvalidSchedule) {
			t.Fatalf("expected ErrInvalidSchedule, got %v", err)
		}
	})
}

func TestScheduledSparkApplicationValidatorValidateDelete(t *testing.T) {
	validator := newTestScheduledValidator(t)

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

func TestScheduledSparkApplicationValidatorValidateSchedule(t *testing.T) {
	validator := newTestScheduledValidator(t)

	t.Run("rejects empty schedule", func(t *testing.T) {
		app := newScheduledSparkApplication()
		app.Spec.Schedule = ""
		if err := validator.validateSchedule(app); !errors.Is(err, ErrEmptySchedule) {
			t.Fatalf("expected ErrEmptySchedule, got %v", err)
		}
	})

	t.Run("rejects whitespace-only schedule", func(t *testing.T) {
		app := newScheduledSparkApplication()
		app.Spec.Schedule = "   "
		if err := validator.validateSchedule(app); !errors.Is(err, ErrEmptySchedule) {
			t.Fatalf("expected ErrEmptySchedule, got %v", err)
		}
	})

	t.Run("classifies invalid timezone and invalid schedule", func(t *testing.T) {
		tzApp := newScheduledSparkApplication()
		tzApp.Spec.Schedule = "*/5 * * * *"
		tzApp.Spec.TimeZone = "Mars/Olympus_Mons"
		if err := validator.validateSchedule(tzApp); !errors.Is(err, util.ErrInvalidTimeZone) {
			t.Fatalf("expected util.ErrInvalidTimeZone, got %v", err)
		}

		cronApp := newScheduledSparkApplication()
		cronApp.Spec.Schedule = "garbage"
		if err := validator.validateSchedule(cronApp); !errors.Is(err, util.ErrInvalidSchedule) {
			t.Fatalf("expected util.ErrInvalidSchedule, got %v", err)
		}
	})
}

func TestScheduledSparkApplicationValidatorValidateConcurrencyPolicy(t *testing.T) {
	validator := newTestScheduledValidator(t)

	tests := []struct {
		policy    v1beta2.ConcurrencyPolicy
		wantError bool
	}{
		{"", false},
		{v1beta2.ConcurrencyAllow, false},
		{v1beta2.ConcurrencyForbid, false},
		{v1beta2.ConcurrencyReplace, false},
		{"MaybeSometimes", true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("policy %q", tt.policy), func(t *testing.T) {
			err := validator.validateConcurrencyPolicy(tt.policy)
			switch {
			case tt.wantError && !errors.Is(err, ErrInvalidConcurrencyPolicy):
				t.Fatalf("expected ErrInvalidConcurrencyPolicy for policy %q, got %v", tt.policy, err)
			case !tt.wantError && err != nil:
				t.Fatalf("expected nil error for policy %q, got %v", tt.policy, err)
			}
		})
	}
}

func TestScheduledSparkApplicationValidatorValidateHistoryLimits(t *testing.T) {
	validator := newTestScheduledValidator(t)

	t.Run("nil pointers are accepted", func(t *testing.T) {
		app := newScheduledSparkApplication()
		if err := validator.validateHistoryLimits(app); err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
	})

	t.Run("zero values are accepted", func(t *testing.T) {
		app := newScheduledSparkApplication()
		app.Spec.SuccessfulRunHistoryLimit = ptr.To(int32(0))
		app.Spec.FailedRunHistoryLimit = ptr.To(int32(0))
		if err := validator.validateHistoryLimits(app); err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
	})

	t.Run("negative limit is rejected", func(t *testing.T) {
		app := newScheduledSparkApplication()
		app.Spec.SuccessfulRunHistoryLimit = ptr.To(int32(-1))
		err := validator.validateHistoryLimits(app)
		if !errors.Is(err, ErrInvalidHistoryLimit) {
			t.Fatalf("expected ErrInvalidHistoryLimit, got %v", err)
		}
		if !strings.Contains(err.Error(), "successfulRunHistoryLimit") {
			t.Fatalf("expected message to identify successfulRunHistoryLimit, got %q", err.Error())
		}
	})
}

func TestScheduledSparkApplicationValidatorTemplateForwarding(t *testing.T) {
	validator := newTestScheduledValidator(t)

	t.Run("template node selector conflict propagates", func(t *testing.T) {
		app := newScheduledSparkApplication()
		app.Spec.Template.NodeSelector = map[string]string{"role": "shared"}
		app.Spec.Template.Driver.NodeSelector = map[string]string{"role": "driver"}

		_, err := validator.ValidateCreate(context.Background(), app)
		if !errors.Is(err, ErrInvalidTemplate) {
			t.Fatalf("expected ErrInvalidTemplate, got %v", err)
		}
	})

	t.Run("template config map errors use the spec.template path", func(t *testing.T) {
		app := newScheduledSparkApplication()
		app.Spec.Template.Driver.ConfigMaps = configMapRefs("MY_CONFIG")

		_, err := validator.ValidateCreate(context.Background(), app)
		if err == nil || !strings.Contains(err.Error(), `spec.template.driver.configMaps[0].name has invalid ConfigMap name "MY_CONFIG"`) {
			t.Fatalf("expected an invalid ConfigMap name error, got %v", err)
		}
	})
}

func TestScheduledSparkApplicationValidatorSparkConf_SecurityVectorsRejected(t *testing.T) {
	validator := newTestScheduledValidator(t)

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
	validator := newTestScheduledValidator(t)

	oldApp := newScheduledSparkApplication()
	newApp := newScheduledSparkApplication()
	newApp.Spec.Template.SparkConf = map[string]string{common.SparkMaster: "k8s://https://attacker-cluster:443"}

	if _, err := validator.ValidateUpdate(context.Background(), oldApp, newApp); err == nil {
		t.Fatalf("expected sparkConf to be rejected on update, but it was allowed")
	}
}

func TestScheduledSparkApplicationValidatorValidateName(t *testing.T) {
	validator := newTestScheduledValidator(t)

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
		{"name starting with hyphen", "-test-app", true},
		{"name ending with hyphen", "test-app-", true},
		{"empty name", "", true},
		{"name too long", strings.Repeat("a", 64), true},
		{"name with special characters", "test@app", true},
		{"name with underscore", "test_app", true},
		{"name with spaces", "test app", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := newScheduledSparkApplication()
			app.Name = tt.appName

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

func TestScheduledSparkApplicationValidatorValidateCreate_ClientModeWarning(t *testing.T) {
	validator := newTestScheduledValidator(t)

	app := newScheduledSparkApplication()
	app.Spec.Template.Mode = v1beta2.DeployModeClient

	warnings, err := validator.ValidateCreate(context.Background(), app)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "spec.template.mode: client mode is deprecated") {
		t.Fatalf("expected client mode deprecation warning, got %v", warnings)
	}
}

func TestScheduledSparkApplicationValidatorValidateUpdate_ClientModeWarning(t *testing.T) {
	validator := newTestScheduledValidator(t)

	oldApp := newScheduledSparkApplication()
	newApp := newScheduledSparkApplication()
	newApp.Spec.Template.Mode = v1beta2.DeployModeClient

	warnings, err := validator.ValidateUpdate(context.Background(), oldApp, newApp)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "spec.template.mode: client mode is deprecated") {
		t.Fatalf("expected client mode deprecation warning, got %v", warnings)
	}
}
