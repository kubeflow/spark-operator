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
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// newTestScheduledValidator builds a ScheduledSparkApplicationValidator that
// delegates template validation to a test SparkApplicationValidator. Quota
// enforcement is left off so the SparkApplication-level checks under test are
// purely spec-shape checks (validateSpec), not resource-quota interactions.
func newTestScheduledValidator(t *testing.T) *ScheduledSparkApplicationValidator {
	t.Helper()
	return NewScheduledSparkApplicationValidator(newTestValidator(t, false))
}

// newScheduledSparkApplication returns a minimal valid SSA that should pass
// every validator below if no field is mutated. Tests then mutate one field at
// a time to assert the new validator catches the regression.
func newScheduledSparkApplication() *v1beta2.ScheduledSparkApplication {
	return &v1beta2.ScheduledSparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "default",
		},
		Spec: v1beta2.ScheduledSparkApplicationSpec{
			Schedule:          "*/5 * * * *",
			ConcurrencyPolicy: v1beta2.ConcurrencyAllow,
			Template:          newSparkApplication().Spec,
		},
	}
}

func TestScheduledSparkApplicationValidatorValidateCreate(t *testing.T) {
	validator := newTestScheduledValidator(t)

	t.Run("returns nil for unrelated object types", func(t *testing.T) {
		warnings, err := validator.ValidateCreate(context.Background(), &v1beta2.SparkApplication{})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(warnings) != 0 {
			t.Fatalf("expected no warnings, got %v", warnings)
		}
	})

	t.Run("accepts valid ScheduledSparkApplication instances", func(t *testing.T) {
		warnings, err := validator.ValidateCreate(context.Background(), newScheduledSparkApplication())
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(warnings) != 0 {
			t.Fatalf("expected no warnings, got %v", warnings)
		}
	})

	// End-to-end ValidateCreate cases for each error category. The per-helper
	// tests below cover correctness of each check in isolation; these cases
	// cover the orchestration in validate() and ensure error sentinels propagate
	// all the way out through the admission entry point.
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

	t.Run("accepts unchanged ScheduledSparkApplication updates", func(t *testing.T) {
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
		// Anti-tautology guard: first prove the spec we're about to short-circuit
		// past would actually be rejected if validation ran. Then prove ValidateUpdate
		// returns nil for the unchanged-spec case. Together these prove the
		// equality.Semantic.DeepEqual short-circuit is the path that succeeded,
		// rather than validation succeeding on its own merits.
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

	t.Run("re-validates when spec changes between two equally-invalid forms", func(t *testing.T) {
		// Even when both old and new are invalid, mutating the spec must trigger
		// re-validation: the short-circuit is keyed on Semantic.DeepEqual, not on
		// "old was invalid so anything goes".
		oldApp := newScheduledSparkApplication()
		oldApp.Spec.Schedule = "not a cron"
		newApp := newScheduledSparkApplication()
		newApp.Spec.Schedule = "60 * * * *" // different invalid value
		_, err := validator.ValidateUpdate(context.Background(), oldApp, newApp)
		if !errors.Is(err, ErrInvalidSchedule) {
			t.Fatalf("expected ErrInvalidSchedule, got %v", err)
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

	t.Run("nil oldObj falls through to full validation", func(t *testing.T) {
		// The runtime.Object oldObj parameter is documented as optional in the
		// admission API. Guard against a panic and confirm validation still runs.
		newApp := newScheduledSparkApplication()
		newApp.Spec.Schedule = "not a cron"
		_, err := validator.ValidateUpdate(context.Background(), nil, newApp)
		if !errors.Is(err, ErrInvalidSchedule) {
			t.Fatalf("expected ErrInvalidSchedule from full re-validation, got %v", err)
		}
	})

	t.Run("rejects updates that change to an invalid name", func(t *testing.T) {
		// Names are immutable in the API server but the validator runs validateName
		// defensively. A bad name on the new spec must produce ErrInvalidName even
		// before any spec-shape checks run.
		oldApp := newScheduledSparkApplication()
		newApp := newScheduledSparkApplication()
		newApp.Name = "Invalid_Name"
		_, err := validator.ValidateUpdate(context.Background(), oldApp, newApp)
		if !errors.Is(err, ErrInvalidName) {
			t.Fatalf("expected ErrInvalidName, got %v", err)
		}
	})

	t.Run("accepts updates that mutate the spec to another valid value", func(t *testing.T) {
		// Without a mutated-but-valid case the success path through the full
		// validation chain (validateName -> validateSchedule -> ... -> nil) is not
		// exercised; the unchanged-spec test takes the short-circuit and the
		// rejecting tests bail before reaching the trailing return. Mutate only the
		// schedule so DeepEqual reports inequality but every other check passes.
		oldApp := newScheduledSparkApplication()
		newApp := newScheduledSparkApplication()
		newApp.Spec.Schedule = "0 */2 * * *"
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
	validator := newTestScheduledValidator(t)

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

func TestScheduledSparkApplicationValidatorValidateSchedule(t *testing.T) {
	validator := newTestScheduledValidator(t)

	tests := []struct {
		name     string
		schedule string
		timezone string
		// wantErr is the sentinel the validator must wrap (or nil to mean
		// validation must succeed). errors.Is lets the assertion verify
		// classification without coupling tests to error message wording.
		wantErr error
	}{
		{name: "valid 5-field cron", schedule: "*/5 * * * *"},
		{name: "valid descriptor", schedule: "@hourly"},
		{name: "valid with explicit UTC timezone", schedule: "0 12 * * *", timezone: "UTC"},
		{name: "valid IANA timezone", schedule: "0 12 * * *", timezone: "America/New_York"},
		{name: "valid embedded CRON_TZ overrides timezone field", schedule: "CRON_TZ=UTC 0 12 * * *", timezone: "America/New_York"},

		{name: "empty schedule is rejected", schedule: "", wantErr: ErrEmptySchedule},
		{name: "whitespace-only schedule is rejected", schedule: "   ", wantErr: ErrEmptySchedule},
		{name: "invalid cron is rejected", schedule: "not a cron", wantErr: ErrInvalidSchedule},
		{name: "out-of-range field is rejected", schedule: "60 * * * *", wantErr: ErrInvalidSchedule},
		{name: "invalid timezone is rejected", schedule: "*/5 * * * *", timezone: "Mars/Olympus_Mons", wantErr: ErrInvalidSchedule},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := newScheduledSparkApplication()
			app.Spec.Schedule = tt.schedule
			app.Spec.TimeZone = tt.timezone

			err := validator.validateSchedule(app)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("expected nil error, got %v", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("expected errors.Is(%v), got %v", tt.wantErr, err)
			}
		})
	}
}

func TestScheduledSparkApplicationValidatorScheduleErrorClassifiesTimezoneVsSchedule(t *testing.T) {
	// When the timezone is bogus the wrapped chain must include
	// util.ErrInvalidTimeZone; when only the cron expression is bogus it must
	// include util.ErrInvalidSchedule. This guards against the validator (or
	// the shared util.ParseSchedule) silently collapsing the two failure modes.
	validator := newTestScheduledValidator(t)

	tzApp := newScheduledSparkApplication()
	tzApp.Spec.Schedule = "*/5 * * * *"
	tzApp.Spec.TimeZone = "Mars/Olympus_Mons"
	tzErr := validator.validateSchedule(tzApp)
	if !errors.Is(tzErr, util.ErrInvalidTimeZone) {
		t.Fatalf("expected wrapped util.ErrInvalidTimeZone, got %v", tzErr)
	}

	cronApp := newScheduledSparkApplication()
	cronApp.Spec.Schedule = "garbage"
	cronErr := validator.validateSchedule(cronApp)
	if !errors.Is(cronErr, util.ErrInvalidSchedule) {
		t.Fatalf("expected wrapped util.ErrInvalidSchedule, got %v", cronErr)
	}
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

		{"allow", true},    // case-sensitive
		{"Bogus", true},
		{"replace ", true}, // trailing space
	}

	for _, tt := range tests {
		t.Run(string(tt.policy), func(t *testing.T) {
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

	t.Run("negative successful limit is rejected", func(t *testing.T) {
		app := newScheduledSparkApplication()
		app.Spec.SuccessfulRunHistoryLimit = ptr.To(int32(-1))
		err := validator.validateHistoryLimits(app)
		if !errors.Is(err, ErrInvalidHistoryLimit) {
			t.Fatalf("expected ErrInvalidHistoryLimit, got %v", err)
		}
		// The detail in the message must still distinguish which field was bad,
		// since the sentinel covers both fields.
		if !strings.Contains(err.Error(), "successfulRunHistoryLimit") {
			t.Fatalf("expected message to identify successfulRunHistoryLimit, got %q", err.Error())
		}
	})

	t.Run("negative failed limit is rejected", func(t *testing.T) {
		app := newScheduledSparkApplication()
		app.Spec.FailedRunHistoryLimit = ptr.To(int32(-2))
		err := validator.validateHistoryLimits(app)
		if !errors.Is(err, ErrInvalidHistoryLimit) {
			t.Fatalf("expected ErrInvalidHistoryLimit, got %v", err)
		}
		if !strings.Contains(err.Error(), "failedRunHistoryLimit") {
			t.Fatalf("expected message to identify failedRunHistoryLimit, got %q", err.Error())
		}
	})
}

func TestScheduledSparkApplicationValidatorTemplateForwarding(t *testing.T) {
	validator := newTestScheduledValidator(t)

	t.Run("template node selector conflict propagates", func(t *testing.T) {
		// Prove the SSA admission classifies the failure as a template error
		// (errors.Is ErrInvalidTemplate) AND that the wrapped chain originates
		// from the SparkApplication validator (a direct call against the same
		// template produces an equivalent error message). Together these prevent
		// a regression where an unrelated code path could satisfy substring-only
		// assertions on "node selector cannot be defined".
		app := newScheduledSparkApplication()
		app.Spec.Template.NodeSelector = map[string]string{"role": "shared"}
		app.Spec.Template.Driver.NodeSelector = map[string]string{"role": "driver"}

		_, err := validator.ValidateCreate(context.Background(), app)
		if !errors.Is(err, ErrInvalidTemplate) {
			t.Fatalf("expected ErrInvalidTemplate, got %v", err)
		}

		// Cross-check: feeding the same template directly to the SA validator
		// produces an equivalent error. The SSA path must therefore have
		// delegated through the SA validator and not produced this error from
		// some other code path that happens to mention node selectors.
		saValidator := newTestValidator(t, false)
		templateApp := &v1beta2.SparkApplication{ObjectMeta: app.ObjectMeta, Spec: app.Spec.Template}
		saErr := saValidator.validateSpec(context.Background(), templateApp)
		if saErr == nil {
			t.Fatalf("precondition: SA validator should reject this template directly")
		}
		if !strings.Contains(err.Error(), saErr.Error()) {
			t.Fatalf("expected SSA error to contain SA validator's error %q, got %q", saErr.Error(), err.Error())
		}
	})

	t.Run("template pod-template version requirement propagates", func(t *testing.T) {
		app := newScheduledSparkApplication()
		// Upstream SparkApplication validator requires Spark >= 3.0.0 when a pod
		// template is supplied; force the failure path by overriding the version.
		app.Spec.Template.SparkVersion = "2.4.5"
		app.Spec.Template.Driver.Template = &corev1.PodTemplateSpec{}

		_, err := validator.ValidateCreate(context.Background(), app)
		if !errors.Is(err, ErrInvalidTemplate) {
			t.Fatalf("expected ErrInvalidTemplate, got %v", err)
		}
		// SA validator currently emits this as a plain error; substring is the
		// best we can do until the SA validator gains its own sentinels. This is
		// the one place where message-content matching is unavoidable.
		if !strings.Contains(err.Error(), "pod template feature requires") {
			t.Fatalf("expected pod-template version error in chain, got %q", err.Error())
		}
	})

	t.Run("validator without sparkAppValidator skips template validation", func(t *testing.T) {
		// If the operator is built without a delegate validator the SSA validator
		// must still run its own checks but skip template validation. Without this,
		// downstream consumers (e.g. tests) that legitimately want a thin validator
		// would be forced to construct a full SparkApplicationValidator.
		v := NewScheduledSparkApplicationValidator(nil)
		app := newScheduledSparkApplication()
		app.Spec.Template.NodeSelector = map[string]string{"role": "shared"}
		app.Spec.Template.Driver.NodeSelector = map[string]string{"role": "driver"}
		if _, err := v.ValidateCreate(context.Background(), app); err != nil {
			t.Fatalf("expected no error when sparkAppValidator is nil, got %v", err)
		}
	})
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
			app := newScheduledSparkApplication()
			app.ObjectMeta.Name = tt.appName

			_, err := validator.ValidateCreate(context.Background(), app)

			if tt.wantError {
				if !errors.Is(err, ErrInvalidName) {
					t.Errorf("validateName(%q): expected ErrInvalidName, got %v", tt.appName, err)
				}
				return
			}
			if err != nil {
				t.Errorf("validateName(%q): expected no error, got %v", tt.appName, err)
			}
		})
	}
}
