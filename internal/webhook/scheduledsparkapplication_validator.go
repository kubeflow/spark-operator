/*
Copyright 2024 The Kubeflow Authors.

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

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:admissionReviewVersions=v1,failurePolicy=fail,groups=sparkoperator.k8s.io,matchPolicy=Exact,mutating=false,name=validate-scheduledsparkapplication.sparkoperator.k8s.io,path=/validate-sparkoperator-k8s-io-v1beta2-scheduledsparkapplication,reinvocationPolicy=Never,resources=scheduledsparkapplications,sideEffects=None,verbs=create;update,versions=v1beta2,webhookVersions=v1

// ErrInvalidName is returned when the ScheduledSparkApplication name is invalid.
var ErrInvalidName = errors.New("invalid ScheduledSparkApplication name")

// ErrEmptySchedule is returned when the schedule is empty.
var ErrEmptySchedule = errors.New("spec.schedule must not be empty")

// ErrInvalidSchedule is returned when the schedule cannot be parsed.
var ErrInvalidSchedule = errors.New("invalid spec.schedule/spec.timeZone")

// ErrInvalidConcurrencyPolicy is returned when the concurrency policy is not allowed.
var ErrInvalidConcurrencyPolicy = errors.New("invalid spec.concurrencyPolicy")

// ErrInvalidHistoryLimit is returned when a run history limit is negative.
var ErrInvalidHistoryLimit = errors.New("invalid run history limit")

// ErrInvalidTemplate is returned when the embedded SparkApplication template is invalid.
var ErrInvalidTemplate = errors.New("invalid ScheduledSparkApplication template")

type ScheduledSparkApplicationValidator struct {
	sparkAppValidator *SparkApplicationValidator
}

// NewScheduledSparkApplicationValidator creates a validator that delegates template validation.
func NewScheduledSparkApplicationValidator(sparkAppValidator *SparkApplicationValidator) *ScheduledSparkApplicationValidator {
	return &ScheduledSparkApplicationValidator{
		sparkAppValidator: sparkAppValidator,
	}
}

var _ admission.Validator[*v1beta2.ScheduledSparkApplication] = &ScheduledSparkApplicationValidator{}

// ValidateCreate implements admission.Validator.
func (v *ScheduledSparkApplicationValidator) ValidateCreate(ctx context.Context, app *v1beta2.ScheduledSparkApplication) (warnings admission.Warnings, err error) {
	if app == nil {
		return nil, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("Validating ScheduledSparkApplication create")
	// Validate metadata.name early to prevent downstream Service creation failures
	if err := v.validateName(app.Name); err != nil {
		return nil, err
	}
	if err := v.validate(ctx, app); err != nil {
		return nil, err
	}
	return deprecationWarnings(&app.Spec.Template, field.NewPath("spec", "template")), nil
}

// ValidateUpdate implements admission.Validator.
func (v *ScheduledSparkApplicationValidator) ValidateUpdate(ctx context.Context, oldApp *v1beta2.ScheduledSparkApplication, newApp *v1beta2.ScheduledSparkApplication) (warnings admission.Warnings, err error) {
	if oldApp == nil || newApp == nil {
		return nil, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("Validating ScheduledSparkApplication update")
	// Name is immutable in Kubernetes, but validate anyway for safety in case of admission reconcilers
	if err := v.validateName(newApp.Name); err != nil {
		return nil, err
	}

	// Skip validating when spec does not change.
	if equality.Semantic.DeepEqual(oldApp.Spec, newApp.Spec) {
		return nil, nil
	}

	if err := v.validate(ctx, newApp); err != nil {
		return nil, err
	}
	return deprecationWarnings(&newApp.Spec.Template, field.NewPath("spec", "template")), nil
}

// ValidateDelete implements admission.Validator.
func (v *ScheduledSparkApplicationValidator) ValidateDelete(ctx context.Context, app *v1beta2.ScheduledSparkApplication) (warnings admission.Warnings, err error) {
	if app == nil {
		return nil, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("Validating ScheduledSparkApplication delete")
	return nil, nil
}

// validate runs all spec-level checks for a ScheduledSparkApplication; errors here become webhook denials.
func (v *ScheduledSparkApplicationValidator) validate(ctx context.Context, app *v1beta2.ScheduledSparkApplication) error {
	if err := v.validateSchedule(app); err != nil {
		return err
	}
	if err := v.validateConcurrencyPolicy(app.Spec.ConcurrencyPolicy); err != nil {
		return err
	}
	if err := v.validateHistoryLimits(app); err != nil {
		return err
	}
	if v.sparkAppValidator != nil {
		// Validate the embedded template via the shared SparkApplication validator.
		sparkApp := &v1beta2.SparkApplication{
			ObjectMeta: app.ObjectMeta,
			Spec:       app.Spec.Template,
		}
		if err := v.sparkAppValidator.validateSpecWithPath(ctx, sparkApp, field.NewPath("spec", "template")); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidTemplate, err)
		}
	}
	return nil
}

// validateSchedule rejects empty or unparseable cron schedules and unknown
// timezones. Parsing is delegated to util.ParseSchedule so the controller and
// webhook agree; admission is stricter than the suspended-state runtime path,
// which tolerates invalid schedules.
func (v *ScheduledSparkApplicationValidator) validateSchedule(app *v1beta2.ScheduledSparkApplication) error {
	if strings.TrimSpace(app.Spec.Schedule) == "" {
		return ErrEmptySchedule
	}
	if _, err := util.ParseSchedule(app.Spec.Schedule, app.Spec.TimeZone); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSchedule, err)
	}
	return nil
}

// validateConcurrencyPolicy rejects values outside the documented set. An empty
// value is accepted; the controller treats it as Allow (defaulter not implemented yet).
func (v *ScheduledSparkApplicationValidator) validateConcurrencyPolicy(policy v1beta2.ConcurrencyPolicy) error {
	switch policy {
	case "", v1beta2.ConcurrencyAllow, v1beta2.ConcurrencyForbid, v1beta2.ConcurrencyReplace:
		return nil
	default:
		return fmt.Errorf("%w %q: must be one of %q, %q, %q", ErrInvalidConcurrencyPolicy, policy, v1beta2.ConcurrencyAllow, v1beta2.ConcurrencyForbid, v1beta2.ConcurrencyReplace)
	}
}

// validateHistoryLimits rejects negative values for the run-history pointers.
// Nil is accepted (the controller applies its own default).
func (v *ScheduledSparkApplicationValidator) validateHistoryLimits(app *v1beta2.ScheduledSparkApplication) error {
	if l := app.Spec.SuccessfulRunHistoryLimit; l != nil && *l < 0 {
		return fmt.Errorf("%w: spec.successfulRunHistoryLimit must be >= 0, got %d", ErrInvalidHistoryLimit, *l)
	}
	if l := app.Spec.FailedRunHistoryLimit; l != nil && *l < 0 {
		return fmt.Errorf("%w: spec.failedRunHistoryLimit must be >= 0, got %d", ErrInvalidHistoryLimit, *l)
	}
	return nil
}

// validateName ensures the ScheduledSparkApplication metadata.name, when combined with suffixes,
// results in a valid DNS-1035 label for Kubernetes Service names. This prevents failures later
// when creating SparkApplication resources that require DNS-1035 compliant names.
func (v *ScheduledSparkApplicationValidator) validateName(name string) error {
	if errs := validation.IsDNS1035Label(name); len(errs) > 0 {
		return fmt.Errorf("%w %q: %s", ErrInvalidName, name, strings.Join(errs, ", "))
	}
	return nil
}
