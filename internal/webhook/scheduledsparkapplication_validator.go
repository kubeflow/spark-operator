/*
Copyright 2024 The Kubeflow authors.

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
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// Sentinel errors for ScheduledSparkApplication admission. Wrapped errors
// returned by the validator can be classified with errors.Is so callers
// (notably tests, but also any future status-mapping logic in the webhook
// envelope) can react to a category without parsing error message text.
//
// The wrapped chain layers like this for example:
//
//	ErrInvalidTemplate -> SparkApplicationValidator's own error
//
// so a caller can errors.Is(err, ErrInvalidTemplate) to know which validator
// rejected the spec, while still surfacing the underlying detail to the user.
var (
	ErrInvalidName              = errors.New("invalid ScheduledSparkApplication name")
	ErrEmptySchedule            = errors.New("spec.schedule must not be empty")
	ErrInvalidSchedule          = errors.New("invalid spec.schedule/spec.timeZone")
	ErrInvalidConcurrencyPolicy = errors.New("invalid spec.concurrencyPolicy")
	ErrInvalidHistoryLimit      = errors.New("invalid run history limit")
	ErrInvalidTemplate          = errors.New("invalid ScheduledSparkApplication template")
)

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:admissionReviewVersions=v1,failurePolicy=fail,groups=sparkoperator.k8s.io,matchPolicy=Exact,mutating=false,name=validate-scheduledsparkapplication.sparkoperator.k8s.io,path=/validate-sparkoperator-k8s-io-v1beta2-scheduledsparkapplication,reinvocationPolicy=Never,resources=scheduledsparkapplications,sideEffects=None,verbs=create;update,versions=v1beta2,webhookVersions=v1

// ScheduledSparkApplicationValidator validates the spec of a ScheduledSparkApplication
// at admission time. It delegates validation of the embedded SparkApplication template
// to a SparkApplicationValidator so that the same checks the SparkApplication webhook
// runs are also enforced when a SparkApplication is materialized via a schedule.
type ScheduledSparkApplicationValidator struct {
	// sparkAppValidator is used to run the SparkApplication-level checks against
	// Spec.Template. It must not be nil for non-trivial validation; constructors
	// should always provide one. The pointer (rather than the concrete value) lets
	// us reuse the SparkApplication validator's existing client and configuration.
	sparkAppValidator *SparkApplicationValidator
}

// NewScheduledSparkApplicationValidator creates a new ScheduledSparkApplicationValidator
// that delegates template validation to the supplied SparkApplicationValidator.
func NewScheduledSparkApplicationValidator(sparkAppValidator *SparkApplicationValidator) *ScheduledSparkApplicationValidator {
	return &ScheduledSparkApplicationValidator{
		sparkAppValidator: sparkAppValidator,
	}
}

var _ admission.CustomValidator = &ScheduledSparkApplicationValidator{}

// ValidateCreate implements admission.CustomValidator.
func (v *ScheduledSparkApplicationValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (warnings admission.Warnings, err error) {
	app, ok := obj.(*v1beta2.ScheduledSparkApplication)
	if !ok {
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
	return nil, nil
}

// ValidateUpdate implements admission.CustomValidator.
func (v *ScheduledSparkApplicationValidator) ValidateUpdate(ctx context.Context, oldObj runtime.Object, newObj runtime.Object) (warnings admission.Warnings, err error) {
	oldApp, _ := oldObj.(*v1beta2.ScheduledSparkApplication)
	newApp, ok := newObj.(*v1beta2.ScheduledSparkApplication)
	if !ok {
		return nil, nil
	}
	logger := log.FromContext(ctx)
	logger.Info("Validating ScheduledSparkApplication update")
	// Name is immutable in Kubernetes, but validate anyway for safety in case of admission reconcilers
	if err := v.validateName(newApp.Name); err != nil {
		return nil, err
	}

	// Skip validating when the spec did not change. Mirrors the SparkApplication
	// validator behaviour so status writebacks that round-trip through update
	// admission don't repeatedly re-run cron parsing or template validation.
	if oldApp != nil && equality.Semantic.DeepEqual(oldApp.Spec, newApp.Spec) {
		return nil, nil
	}

	if err := v.validate(ctx, newApp); err != nil {
		return nil, err
	}
	return nil, nil
}

// ValidateDelete implements admission.CustomValidator.
func (v *ScheduledSparkApplicationValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (warnings admission.Warnings, err error) {
	if _, ok := obj.(*v1beta2.ScheduledSparkApplication); !ok {
		return nil, nil
	}
	logger := log.FromContext(ctx)
	logger.Info("Validating ScheduledSparkApplication delete")
	return nil, nil
}

// validate runs all spec-level checks for a ScheduledSparkApplication. Errors
// returned here become webhook denials, so the user sees them on `kubectl apply`
// rather than only as a Status.ScheduleState=FailedValidation after the fact.
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
		// Validate the embedded SparkApplication template using the same spec-level
		// checks the SparkApplication webhook runs. We synthesize a SparkApplication
		// so the existing validateSpec contract is preserved, including its logging.
		templateApp := &v1beta2.SparkApplication{
			ObjectMeta: app.ObjectMeta,
			Spec:       app.Spec.Template,
		}
		if err := v.sparkAppValidator.validateSpec(ctx, templateApp); err != nil {
			return fmt.Errorf("%w: %w", ErrInvalidTemplate, err)
		}
	}
	return nil
}

// validateSchedule rejects empty or unparseable cron schedules and unknown
// timezones. The actual parsing is delegated to util.ParseSchedule so the
// validator and the controller agree on the set of accepted inputs; a change
// to one is automatically reflected in the other.
//
// Note: the controller short-circuits reconciliation when Spec.Suspend is true
// (see internal/controller/scheduledsparkapplication/controller.go), so a
// suspended SSA with a garbage schedule is tolerated at runtime. Admission is
// intentionally stricter: a persisted invalid schedule that gets unsuspended
// later would silently fail at the next reconcile, which is harder to debug
// than a synchronous rejection at apply time.
func (v *ScheduledSparkApplicationValidator) validateSchedule(app *v1beta2.ScheduledSparkApplication) error {
	if strings.TrimSpace(app.Spec.Schedule) == "" {
		return ErrEmptySchedule
	}
	if _, err := util.ParseSchedule(app.Spec.Schedule, app.Spec.TimeZone); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSchedule, err)
	}
	return nil
}

// validateConcurrencyPolicy rejects values outside the documented enum. An
// empty value is accepted today because the SSA defaulter is a no-op and
// surfacing a denial here would force every existing user to set the field
// explicitly. The controller's shouldStartNextRun() returns (false, nil) for
// an unrecognised policy, so an empty value silently never runs anything;
// once the defaulter is implemented to populate "" -> ConcurrencyAllow the
// empty case can be removed from this allowlist.
func (v *ScheduledSparkApplicationValidator) validateConcurrencyPolicy(policy v1beta2.ConcurrencyPolicy) error {
	switch policy {
	case "", v1beta2.ConcurrencyAllow, v1beta2.ConcurrencyForbid, v1beta2.ConcurrencyReplace:
		return nil
	default:
		return fmt.Errorf(
			"%w %q: must be one of %q, %q, %q",
			ErrInvalidConcurrencyPolicy,
			policy,
			v1beta2.ConcurrencyAllow,
			v1beta2.ConcurrencyForbid,
			v1beta2.ConcurrencyReplace,
		)
	}
}

// validateHistoryLimits rejects negative values for the run-history pointers.
// Nil is accepted (the controller will apply its own default).
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
