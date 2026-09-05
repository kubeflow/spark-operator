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

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:admissionReviewVersions=v1,failurePolicy=fail,groups=sparkoperator.k8s.io,matchPolicy=Exact,mutating=false,name=validate-scheduledsparkapplication.sparkoperator.k8s.io,path=/validate-sparkoperator-k8s-io-v1beta2-scheduledsparkapplication,reinvocationPolicy=Never,resources=scheduledsparkapplications,sideEffects=None,verbs=create;update,versions=v1beta2,webhookVersions=v1

var scheduledSparkApplicationGroupKind = v1beta2.SchemeGroupVersion.WithKind("ScheduledSparkApplication").GroupKind()

type ScheduledSparkApplicationValidator struct{}

// NewScheduledSparkApplicationValidator creates a new ScheduledSparkApplicationValidator instance.
func NewScheduledSparkApplicationValidator() *ScheduledSparkApplicationValidator {
	return &ScheduledSparkApplicationValidator{}
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
	if err := newInvalidError(scheduledSparkApplicationGroupKind, app.Name, validateObjectName(app.Name)); err != nil {
		return nil, err
	}
	if err := newInvalidError(scheduledSparkApplicationGroupKind, app.Name, v.validate(app)); err != nil {
		return nil, err
	}
	return nil, nil
}

// ValidateUpdate implements admission.Validator.
func (v *ScheduledSparkApplicationValidator) ValidateUpdate(ctx context.Context, oldApp *v1beta2.ScheduledSparkApplication, newApp *v1beta2.ScheduledSparkApplication) (warnings admission.Warnings, err error) {
	if oldApp == nil || newApp == nil {
		return nil, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("Validating ScheduledSparkApplication update")
	// Name is immutable in Kubernetes, but validate anyway for safety in case of admission reconcilers
	if err := newInvalidError(scheduledSparkApplicationGroupKind, newApp.Name, validateObjectName(newApp.Name)); err != nil {
		return nil, err
	}

	// Skip validating when spec does not change.
	if equality.Semantic.DeepEqual(oldApp.Spec, newApp.Spec) {
		return nil, nil
	}

	if err := newInvalidError(scheduledSparkApplicationGroupKind, newApp.Name, v.validate(newApp)); err != nil {
		return nil, err
	}
	return nil, nil
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

func (v *ScheduledSparkApplicationValidator) validate(app *v1beta2.ScheduledSparkApplication) field.ErrorList {
	templatePath := field.NewPath("spec", "template")

	if errs := validateSparkConf(templatePath.Child("sparkConf"), app.Spec.Template.SparkConf, app.Namespace); len(errs) > 0 {
		return errs
	}
	return validateConfigMaps(&app.Spec.Template, templatePath)
}
