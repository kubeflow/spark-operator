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

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kubeflow/spark-operator/api/v1beta2"
)

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:admissionReviewVersions=v1,failurePolicy=fail,groups=sparkoperator.k8s.io,matchPolicy=Exact,mutating=false,name=validate-scheduledsparkapplication.sparkoperator.k8s.io,path=/validate-sparkoperator-k8s-io-v1beta2-scheduledsparkapplication,reinvocationPolicy=Never,resources=scheduledsparkapplications,sideEffects=None,verbs=create;update,versions=v1beta2,webhookVersions=v1

type ScheduledSparkApplicationValidator struct{}

// NewScheduledSparkApplicationValidator creates a new ScheduledSparkApplicationValidator instance.
func NewScheduledSparkApplicationValidator() *ScheduledSparkApplicationValidator {
	return &ScheduledSparkApplicationValidator{}
}

var _ admission.CustomValidator = &ScheduledSparkApplicationValidator{}

// ValidateCreate implements admission.CustomValidator.
func (v *ScheduledSparkApplicationValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (warnings admission.Warnings, err error) {
	app, ok := obj.(*v1beta2.ScheduledSparkApplication)
	if !ok {
		return nil, nil
	}
	logger.Info("Validating SchedulingSparkApplication create", "name", app.Name, "namespace", app.Namespace)
	if err := v.validate(app); err != nil {
		return nil, err
	}
	return nil, nil
}

// ValidateUpdate implements admission.CustomValidator.
func (v *ScheduledSparkApplicationValidator) ValidateUpdate(ctx context.Context, oldObj runtime.Object, newObj runtime.Object) (warnings admission.Warnings, err error) {
	newApp, ok := newObj.(*v1beta2.ScheduledSparkApplication)
	if !ok {
		return nil, nil
	}
	logger.Info("Validating SchedulingSparkApplication update", "name", newApp.Name, "namespace", newApp.Namespace)
	if err := v.validate(newApp); err != nil {
		return nil, err
	}
	return nil, nil
}

// ValidateDelete implements admission.CustomValidator.
func (v *ScheduledSparkApplicationValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (warnings admission.Warnings, err error) {
	app, ok := obj.(*v1beta2.ScheduledSparkApplication)
	if !ok {
		return nil, nil
	}
	logger.Info("Validating ScheduledSparkApplication delete", "name", app.Name, "namespace", app.Namespace)
	return nil, nil
}

func (v *ScheduledSparkApplicationValidator) validate(_ *v1beta2.ScheduledSparkApplication) error {
	// TODO: implement validate logic
	return nil
}
