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
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kubeflow/spark-operator/api/v1beta2"
)

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-sparkoperator-k8s-io-v1beta2-sparkapplication,mutating=false,failurePolicy=fail,sideEffects=None,groups=sparkoperator.k8s.io,resources=sparkapplications,verbs=create;update,versions=v1beta2,name=vsparkapplication.kb.io,admissionReviewVersions=v1

type SparkApplicationValidator struct{}

// NewSparkApplicationValidator creates a new SparkApplicationValidator instance.
func NewSparkApplicationValidator() *SparkApplicationValidator {
	return &SparkApplicationValidator{}
}

var _ admission.CustomValidator = &SparkApplicationValidator{}

// ValidateCreate implements admission.CustomValidator.
func (v *SparkApplicationValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (warnings admission.Warnings, err error) {
	app, ok := obj.(*v1beta2.SparkApplication)
	if !ok {
		return nil, nil
	}
	logger.Info("Validating SparkApplication create", "name", app.Name, "namespace", app.Namespace)
	if err := v.validate(app); err != nil {
		return nil, err
	}
	return nil, nil
}

// ValidateUpdate implements admission.CustomValidator.
func (v *SparkApplicationValidator) ValidateUpdate(ctx context.Context, oldObj runtime.Object, newObj runtime.Object) (warnings admission.Warnings, err error) {
	newApp, ok := newObj.(*v1beta2.SparkApplication)
	if !ok {
		return nil, nil
	}
	logger.Info("Validating SparkApplication update", "name", newApp.Name, "namespace", newApp.Namespace)
	if err := v.validate(newApp); err != nil {
		return nil, err
	}
	return nil, nil
}

// ValidateDelete implements admission.CustomValidator.
func (v *SparkApplicationValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (warnings admission.Warnings, err error) {
	app, ok := obj.(*v1beta2.SparkApplication)
	if !ok {
		return nil, nil
	}
	logger.Info("Validating SparkApplication delete", "name", app.Name, "namespace", app.Namespace)
	return nil, nil
}

func (v *SparkApplicationValidator) validate(app *v1beta2.SparkApplication) error {
	appSpec := app.Spec
	driverSpec := appSpec.Driver
	executorSpec := appSpec.Executor

	if appSpec.NodeSelector != nil && (driverSpec.NodeSelector != nil || executorSpec.NodeSelector != nil) {
		return fmt.Errorf("NodeSelector property can be defined at SparkApplication or at any of Driver,Executor")
	}

	servicePorts := make(map[int32]bool)
	ingressURLFormats := make(map[string]bool)
	for _, item := range appSpec.DriverIngressOptions {
		if item.ServicePort == nil {
			return fmt.Errorf("DriverIngressOptions has nill ServicePort")
		}
		if servicePorts[*item.ServicePort] {
			return fmt.Errorf("DriverIngressOptions has duplicate ServicePort: %d", *item.ServicePort)
		}
		servicePorts[*item.ServicePort] = true

		if item.IngressURLFormat == "" {
			return fmt.Errorf("DriverIngressOptions has empty IngressURLFormat")
		}
		if ingressURLFormats[item.IngressURLFormat] {
			return fmt.Errorf("DriverIngressOptions has duplicate IngressURLFormat: %s", item.IngressURLFormat)
		}
		ingressURLFormats[item.IngressURLFormat] = true
	}

	return nil
}
