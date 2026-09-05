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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:admissionReviewVersions=v1,failurePolicy=fail,groups=sparkoperator.k8s.io,matchPolicy=Exact,mutating=false,name=validate-sparkapplication.sparkoperator.k8s.io,path=/validate-sparkoperator-k8s-io-v1beta2-sparkapplication,reinvocationPolicy=Never,resources=sparkapplications,sideEffects=None,verbs=create;update,versions=v1beta2,webhookVersions=v1

var sparkApplicationGroupKind = v1beta2.SchemeGroupVersion.WithKind("SparkApplication").GroupKind()

type SparkApplicationValidator struct {
	client client.Client

	enableResourceQuotaEnforcement bool
}

// NewSparkApplicationValidator creates a new SparkApplicationValidator instance.
func NewSparkApplicationValidator(client client.Client, enableResourceQuotaEnforcement bool) *SparkApplicationValidator {
	return &SparkApplicationValidator{
		client: client,

		enableResourceQuotaEnforcement: enableResourceQuotaEnforcement,
	}
}

var _ admission.Validator[*v1beta2.SparkApplication] = &SparkApplicationValidator{}

// ValidateCreate implements admission.Validator.
func (v *SparkApplicationValidator) ValidateCreate(ctx context.Context, app *v1beta2.SparkApplication) (warnings admission.Warnings, err error) {
	if app == nil {
		return nil, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("Validating SparkApplication create", "state", util.GetApplicationState(app))

	// Validate metadata.name early to prevent downstream Service creation failures
	if errs := v.validateName(app.Name); len(errs) > 0 {
		return nil, apierrors.NewInvalid(sparkApplicationGroupKind, app.Name, errs)
	}
	if errs := v.validateSpec(app); len(errs) > 0 {
		return nil, apierrors.NewInvalid(sparkApplicationGroupKind, app.Name, errs)
	}

	if v.enableResourceQuotaEnforcement {
		if err := v.validateResourceUsage(ctx, app); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

// ValidateUpdate implements admission.Validator.
func (v *SparkApplicationValidator) ValidateUpdate(ctx context.Context, oldApp *v1beta2.SparkApplication, newApp *v1beta2.SparkApplication) (warnings admission.Warnings, err error) {
	if oldApp == nil || newApp == nil {
		return nil, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("Validating SparkApplication update", "state", util.GetApplicationState(newApp))

	// Name is immutable in Kubernetes, but validate anyway for safety in case of admission reconcilers
	if errs := v.validateName(newApp.Name); len(errs) > 0 {
		return nil, apierrors.NewInvalid(sparkApplicationGroupKind, newApp.Name, errs)
	}

	// Skip validating when spec does not change.
	if equality.Semantic.DeepEqual(oldApp.Spec, newApp.Spec) {
		return nil, nil
	}

	if errs := v.validateSpec(newApp); len(errs) > 0 {
		return nil, apierrors.NewInvalid(sparkApplicationGroupKind, newApp.Name, errs)
	}

	// Validate SparkApplication resource usage when resource quota enforcement is enabled.
	if v.enableResourceQuotaEnforcement {
		if err := v.validateResourceUsage(ctx, newApp); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

// ValidateDelete implements admission.Validator.
func (v *SparkApplicationValidator) ValidateDelete(ctx context.Context, app *v1beta2.SparkApplication) (warnings admission.Warnings, err error) {
	if app == nil {
		return nil, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("Validating SparkApplication delete", "state", util.GetApplicationState(app))
	return nil, nil
}

func (v *SparkApplicationValidator) validateSpec(app *v1beta2.SparkApplication) field.ErrorList {
	specPath := field.NewPath("spec")

	if errs := v.validateSparkVersion(specPath, app); len(errs) > 0 {
		return errs
	}

	if app.Spec.NodeSelector != nil && (app.Spec.Driver.NodeSelector != nil || app.Spec.Executor.NodeSelector != nil) {
		return field.ErrorList{field.Forbidden(specPath.Child("nodeSelector"), "cannot be set together with spec.driver.nodeSelector or spec.executor.nodeSelector")}
	}

	if errs := validateDriverIngressOptions(specPath.Child("driverIngressOptions"), app.Spec.DriverIngressOptions); len(errs) > 0 {
		return errs
	}

	if errs := validateSparkConf(specPath.Child("sparkConf"), app.Spec.SparkConf, app.Namespace); len(errs) > 0 {
		return errs
	}

	return validateConfigMaps(&app.Spec, specPath)
}

// validateName ensures the SparkApplication metadata.name is a valid DNS-1035 label
// This prevents failures later when creating related resources like Services which
// require DNS-1035 compliant names.
func (v *SparkApplicationValidator) validateName(name string) field.ErrorList {
	return newInvalidErrors(field.NewPath("metadata", "name"), name, validation.IsDNS1035Label(name))
}

func (v *SparkApplicationValidator) validateSparkVersion(path *field.Path, app *v1beta2.SparkApplication) field.ErrorList {
	// The pod template feature requires Spark version 3.0.0 or higher.
	if app.Spec.Driver.Template == nil && app.Spec.Executor.Template == nil {
		return nil
	}
	if util.CompareSemanticVersion(app.Spec.SparkVersion, "3.0.0") < 0 {
		return field.ErrorList{field.Invalid(path.Child("sparkVersion"), app.Spec.SparkVersion, "pod template feature requires Spark version 3.0.0 or higher")}
	}
	return nil
}

func validateDriverIngressOptions(path *field.Path, options []v1beta2.DriverIngressConfiguration) field.ErrorList {
	servicePorts := make(map[int32]bool)
	ingressURLFormats := make(map[string]bool)
	for i, item := range options {
		itemPath := path.Index(i)

		if item.ServicePort == nil {
			return field.ErrorList{field.Required(itemPath.Child("servicePort"), "")}
		}
		if servicePorts[*item.ServicePort] {
			return field.ErrorList{field.Duplicate(itemPath.Child("servicePort"), *item.ServicePort)}
		}
		servicePorts[*item.ServicePort] = true

		if item.IngressURLFormat == "" {
			return field.ErrorList{field.Required(itemPath.Child("ingressURLFormat"), "")}
		}
		if ingressURLFormats[item.IngressURLFormat] {
			return field.ErrorList{field.Duplicate(itemPath.Child("ingressURLFormat"), item.IngressURLFormat)}
		}
		ingressURLFormats[item.IngressURLFormat] = true
	}
	return nil
}

func (v *SparkApplicationValidator) validateResourceUsage(ctx context.Context, app *v1beta2.SparkApplication) error {
	requests, err := getResourceList(app)
	if err != nil {
		return fmt.Errorf("failed to calculate resource requests: %w", err)
	}

	resourceQuotaList := &corev1.ResourceQuotaList{}
	if err := v.client.List(ctx, resourceQuotaList, client.InNamespace(app.Namespace)); err != nil {
		return fmt.Errorf("failed to list resource quotas: %w", err)
	}

	for _, resourceQuota := range resourceQuotaList.Items {
		// Scope selectors not currently supported, ignore any ResourceQuota that does not match everything.
		// TODO: Add support for scope selectors.
		if resourceQuota.Spec.ScopeSelector != nil || len(resourceQuota.Spec.Scopes) > 0 {
			continue
		}

		if !validateResourceQuota(requests, resourceQuota) {
			// Exhausting a quota does not make any single field invalid, so this stays the
			// Forbidden status the API server uses for its own quota admission failures.
			return apierrors.NewForbidden(
				v1beta2.Resource("sparkapplications"),
				app.Name,
				fmt.Errorf("exceeds resource quota %q", resourceQuota.Namespace+"/"+resourceQuota.Name),
			)
		}
	}

	return nil
}
