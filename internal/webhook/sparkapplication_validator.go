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
	"regexp"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:admissionReviewVersions=v1,failurePolicy=fail,groups=sparkoperator.k8s.io,matchPolicy=Exact,mutating=false,name=validate-sparkapplication.sparkoperator.k8s.io,path=/validate-sparkoperator-k8s-io-v1beta2-sparkapplication,reinvocationPolicy=Never,resources=sparkapplications,sideEffects=None,verbs=create;update,versions=v1beta2,webhookVersions=v1

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

var _ admission.CustomValidator = &SparkApplicationValidator{}

// ValidateCreate implements admission.CustomValidator.
func (v *SparkApplicationValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (warnings admission.Warnings, err error) {
	app, ok := obj.(*v1beta2.SparkApplication)
	if !ok {
		return nil, nil
	}
	logger.Info("Validating SparkApplication create", "name", app.Name, "namespace", app.Namespace, "state", util.GetApplicationState(app))
	if err := v.validateSpec(ctx, app); err != nil {
		return nil, err
	}

	if v.enableResourceQuotaEnforcement {
		if err := v.validateResourceUsage(ctx, app); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

// ValidateUpdate implements admission.CustomValidator.
func (v *SparkApplicationValidator) ValidateUpdate(ctx context.Context, oldObj runtime.Object, newObj runtime.Object) (warnings admission.Warnings, err error) {
	oldApp, ok := oldObj.(*v1beta2.SparkApplication)
	if !ok {
		return nil, nil
	}

	newApp, ok := newObj.(*v1beta2.SparkApplication)
	if !ok {
		return nil, nil
	}

	logger.Info("Validating SparkApplication update", "name", newApp.Name, "namespace", newApp.Namespace)

	// Skip validating when spec does not change.
	if equality.Semantic.DeepEqual(oldApp.Spec, newApp.Spec) {
		return nil, nil
	}

	if err := v.validateSpec(ctx, newApp); err != nil {
		return nil, err
	}

	// Validate SparkApplication resource usage when resource quota enforcement is enabled.
	if v.enableResourceQuotaEnforcement {
		if err := v.validateResourceUsage(ctx, newApp); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

// ValidateDelete implements admission.CustomValidator.
func (v *SparkApplicationValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (warnings admission.Warnings, err error) {
	app, ok := obj.(*v1beta2.SparkApplication)
	if !ok {
		return nil, nil
	}
	logger.Info("Validating SparkApplication delete", "name", app.Name, "namespace", app.Namespace, "state", util.GetApplicationState(app))
	return nil, nil
}

func (v *SparkApplicationValidator) validateSpec(_ context.Context, app *v1beta2.SparkApplication) error {
	logger.V(1).Info("Validating SparkApplication spec", "name", app.Name, "namespace", app.Namespace, "state", util.GetApplicationState(app))

	if err := v.validateSparkVersion(app); err != nil {
		return err
	}

	if app.Spec.NodeSelector != nil && (app.Spec.Driver.NodeSelector != nil || app.Spec.Executor.NodeSelector != nil) {
		return fmt.Errorf("node selector cannot be defined at both SparkApplication and Driver/Executor")
	}

	servicePorts := make(map[int32]bool)
	ingressURLFormats := make(map[string]bool)
	for _, item := range app.Spec.DriverIngressOptions {
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

	if app.Spec.Driver.SparkApplicationLabelsMutation != nil {
		path := field.NewPath("spec").Child("driver").Child("sparkApplicationLabelsMutation")
		if errs := validateSparkApplicationLabelsMutationSpec(path, app.Spec.Driver.SparkApplicationLabelsMutation); len(errs) > 0 {
			return errs.ToAggregate()
		}
	}
	if app.Spec.Executor.SparkApplicationLabelsMutation != nil {
		path := field.NewPath("spec").Child("executor").Child("sparkApplicationLabelsMutation")
		if errs := validateSparkApplicationLabelsMutationSpec(path, app.Spec.Executor.SparkApplicationLabelsMutation); len(errs) > 0 {
			return errs.ToAggregate()
		}
	}

	return nil
}

func (v *SparkApplicationValidator) validateSparkVersion(app *v1beta2.SparkApplication) error {
	// The pod template feature requires Spark version 3.0.0 or higher.
	if app.Spec.Driver.Template != nil || app.Spec.Executor.Template != nil {
		if util.CompareSemanticVersion(app.Spec.SparkVersion, "3.0.0") < 0 {
			return fmt.Errorf("pod template feature requires Spark version 3.0.0 or higher")
		}
	}
	return nil
}

func (v *SparkApplicationValidator) validateResourceUsage(ctx context.Context, app *v1beta2.SparkApplication) error {
	logger.V(1).Info("Validating SparkApplication resource usage", "name", app.Name, "namespace", app.Namespace, "state", util.GetApplicationState(app))

	requests, err := getResourceList(app)
	if err != nil {
		return fmt.Errorf("failed to calculate resource quests: %v", err)
	}

	resourceQuotaList := &corev1.ResourceQuotaList{}
	if err := v.client.List(ctx, resourceQuotaList, client.InNamespace(app.Namespace)); err != nil {
		return fmt.Errorf("failed to list resource quotas: %v", err)
	}

	for _, resourceQuota := range resourceQuotaList.Items {
		// Scope selectors not currently supported, ignore any ResourceQuota that does not match everything.
		// TODO: Add support for scope selectors.
		if resourceQuota.Spec.ScopeSelector != nil || len(resourceQuota.Spec.Scopes) > 0 {
			continue
		}

		if !validateResourceQuota(requests, resourceQuota) {
			return fmt.Errorf("failed to validate resource quota \"%s/%s\"", resourceQuota.Namespace, resourceQuota.Name)
		}
	}

	return nil
}

func validateSparkApplicationLabelsMutationSpec(path *field.Path, mutation *v1beta2.SparkApplicationLabelsMutationSpec) field.ErrorList {
	var allErrs field.ErrorList
	if mutation == nil {
		return nil
	}
	allErrs = append(allErrs, validateMutatingLabelKeyMatchConditions(path.Child("labelKeyMatches"), mutation.LabelKeyMatches)...)
	if len(allErrs) > 0 {
		return allErrs
	}
	return nil
}

func validateMutatingLabelKeyMatchConditions(path *field.Path, conditions []v1beta2.MutatingLabelKeyMatchCondition) field.ErrorList {
	var allErrs field.ErrorList
	for i := range conditions {
		allErrs = append(allErrs, validateMutatingLabelKeyCondition(path.Index(i), conditions[i])...)
	}
	if len(allErrs) > 0 {
		return allErrs
	}
	return nil
}

func validateMutatingLabelKeyCondition(path *field.Path, condition v1beta2.MutatingLabelKeyMatchCondition) field.ErrorList {
	var allErrs field.ErrorList

	if condition.Fixed == nil && condition.Regex == nil {
		allErrs = append(allErrs, field.Required(path, "must specify either fixed or regex"))
	} else if condition.Fixed != nil && condition.Regex != nil {
		allErrs = append(allErrs, field.Invalid(path, "", "only one of fixed or regex can be specified"))
	}

	if condition.Regex != nil {
		var err error
		if _, err = regexp.Compile(*condition.Regex); err != nil {
			allErrs = append(allErrs, field.Invalid(path.Child("regex"), *condition.Regex, fmt.Sprintf("invalid regex: %v", err)))
		}
	}

	if len(allErrs) > 0 {
		return allErrs
	}
	return nil
}
