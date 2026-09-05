/*
Copyright 2026 The Kubeflow authors.

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
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:admissionReviewVersions=v1,failurePolicy=fail,groups=sparkoperator.k8s.io,matchPolicy=Exact,mutating=false,name=validate-sparkconnect.sparkoperator.k8s.io,path=/validate-sparkoperator-k8s-io-v1alpha1-sparkconnect,reinvocationPolicy=Never,resources=sparkconnects,sideEffects=None,verbs=create;update,versions=v1alpha1,webhookVersions=v1

var sparkConnectGroupKind = v1alpha1.SchemeGroupVersion.WithKind("SparkConnect").GroupKind()

// SparkConnectValidator validates SparkConnect resources.
type SparkConnectValidator struct{}

// NewSparkConnectValidator creates a new SparkConnectValidator instance.
func NewSparkConnectValidator() *SparkConnectValidator {
	return &SparkConnectValidator{}
}

var _ admission.Validator[*v1alpha1.SparkConnect] = &SparkConnectValidator{}

// ValidateCreate implements admission.Validator.
func (v *SparkConnectValidator) ValidateCreate(ctx context.Context, sc *v1alpha1.SparkConnect) (warnings admission.Warnings, err error) {
	if sc == nil {
		return nil, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("Validating SparkConnect create", "name", sc.Name, "namespace", sc.Namespace)

	// Validate metadata.name early to prevent downstream Service creation failures
	if errs := v.validateName(sc.Name); len(errs) > 0 {
		return nil, apierrors.NewInvalid(sparkConnectGroupKind, sc.Name, errs)
	}

	if errs := v.validateSpec(sc); len(errs) > 0 {
		return nil, apierrors.NewInvalid(sparkConnectGroupKind, sc.Name, errs)
	}

	return nil, nil
}

// ValidateUpdate implements admission.Validator.
func (v *SparkConnectValidator) ValidateUpdate(ctx context.Context, oldSC *v1alpha1.SparkConnect, newSC *v1alpha1.SparkConnect) (warnings admission.Warnings, err error) {
	if oldSC == nil || newSC == nil {
		return nil, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("Validating SparkConnect update", "name", newSC.Name, "namespace", newSC.Namespace)

	// Name is immutable in Kubernetes, but validate anyway for safety
	if errs := v.validateName(newSC.Name); len(errs) > 0 {
		return nil, apierrors.NewInvalid(sparkConnectGroupKind, newSC.Name, errs)
	}

	// Skip validating when spec does not change.
	if equality.Semantic.DeepEqual(oldSC.Spec, newSC.Spec) {
		return nil, nil
	}

	if errs := v.validateSpec(newSC); len(errs) > 0 {
		return nil, apierrors.NewInvalid(sparkConnectGroupKind, newSC.Name, errs)
	}

	return nil, nil
}

// ValidateDelete implements admission.Validator.
func (v *SparkConnectValidator) ValidateDelete(ctx context.Context, sc *v1alpha1.SparkConnect) (warnings admission.Warnings, err error) {
	if sc == nil {
		return nil, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("Validating SparkConnect delete", "name", sc.Name, "namespace", sc.Namespace)

	return nil, nil
}

// validateName ensures the SparkConnect metadata.name is a valid DNS-1035 label.
// This prevents failures later when creating related resources like Services which
// require DNS-1035 compliant names. The operator derives a default Service name as
// "<name>-server", so we must also ensure that this derived name does not exceed
// the DNS-1035 maximum length.
func (v *SparkConnectValidator) validateName(name string) field.ErrorList {
	path := field.NewPath("metadata", "name")

	errs := newInvalidErrors(path, name, validation.IsDNS1035Label(name))
	if len(errs) > 0 {
		return errs
	}

	// Ensure the derived default Service name "<name>-server" also fits within the
	// DNS-1035 label length limit, so Service creation will not fail downstream.
	const serviceSuffix = "-server"
	maxBaseLen := validation.DNS1035LabelMaxLength - len(serviceSuffix)
	if len(name) > maxBaseLen {
		detail := fmt.Sprintf("must be at most %d characters so that the derived Service name %q does not exceed the DNS-1035 label length limit (%d characters)",
			maxBaseLen, name+serviceSuffix, validation.DNS1035LabelMaxLength)
		errs = append(errs, field.Invalid(path, name, detail))
	}

	return errs
}

// validateSpec validates the SparkConnect spec.
func (v *SparkConnectValidator) validateSpec(sc *v1alpha1.SparkConnect) field.ErrorList {
	specPath := field.NewPath("spec")

	if errs := v.validateSparkVersion(specPath, sc); len(errs) > 0 {
		return errs
	}

	if errs := v.validateImage(specPath, sc); len(errs) > 0 {
		return errs
	}

	if errs := v.validateDynamicAllocation(specPath.Child("dynamicAllocation"), sc.Spec.DynamicAllocation); len(errs) > 0 {
		return errs
	}

	if errs := validateMemoryString(specPath.Child("server", "memory"), sc.Spec.Server.Memory); len(errs) > 0 {
		return errs
	}

	if errs := validateMemoryString(specPath.Child("executor", "memory"), sc.Spec.Executor.Memory); len(errs) > 0 {
		return errs
	}

	return validateSparkConf(specPath.Child("sparkConf"), sc.Spec.SparkConf, sc.Namespace)
}

// validateSparkVersion validates the Spark version.
// Pod templates require Spark 3.0.0 or higher.
func (v *SparkConnectValidator) validateSparkVersion(path *field.Path, sc *v1alpha1.SparkConnect) field.ErrorList {
	versionPath := path.Child("sparkVersion")

	if sc.Spec.SparkVersion == "" {
		return field.ErrorList{field.Required(versionPath, "")}
	}

	// If pod templates are used, require Spark 3.0.0+
	if sc.Spec.Server.Template != nil || sc.Spec.Executor.Template != nil {
		if util.CompareSemanticVersion(sc.Spec.SparkVersion, "3.0.0") < 0 {
			return field.ErrorList{field.Invalid(versionPath, sc.Spec.SparkVersion, "pod template feature requires Spark version 3.0.0 or higher")}
		}
	}

	return nil
}

// validateImage validates that container images are available either from the spec-level image
// or from both the server and executor pod templates. This prevents the controller from entering
// a retry loop when it tries to reconcile a SparkConnect without valid images.
func (v *SparkConnectValidator) validateImage(path *field.Path, sc *v1alpha1.SparkConnect) field.ErrorList {
	// If a spec-level image is provided, it will be used for both server and executor.
	if sc.Spec.Image != nil && *sc.Spec.Image != "" {
		return nil
	}

	// Otherwise, require that the server and executor containers selected from the pod templates provide images.
	serverImageFound := podTemplateContainerImage(sc.Spec.Server.Template, common.SparkDriverContainerName) != ""
	executorImageFound := podTemplateContainerImage(sc.Spec.Executor.Template, common.Spark3DefaultExecutorContainerName) != ""

	if serverImageFound && executorImageFound {
		return nil
	}

	return field.ErrorList{field.Required(path.Child("image"), "must be specified here or in the selected server and executor template containers")}
}

func podTemplateContainerImage(template *corev1.PodTemplateSpec, containerName string) string {
	if template == nil || len(template.Spec.Containers) == 0 {
		return ""
	}

	container := util.GetContainerByNameOrFirst(
		template.Spec.Containers,
		containerName,
	)
	return container.Image
}

// validateDynamicAllocation validates DynamicAllocation configuration.
func (v *SparkConnectValidator) validateDynamicAllocation(path *field.Path, da *v1alpha1.DynamicAllocation) field.ErrorList {
	if da == nil || !da.Enabled {
		return nil
	}

	// Validate minExecutors <= maxExecutors
	if da.MinExecutors != nil && da.MaxExecutors != nil && *da.MinExecutors > *da.MaxExecutors {
		return field.ErrorList{field.Invalid(path.Child("minExecutors"), *da.MinExecutors,
			fmt.Sprintf("cannot be greater than maxExecutors (%d)", *da.MaxExecutors))}
	}

	// Validate initialExecutors is within range
	if da.InitialExecutors != nil {
		if da.MinExecutors != nil && *da.InitialExecutors < *da.MinExecutors {
			return field.ErrorList{field.Invalid(path.Child("initialExecutors"), *da.InitialExecutors,
				fmt.Sprintf("cannot be less than minExecutors (%d)", *da.MinExecutors))}
		}
		if da.MaxExecutors != nil && *da.InitialExecutors > *da.MaxExecutors {
			return field.ErrorList{field.Invalid(path.Child("initialExecutors"), *da.InitialExecutors,
				fmt.Sprintf("cannot be greater than maxExecutors (%d)", *da.MaxExecutors))}
		}
	}

	// Validate non-negative values
	if da.MinExecutors != nil && *da.MinExecutors < 0 {
		return field.ErrorList{field.Invalid(path.Child("minExecutors"), *da.MinExecutors, "must be non-negative")}
	}
	if da.MaxExecutors != nil && *da.MaxExecutors < 0 {
		return field.ErrorList{field.Invalid(path.Child("maxExecutors"), *da.MaxExecutors, "must be non-negative")}
	}
	if da.InitialExecutors != nil && *da.InitialExecutors < 0 {
		return field.ErrorList{field.Invalid(path.Child("initialExecutors"), *da.InitialExecutors, "must be non-negative")}
	}

	return nil
}

// validateMemoryString validates a Java/Spark memory string format.
// Valid formats: 1g, 512m, 1024k, 1073741824 (bytes)
func validateMemoryString(path *field.Path, memory *string) field.ErrorList {
	if memory == nil || *memory == "" {
		return nil
	}

	lower := strings.ToLower(strings.TrimSpace(*memory))

	// Check for valid suffixes and extract numeric part
	validSuffixes := []string{"pb", "tb", "gb", "mb", "kb", "p", "t", "g", "m", "k", "b"}
	numericPart := lower

	for _, suffix := range validSuffixes {
		if strings.HasSuffix(lower, suffix) {
			numericPart = strings.TrimSuffix(lower, suffix)
			break
		}
	}

	// Numeric part must not be empty and must be a valid number
	if numericPart == "" {
		return field.ErrorList{field.Invalid(path, *memory, "must have a numeric value")}
	}

	// Check that the numeric part is a non-negative integer (no decimals, no negative sign)
	for _, c := range numericPart {
		if c < '0' || c > '9' {
			return field.ErrorList{field.Invalid(path, *memory, "must be a non-negative integer with optional suffix (e.g., 1g, 512m, 1024k)")}
		}
	}

	return nil
}
