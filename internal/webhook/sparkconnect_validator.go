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

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:admissionReviewVersions=v1,failurePolicy=fail,groups=sparkoperator.k8s.io,matchPolicy=Exact,mutating=false,name=validate-sparkconnect.sparkoperator.k8s.io,path=/validate-sparkoperator-k8s-io-v1alpha1-sparkconnect,reinvocationPolicy=Never,resources=sparkconnects,sideEffects=None,verbs=create;update,versions=v1alpha1,webhookVersions=v1

// SparkConnectValidator validates SparkConnect resources.
type SparkConnectValidator struct{}

// NewSparkConnectValidator creates a new SparkConnectValidator instance.
func NewSparkConnectValidator() *SparkConnectValidator {
	return &SparkConnectValidator{}
}

var _ admission.CustomValidator = &SparkConnectValidator{}

// ValidateCreate implements admission.CustomValidator.
func (v *SparkConnectValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (warnings admission.Warnings, err error) {
	sc, ok := obj.(*v1alpha1.SparkConnect)
	if !ok {
		return nil, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("Validating SparkConnect create", "name", sc.Name, "namespace", sc.Namespace)

	// Validate metadata.name early to prevent downstream Service creation failures
	if err := v.validateName(sc.Name); err != nil {
		return nil, err
	}

	if err := v.validateSpec(sc); err != nil {
		return nil, err
	}

	return nil, nil
}

// ValidateUpdate implements admission.CustomValidator.
func (v *SparkConnectValidator) ValidateUpdate(ctx context.Context, oldObj runtime.Object, newObj runtime.Object) (warnings admission.Warnings, err error) {
	oldSC, ok := oldObj.(*v1alpha1.SparkConnect)
	if !ok {
		return nil, nil
	}

	newSC, ok := newObj.(*v1alpha1.SparkConnect)
	if !ok {
		return nil, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("Validating SparkConnect update", "name", newSC.Name, "namespace", newSC.Namespace)

	// Name is immutable in Kubernetes, but validate anyway for safety
	if err := v.validateName(newSC.Name); err != nil {
		return nil, err
	}

	// Skip validating when spec does not change.
	if equality.Semantic.DeepEqual(oldSC.Spec, newSC.Spec) {
		return nil, nil
	}

	if err := v.validateSpec(newSC); err != nil {
		return nil, err
	}

	return nil, nil
}

// ValidateDelete implements admission.CustomValidator.
func (v *SparkConnectValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (warnings admission.Warnings, err error) {
	sc, ok := obj.(*v1alpha1.SparkConnect)
	if !ok {
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
func (v *SparkConnectValidator) validateName(name string) error {
	if errs := validation.IsDNS1035Label(name); len(errs) > 0 {
		return fmt.Errorf("invalid SparkConnect name %q: %s", name, strings.Join(errs, ", "))
	}

	// Ensure the derived default Service name "<name>-server" also fits within the
	// DNS-1035 label length limit, so Service creation will not fail downstream.
	const serviceSuffix = "-server"
	maxBaseLen := validation.DNS1035LabelMaxLength - len(serviceSuffix)
	if len(name) > maxBaseLen {
		return fmt.Errorf("invalid SparkConnect name %q: must be at most %d characters so that the derived Service name %q does not exceed the DNS-1035 label length limit (%d characters)",
			name, maxBaseLen, name+serviceSuffix, validation.DNS1035LabelMaxLength)
	}

	return nil
}

// validateSpec validates the SparkConnect spec.
func (v *SparkConnectValidator) validateSpec(sc *v1alpha1.SparkConnect) error {
	// Validate SparkVersion
	if err := v.validateSparkVersion(sc); err != nil {
		return err
	}

	// Validate image availability
	if err := v.validateImage(sc); err != nil {
		return err
	}

	// Validate DynamicAllocation
	if err := v.validateDynamicAllocation(sc); err != nil {
		return err
	}

	// Validate Server spec
	if err := v.validateServerSpec(sc); err != nil {
		return err
	}

	// Validate Executor spec
	if err := v.validateExecutorSpec(sc); err != nil {
		return err
	}

	return nil
}

// validateSparkVersion validates the Spark version.
// Pod templates require Spark 3.0.0 or higher.
func (v *SparkConnectValidator) validateSparkVersion(sc *v1alpha1.SparkConnect) error {
	// SparkVersion is required
	if sc.Spec.SparkVersion == "" {
		return fmt.Errorf("sparkVersion is required")
	}

	// If pod templates are used, require Spark 3.0.0+
	if sc.Spec.Server.Template != nil || sc.Spec.Executor.Template != nil {
		if util.CompareSemanticVersion(sc.Spec.SparkVersion, "3.0.0") < 0 {
			return fmt.Errorf("pod template feature requires Spark version 3.0.0 or higher, got %s", sc.Spec.SparkVersion)
		}
	}

	return nil
}

// validateImage validates that container images are available either from the spec-level image
// or from both the server and executor pod templates. This prevents the controller from entering
// a retry loop when it tries to reconcile a SparkConnect without valid images.
func (v *SparkConnectValidator) validateImage(sc *v1alpha1.SparkConnect) error {
	// If a spec-level image is provided, it will be used for both server and executor.
	if sc.Spec.Image != nil && *sc.Spec.Image != "" {
		return nil
	}

	// Otherwise, require that both server and executor pod templates provide container images.
	serverImageFound := false
	if sc.Spec.Server.Template != nil {
		for _, container := range sc.Spec.Server.Template.Spec.Containers {
			if container.Image != "" {
				serverImageFound = true
				break
			}
		}
	}

	executorImageFound := false
	if sc.Spec.Executor.Template != nil {
		for _, container := range sc.Spec.Executor.Template.Spec.Containers {
			if container.Image != "" {
				executorImageFound = true
				break
			}
		}
	}

	if serverImageFound && executorImageFound {
		return nil
	}

	return fmt.Errorf("image must be specified in spec.image or both server and executor pod templates must provide container images")
}

// validateDynamicAllocation validates DynamicAllocation configuration.
func (v *SparkConnectValidator) validateDynamicAllocation(sc *v1alpha1.SparkConnect) error {
	da := sc.Spec.DynamicAllocation
	if da == nil || !da.Enabled {
		return nil
	}

	// Validate minExecutors <= maxExecutors
	if da.MinExecutors != nil && da.MaxExecutors != nil {
		if *da.MinExecutors > *da.MaxExecutors {
			return fmt.Errorf("dynamicAllocation.minExecutors (%d) cannot be greater than dynamicAllocation.maxExecutors (%d)",
				*da.MinExecutors, *da.MaxExecutors)
		}
	}

	// Validate initialExecutors is within range
	if da.InitialExecutors != nil {
		if da.MinExecutors != nil && *da.InitialExecutors < *da.MinExecutors {
			return fmt.Errorf("dynamicAllocation.initialExecutors (%d) cannot be less than dynamicAllocation.minExecutors (%d)",
				*da.InitialExecutors, *da.MinExecutors)
		}
		if da.MaxExecutors != nil && *da.InitialExecutors > *da.MaxExecutors {
			return fmt.Errorf("dynamicAllocation.initialExecutors (%d) cannot be greater than dynamicAllocation.maxExecutors (%d)",
				*da.InitialExecutors, *da.MaxExecutors)
		}
	}

	// Validate non-negative values
	if da.MinExecutors != nil && *da.MinExecutors < 0 {
		return fmt.Errorf("dynamicAllocation.minExecutors must be non-negative, got %d", *da.MinExecutors)
	}
	if da.MaxExecutors != nil && *da.MaxExecutors < 0 {
		return fmt.Errorf("dynamicAllocation.maxExecutors must be non-negative, got %d", *da.MaxExecutors)
	}
	if da.InitialExecutors != nil && *da.InitialExecutors < 0 {
		return fmt.Errorf("dynamicAllocation.initialExecutors must be non-negative, got %d", *da.InitialExecutors)
	}

	return nil
}

// validateServerSpec validates the Server specification.
func (v *SparkConnectValidator) validateServerSpec(sc *v1alpha1.SparkConnect) error {
	server := sc.Spec.Server

	// Validate memory format if specified
	if server.Memory != nil && *server.Memory != "" {
		if err := validateMemoryString(*server.Memory); err != nil {
			return fmt.Errorf("invalid server.memory: %v", err)
		}
	}

	return nil
}

// validateExecutorSpec validates the Executor specification.
func (v *SparkConnectValidator) validateExecutorSpec(sc *v1alpha1.SparkConnect) error {
	executor := sc.Spec.Executor

	// Validate memory format if specified
	if executor.Memory != nil && *executor.Memory != "" {
		if err := validateMemoryString(*executor.Memory); err != nil {
			return fmt.Errorf("invalid executor.memory: %v", err)
		}
	}

	return nil
}

// validateMemoryString validates a Java/Spark memory string format.
// Valid formats: 1g, 512m, 1024k, 1073741824 (bytes)
func validateMemoryString(memory string) error {
	if memory == "" {
		return nil
	}

	lower := strings.ToLower(strings.TrimSpace(memory))

	// Check for valid suffixes and extract numeric part
	validSuffixes := []string{"pb", "tb", "gb", "mb", "kb", "p", "t", "g", "m", "k", "b"}
	numericPart := lower
	hasValidSuffix := false

	for _, suffix := range validSuffixes {
		if strings.HasSuffix(lower, suffix) {
			numericPart = strings.TrimSuffix(lower, suffix)
			hasValidSuffix = true
			break
		}
	}

	// Numeric part must not be empty and must be a valid number
	if numericPart == "" {
		return fmt.Errorf("invalid memory format %q: must have a numeric value", memory)
	}

	// Check that the numeric part is a non-negative integer (no decimals, no negative sign)
	for _, c := range numericPart {
		if c < '0' || c > '9' {
			return fmt.Errorf("invalid memory format %q: must be a non-negative integer with optional suffix (e.g., 1g, 512m, 1024k)", memory)
		}
	}

	// If no valid suffix, should be a pure number (bytes)
	if !hasValidSuffix {
		for _, c := range lower {
			if c < '0' || c > '9' {
				return fmt.Errorf("invalid memory format %q: must be a number with optional suffix (e.g., 1g, 512m, 1024k)", memory)
			}
		}
	}

	return nil
}
