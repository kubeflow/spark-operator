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
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
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

var _ admission.Validator[*v1alpha1.SparkConnect] = &SparkConnectValidator{}

// ValidateCreate implements admission.Validator.
func (v *SparkConnectValidator) ValidateCreate(ctx context.Context, sc *v1alpha1.SparkConnect) (warnings admission.Warnings, err error) {
	if sc == nil {
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

// ValidateUpdate implements admission.Validator.
func (v *SparkConnectValidator) ValidateUpdate(ctx context.Context, oldSC *v1alpha1.SparkConnect, newSC *v1alpha1.SparkConnect) (warnings admission.Warnings, err error) {
	if oldSC == nil || newSC == nil {
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

	// Validate the CPU-related sparkConf keys before the CPU cross-validation below reads them to
	// compute the effective executor CPU resources.
	if err := validateSparkConfCPUKeys(sc.Spec.SparkConf); err != nil {
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

	// Validate GPU discovery configuration
	if err := v.validateGPUDiscovery(sc); err != nil {
		return err
	}

	if err := validateSparkConf(sc.Spec.SparkConf, sc.Namespace); err != nil {
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

	// Otherwise, require that the server and executor containers selected from the pod templates provide images.
	serverImageFound := podTemplateContainerImage(sc.Spec.Server.Template, common.SparkDriverContainerName) != ""
	executorImageFound := podTemplateContainerImage(sc.Spec.Executor.Template, common.Spark3DefaultExecutorContainerName) != ""

	if serverImageFound && executorImageFound {
		return nil
	}

	return fmt.Errorf("image must be specified in spec.image or in the selected server and executor template containers")
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
			return fmt.Errorf("invalid server.memory: %w", err)
		}
	}

	// Validate CoreRequest format if specified
	if server.CoreRequest != nil {
		if err := validateCPUQuantity(server.CoreRequest); err != nil {
			return fmt.Errorf("invalid server.coreRequest: %w", err)
		}
	}

	// Validate CoreLimit format if specified
	if server.CoreLimit != nil {
		if err := validateCPUQuantity(server.CoreLimit); err != nil {
			return fmt.Errorf("invalid server.coreLimit: %w", err)
		}
	}

	// Cross-validate that the effective coreRequest is less than or equal to the effective
	// coreLimit. This is enforced by Kubernetes itself for container resources, but rejecting
	// it here gives a clearer error at admission time.
	request, limit := effectiveServerCPUResources(sc)
	if request != nil && limit != nil {
		if err := validateCPURequestLELimit(request, limit); err != nil {
			return fmt.Errorf("invalid server CPU request/limit: %w", err)
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
			return fmt.Errorf("invalid executor.memory: %w", err)
		}
	}

	// Validate CoreRequest format if specified
	if executor.CoreRequest != nil {
		if err := validateCPUQuantity(executor.CoreRequest); err != nil {
			return fmt.Errorf("invalid executor.coreRequest: %w", err)
		}
	}

	// Validate CoreLimit format if specified
	if executor.CoreLimit != nil {
		if err := validateCPUQuantity(executor.CoreLimit); err != nil {
			return fmt.Errorf("invalid executor.coreLimit: %w", err)
		}
	}

	// Cross-validate that the effective coreRequest is less than or equal to the effective
	// coreLimit. Spark always sets an executor CPU request, so the effective request is never
	// unset and only the effective limit needs a nil check.
	request, limit := effectiveExecutorCPUResources(sc)
	if limit != nil {
		if err := validateCPURequestLELimit(request, limit); err != nil {
			return fmt.Errorf("invalid executor CPU request/limit: %w", err)
		}
	}

	return nil
}

// validateGPUDiscovery ensures that Spark is told how to discover GPUs whenever a GPU is requested.
// Spark refuses to start a driver or executor that requests a GPU resource without either a
// discovery script for that role or a discovery plugin.
func (v *SparkConnectValidator) validateGPUDiscovery(sc *v1alpha1.SparkConnect) error {
	if sc.Spec.SparkConf[common.SparkResourcesDiscoveryPlugin] != "" {
		return nil
	}

	roles := []struct {
		field  string
		gpu    *v1alpha1.GPUSpec
		script string
	}{
		{field: "server", gpu: sc.Spec.Server.GPU, script: common.SparkDriverGPUDiscoveryScript},
		{field: "executor", gpu: sc.Spec.Executor.GPU, script: common.SparkExecutorGPUDiscoveryScript},
	}
	for _, role := range roles {
		if role.gpu == nil || sc.Spec.SparkConf[role.script] != "" {
			continue
		}
		return fmt.Errorf("%s.gpu is set but GPU discovery is not configured: set sparkConf %q or %q",
			role.field, role.script, common.SparkResourcesDiscoveryPlugin)
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

// validateCPUQuantity validates a Kubernetes CPU quantity.
//
// It rejects a nil quantity as well as any quantity that is zero or negative. Callers must
// therefore only invoke it for fields that are actually set.
func validateCPUQuantity(cpu *resource.Quantity) error {
	if cpu == nil {
		return fmt.Errorf("CPU quantity cannot be nil")
	}

	if cpu.Sign() <= 0 {
		return fmt.Errorf("invalid CPU quantity: must be greater than zero")
	}

	return nil
}

// validateCPURequestLELimit validates that the effective CPU request is less than or equal to
// the effective CPU limit. The effective values may come from the CRD spec or fall back to the
// pod template container resources; both inputs must already be valid Kubernetes CPU quantities.
func validateCPURequestLELimit(request, limit *resource.Quantity) error {
	if request.Cmp(*limit) > 0 {
		return fmt.Errorf("effective coreRequest %q must not be greater than effective coreLimit %q", request.String(), limit.String())
	}
	return nil
}

// effectiveServerCPUResources returns the CPU request and limit that will actually be applied
// to the server container. A CRD field that is set always wins; when it is missing, the value
// set on the server pod template's container resources applies (spec.server.template).
func effectiveServerCPUResources(sc *v1alpha1.SparkConnect) (request, limit *resource.Quantity) {
	request = sc.Spec.Server.CoreRequest
	limit = sc.Spec.Server.CoreLimit

	if template := sc.Spec.Server.Template; template != nil {
		if container := util.GetContainerByNameOrFirst(
			template.Spec.Containers,
			common.SparkDriverContainerName,
		); container != nil {
			if request == nil {
				if v, ok := container.Resources.Requests[corev1.ResourceCPU]; ok {
					request = &v
				}
			}
			if limit == nil {
				if v, ok := container.Resources.Limits[corev1.ResourceCPU]; ok {
					limit = &v
				}
			}
		}
	}

	return request, limit
}

// effectiveExecutorCPUResources returns the CPU request and limit that Spark will actually apply
// to executor pods.
//
// Spark, not the operator, creates executor pods, so these values follow Spark's own resolution
// order rather than the CRD-over-template rule used for the server pod:
//
//	request: spec.executor.coreRequest, then sparkConf spark.kubernetes.executor.request.cores,
//	         then spark.executor.cores, then Spark's default of a single core.
//	limit:   spec.executor.coreLimit, then sparkConf spark.kubernetes.executor.limit.cores, then
//	         the executor pod template's container limit.
//
// The asymmetry is Spark's: the executor container's CPU request is always overwritten from
// spark.kubernetes.executor.request.cores or spark.executor.cores, so a request set on the pod
// template is ignored, while the limit is only set when limit.cores is configured and otherwise
// keeps whatever the pod template specified. Ref: running-on-kubernetes.md, "Container spec".
//
// Where a CRD field and its equivalent sparkConf key are both set, the CRD field wins: the operator
// appends its generated configuration after the user-supplied sparkConf, and spark-submit resolves
// duplicate --conf entries last-wins.
func effectiveExecutorCPUResources(sc *v1alpha1.SparkConnect) (request, limit *resource.Quantity) {
	request = sc.Spec.Executor.CoreRequest
	limit = sc.Spec.Executor.CoreLimit

	if request == nil {
		request = sparkConfCPUQuantity(sc.Spec.SparkConf, common.SparkKubernetesExecutorRequestCores)
	}
	if request == nil && sc.Spec.Executor.Cores != nil {
		request = resource.NewQuantity(int64(*sc.Spec.Executor.Cores), resource.DecimalSI)
	}
	if request == nil {
		request = sparkConfCPUQuantity(sc.Spec.SparkConf, common.SparkExecutorCores)
	}
	if request == nil {
		// Spark always sets an executor CPU request, defaulting to one core.
		request = resource.NewQuantity(1, resource.DecimalSI)
	}

	if limit == nil {
		limit = sparkConfCPUQuantity(sc.Spec.SparkConf, common.SparkKubernetesExecutorLimitCores)
	}
	if limit == nil {
		limit = executorTemplateCPULimit(sc)
	}

	return request, limit
}

// executorTemplateCPULimit returns the CPU limit set on the executor pod template's container, or
// nil when it is not set. Only the limit is read from the template: Spark overwrites the executor
// CPU request unconditionally.
func executorTemplateCPULimit(sc *v1alpha1.SparkConnect) *resource.Quantity {
	template := sc.Spec.Executor.Template
	if template == nil {
		return nil
	}

	container := util.GetContainerByNameOrFirst(
		template.Spec.Containers,
		common.Spark3DefaultExecutorContainerName,
	)
	if container == nil {
		return nil
	}

	if v, ok := container.Resources.Limits[corev1.ResourceCPU]; ok {
		return &v
	}

	return nil
}

// sparkConfCPUKeys are the sparkConf keys that carry a CPU quantity and therefore take part in
// resolving the effective executor CPU resources. Driver CPU configuration is deliberately absent:
// the operator creates the Spark Connect server pod itself and never emits
// spark.kubernetes.driver.{request,limit}.cores, so those keys have no effect on a SparkConnect.
var sparkConfCPUKeys = []string{
	common.SparkKubernetesExecutorRequestCores,
	common.SparkKubernetesExecutorLimitCores,
	common.SparkExecutorCores,
}

// validateSparkConfCPUKeys validates the CPU quantities set directly in spec.sparkConf. These keys
// are read to compute the effective executor CPU resources, so a value that cannot be parsed as a
// Kubernetes quantity -- or that is not positive -- must be rejected at admission time instead of
// being silently ignored or turned into an invalid executor pod.
//
// Setting both a CRD field and its equivalent sparkConf key is permitted rather than rejected as
// ambiguous: the operator emits its generated configuration after the user-supplied sparkConf, so
// the CRD field wins at runtime. The sparkConf value is validated all the same -- it is inert only
// while the CRD field is set, and would otherwise silently become the effective value if that field
// were later removed.
func validateSparkConfCPUKeys(sparkConf map[string]string) error {
	for _, key := range sparkConfCPUKeys {
		value, ok := sparkConf[key]
		if !ok {
			continue
		}

		quantity, err := resource.ParseQuantity(value)
		if err != nil {
			return fmt.Errorf("invalid sparkConf %s %q: %w", key, value, err)
		}
		if err := validateCPUQuantity(&quantity); err != nil {
			return fmt.Errorf("invalid sparkConf %s %q: %w", key, value, err)
		}
	}

	return nil
}

// sparkConfCPUQuantity parses a CPU quantity from spec.sparkConf, returning nil when the key is
// absent or its value cannot be parsed. Unparseable values are reported separately by
// validateSparkConfCPUKeys, which runs before the effective values are computed.
func sparkConfCPUQuantity(sparkConf map[string]string, key string) *resource.Quantity {
	value, ok := sparkConf[key]
	if !ok {
		return nil
	}

	quantity, err := resource.ParseQuantity(value)
	if err != nil {
		return nil
	}

	return &quantity
}
