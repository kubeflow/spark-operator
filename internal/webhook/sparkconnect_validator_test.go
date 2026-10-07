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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

func TestSparkConnectValidatorValidateCreate_Success(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	if _, err := validator.ValidateCreate(context.Background(), newSparkConnect()); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
}

func TestSparkConnectValidatorValidateCreate_SparkVersionRequired(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	sc.Spec.SparkVersion = ""

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "sparkVersion is required") {
		t.Fatalf("expected sparkVersion required error, got %v", err)
	}
}

func TestSparkConnectValidatorValidateCreate_PodTemplateRequiresSpark3(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	sc.Spec.SparkVersion = "2.4.0"
	sc.Spec.Server.Template = &corev1.PodTemplateSpec{}

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "requires Spark version 3.0.0 or higher") {
		t.Fatalf("expected spark version validation error, got %v", err)
	}
}

func TestSparkConnectValidatorValidateCreate_ImageRequired(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	sc.Spec.Image = nil

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "image must be specified") {
		t.Fatalf("expected image validation error, got %v", err)
	}
}

func TestSparkConnectValidatorValidateCreate_ImageInBothTemplates(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	sc.Spec.Image = nil
	sc.Spec.Server.Template = &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "spark-connect",
					Image: "spark:3.5.0",
				},
			},
		},
	}
	sc.Spec.Executor.Template = &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "executor",
					Image: "spark:3.5.0",
				},
			},
		},
	}

	if _, err := validator.ValidateCreate(context.Background(), sc); err != nil {
		t.Fatalf("expected success with image in both server and executor templates, got %v", err)
	}
}

func TestSparkConnectValidatorValidateCreate_ImageOnlyInServerTemplate(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	sc.Spec.Image = nil
	sc.Spec.Server.Template = &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "spark-connect",
					Image: "spark:3.5.0",
				},
			},
		},
	}
	// No executor template - should fail

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "image must be specified") {
		t.Fatalf("expected image validation error when only server template has image, got %v", err)
	}
}

func TestSparkConnectValidatorValidateCreate_ImageOnlyInUnselectedContainer(t *testing.T) {
	tests := []struct {
		name     string
		server   []corev1.Container
		executor []corev1.Container
	}{
		{
			name: "server sidecar",
			server: []corev1.Container{
				{Name: "sidecar", Image: "busybox:1.36"},
				{Name: common.SparkDriverContainerName},
			},
			executor: []corev1.Container{{Name: "executor", Image: "spark:3.5.0"}},
		},
		{
			name:   "executor sidecar",
			server: []corev1.Container{{Name: "server", Image: "spark:3.5.0"}},
			executor: []corev1.Container{
				{Name: "sidecar", Image: "busybox:1.36"},
				{Name: common.Spark3DefaultExecutorContainerName},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			validator := newTestSparkConnectValidator(t)
			sc := newSparkConnect()
			sc.Spec.Image = nil
			sc.Spec.Server.Template = &corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: test.server}}
			sc.Spec.Executor.Template = &corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: test.executor}}

			if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "image must be specified") {
				t.Fatalf("expected image validation error, got %v", err)
			}
		})
	}
}

func TestSparkConnectValidatorValidateCreate_DynamicAllocationMinGreaterThanMax(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	sc.Spec.DynamicAllocation = &v1alpha1.DynamicAllocation{
		Enabled:      true,
		MinExecutors: ptr.To[int32](10),
		MaxExecutors: ptr.To[int32](5),
	}

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "cannot be greater than") {
		t.Fatalf("expected min/max executors validation error, got %v", err)
	}
}

func TestSparkConnectValidatorValidateCreate_DynamicAllocationInitialLessThanMin(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	sc.Spec.DynamicAllocation = &v1alpha1.DynamicAllocation{
		Enabled:          true,
		InitialExecutors: ptr.To[int32](1),
		MinExecutors:     ptr.To[int32](5),
		MaxExecutors:     ptr.To[int32](10),
	}

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "cannot be less than") {
		t.Fatalf("expected initialExecutors validation error, got %v", err)
	}
}

func TestSparkConnectValidatorValidateCreate_DynamicAllocationInitialGreaterThanMax(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	sc.Spec.DynamicAllocation = &v1alpha1.DynamicAllocation{
		Enabled:          true,
		InitialExecutors: ptr.To[int32](15),
		MinExecutors:     ptr.To[int32](5),
		MaxExecutors:     ptr.To[int32](10),
	}

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "cannot be greater than") {
		t.Fatalf("expected initialExecutors validation error, got %v", err)
	}
}

func TestSparkConnectValidatorValidateCreate_DynamicAllocationValid(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	sc.Spec.DynamicAllocation = &v1alpha1.DynamicAllocation{
		Enabled:          true,
		InitialExecutors: ptr.To[int32](5),
		MinExecutors:     ptr.To[int32](2),
		MaxExecutors:     ptr.To[int32](10),
	}

	if _, err := validator.ValidateCreate(context.Background(), sc); err != nil {
		t.Fatalf("expected success for valid dynamic allocation, got %v", err)
	}
}

func TestSparkConnectValidatorValidateCreate_InvalidServerMemory(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	sc.Spec.Server.Memory = ptr.To("invalid-memory")

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "invalid server.memory") {
		t.Fatalf("expected server memory validation error, got %v", err)
	}
}

func TestSparkConnectValidatorValidateCreate_InvalidExecutorMemory(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	sc.Spec.Executor.Memory = ptr.To("bad-format")

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "invalid executor.memory") {
		t.Fatalf("expected executor memory validation error, got %v", err)
	}
}

func TestSparkConnectValidatorValidateCreate_ValidMemoryFormats(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	validMemoryFormats := []string{"1g", "512m", "1024k", "2048mb", "1gb", "100", "1t", "1024"}

	for _, mem := range validMemoryFormats {
		t.Run(mem, func(t *testing.T) {
			sc := newSparkConnect()
			sc.Spec.Server.Memory = ptr.To(mem)
			sc.Spec.Executor.Memory = ptr.To(mem)

			if _, err := validator.ValidateCreate(context.Background(), sc); err != nil {
				t.Fatalf("expected success for memory format %q, got %v", mem, err)
			}
		})
	}
}

func TestSparkConnectValidatorValidateCreate_GPUDiscovery(t *testing.T) {
	gpu := &v1alpha1.GPUSpec{Name: "nvidia.com/gpu", Quantity: 1}

	tests := []struct {
		name      string
		mutate    func(sc *v1alpha1.SparkConnect)
		wantError string
	}{
		{
			name:   "no GPU requires no discovery",
			mutate: func(sc *v1alpha1.SparkConnect) {},
		},
		{
			name:      "executor GPU without discovery",
			mutate:    func(sc *v1alpha1.SparkConnect) { sc.Spec.Executor.GPU = gpu },
			wantError: "executor.gpu is set but GPU discovery is not configured",
		},
		{
			name:      "server GPU without discovery",
			mutate:    func(sc *v1alpha1.SparkConnect) { sc.Spec.Server.GPU = gpu },
			wantError: "server.gpu is set but GPU discovery is not configured",
		},
		{
			name: "executor GPU with executor discovery script",
			mutate: func(sc *v1alpha1.SparkConnect) {
				sc.Spec.Executor.GPU = gpu
				sc.Spec.SparkConf = map[string]string{common.SparkExecutorGPUDiscoveryScript: "/opt/spark/gpus.sh"}
			},
		},
		{
			name: "server GPU with only executor discovery script",
			mutate: func(sc *v1alpha1.SparkConnect) {
				sc.Spec.Server.GPU = gpu
				sc.Spec.SparkConf = map[string]string{common.SparkExecutorGPUDiscoveryScript: "/opt/spark/gpus.sh"}
			},
			wantError: "server.gpu is set but GPU discovery is not configured",
		},
		{
			name: "both GPUs with discovery plugin",
			mutate: func(sc *v1alpha1.SparkConnect) {
				sc.Spec.Server.GPU = gpu
				sc.Spec.Executor.GPU = gpu
				sc.Spec.SparkConf = map[string]string{common.SparkResourcesDiscoveryPlugin: "com.example.GpuPlugin"}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			validator := newTestSparkConnectValidator(t)
			sc := newSparkConnect()
			tt.mutate(sc)

			_, err := validator.ValidateCreate(context.Background(), sc)
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("expected success, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("expected error containing %q, got %v", tt.wantError, err)
			}
		})
	}
}

func TestSparkConnectValidatorValidateUpdate_SameSpecSkipsValidation(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	// Set invalid sparkVersion which would fail validation
	sc.Spec.SparkVersion = ""

	oldSC := sc.DeepCopy()
	newSC := sc.DeepCopy()

	// Should skip validation because spec is unchanged
	// But name validation still happens, so we need a valid name
	oldSC.Spec.SparkVersion = "3.5.0"
	newSC.Spec.SparkVersion = "3.5.0"

	if _, err := validator.ValidateUpdate(context.Background(), oldSC, newSC); err != nil {
		t.Fatalf("expected no error when spec unchanged, got %v", err)
	}
}

func TestSparkConnectValidatorValidateUpdate_SpecChangedTriggersValidation(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	oldSC := newSparkConnect()
	newSC := oldSC.DeepCopy()
	newSC.Spec.SparkVersion = ""

	if _, err := validator.ValidateUpdate(context.Background(), oldSC, newSC); err == nil || !strings.Contains(err.Error(), "sparkVersion is required") {
		t.Fatalf("expected sparkVersion validation error, got %v", err)
	}
}

func TestSparkConnectValidatorValidateDelete_Success(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	if _, err := validator.ValidateDelete(context.Background(), newSparkConnect()); err != nil {
		t.Fatalf("expected successful delete validation, got %v", err)
	}
}

func TestSparkConnectValidatorValidateName(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	// The operator derives a default Service name as "<name>-server" (7 chars suffix).
	// So the effective max name length is 63 - 7 = 56 characters.
	tests := []struct {
		name      string
		scName    string
		wantError bool
	}{
		// Valid names
		{"valid simple name", "test-sc", false},
		{"valid name with numbers", "test-sc-123", false},
		{"valid single letter", "a", false},
		{"valid name ending with number", "my-sc-1", false},
		{"valid name with multiple hyphens", "my-test-sc-123", false},
		{"valid 56 char name (max for derived service name)", strings.Repeat("a", 56), false},
		{"valid name with hyphens in middle", "a-b-c-d-e", false},

		// Invalid names
		{"name starting with number", "123test-sc", true},
		{"name with uppercase", "Test-SC", true},
		{"name with uppercase at start", "TestSC", true},
		{"name with uppercase in middle", "test-SC", true},
		{"name starting with hyphen", "-test-sc", true},
		{"name ending with hyphen", "test-sc-", true},
		{"empty name", "", true},
		{"name 57 chars exceeds derived service name limit", strings.Repeat("a", 57), true},
		{"name 63 chars exceeds derived service name limit", strings.Repeat("a", 63), true},
		{"name too long for DNS-1035", strings.Repeat("a", 64), true},
		{"name with special characters", "test@sc", true},
		{"name with underscore", "test_sc", true},
		{"name with spaces", "test sc", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := newSparkConnect()
			sc.Name = tt.scName

			_, err := validator.ValidateCreate(context.Background(), sc)
			hasError := err != nil

			if hasError != tt.wantError {
				t.Errorf("validateName(%q) = error %v, wantError %v, got error: %v", tt.scName, hasError, tt.wantError, err)
			}

			if hasError && err.Error() == "" {
				t.Errorf("validateName(%q) should return a non-empty error message, got: %v", tt.scName, err)
			}
		})
	}
}

func TestValidateMemoryString(t *testing.T) {
	tests := []struct {
		name      string
		memory    string
		wantError bool
	}{
		// Valid formats
		{"bytes", "1073741824", false},
		{"kilobytes lowercase", "1024k", false},
		{"kilobytes uppercase", "1024K", false},
		{"kilobytes with kb", "1024kb", false},
		{"megabytes lowercase", "512m", false},
		{"megabytes with mb", "512mb", false},
		{"gigabytes lowercase", "1g", false},
		{"gigabytes with gb", "1gb", false},
		{"terabytes lowercase", "1t", false},
		{"terabytes with tb", "1tb", false},
		{"petabytes lowercase", "1p", false},
		{"petabytes with pb", "1pb", false},
		{"empty string", "", false},

		// Invalid formats
		{"invalid suffix", "1x", true},
		{"text only", "invalid", true},
		{"mixed invalid", "1g2m", true},
		{"negative value", "-1g", true},
		{"negative bytes", "-1024", true},
		{"decimal value", "1.5g", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateMemoryString(tt.memory)
			hasError := err != nil

			if hasError != tt.wantError {
				t.Errorf("validateMemoryString(%q) = error %v, wantError %v, got error: %v", tt.memory, hasError, tt.wantError, err)
			}
		})
	}
}

func TestSparkConnectValidatorSparkConf_SecurityVectorsRejected(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	for _, tt := range sparkConfSecurityVectors {
		t.Run(tt.name, func(t *testing.T) {
			sc := newSparkConnect()
			sc.Spec.SparkConf = tt.sparkConf

			if _, err := validator.ValidateCreate(context.Background(), sc); err == nil {
				t.Fatalf("expected sparkConf to be rejected, but it was allowed")
			}
		})
	}
}

func TestSparkConnectValidatorSparkConf_UpdateRejected(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	oldSC := newSparkConnect()
	newSC := newSparkConnect()
	newSC.Spec.SparkConf = map[string]string{common.SparkMaster: "k8s://https://attacker-cluster:443"}

	if _, err := validator.ValidateUpdate(context.Background(), oldSC, newSC); err == nil {
		t.Fatalf("expected sparkConf to be rejected on update, but it was allowed")
	}
}

func TestSparkConnectValidatorValidateCreate_InvalidServerCoreRequest(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	zero := resource.MustParse("0")
	sc.Spec.Server.CoreRequest = &zero

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "invalid server.coreRequest") {
		t.Fatalf("expected invalid coreRequest validation error, got %v", err)
	}
}

func TestSparkConnectValidatorValidateCreate_InvalidServerCoreLimit(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	zero := resource.MustParse("0")
	sc.Spec.Server.CoreLimit = &zero

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "invalid server.coreLimit") {
		t.Fatalf("expected invalid coreLimit validation error, got %v", err)
	}
}

func TestSparkConnectValidatorValidateCreate_InvalidExecutorCoreRequest(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	zero := resource.MustParse("0")
	sc.Spec.Executor.CoreRequest = &zero

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "invalid executor.coreRequest") {
		t.Fatalf("expected invalid coreRequest validation error, got %v", err)
	}
}

func TestSparkConnectValidatorValidateCreate_InvalidExecutorCoreLimit(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	zero := resource.MustParse("0")
	sc.Spec.Executor.CoreLimit = &zero

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "invalid executor.coreLimit") {
		t.Fatalf("expected invalid coreLimit validation error, got %v", err)
	}
}

func TestSparkConnectValidatorValidateCreate_ValidCPUQuantities(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	testCases := []string{
		"500m",
		"1",
		"1.5",
		"2",
		"3500m",
		"4000m",
		"0.5",
	}

	for _, cpu := range testCases {
		t.Run(cpu, func(t *testing.T) {
			sc := newSparkConnect()
			q := resource.MustParse(cpu)
			sc.Spec.Server.CoreRequest = &q
			sc.Spec.Server.CoreLimit = &q
			sc.Spec.Executor.CoreRequest = &q
			sc.Spec.Executor.CoreLimit = &q

			if _, err := validator.ValidateCreate(context.Background(), sc); err != nil {
				t.Fatalf("expected success for valid CPU quantity %q, got %v", cpu, err)
			}
		})
	}
}

func TestValidateCPUQuantity(t *testing.T) {
	testCases := []struct {
		name    string
		cpu     *resource.Quantity
		wantErr bool
	}{
		// Valid cases following Kubernetes quantity semantics
		{"millicores", ptr.To(resource.MustParse("500m")), false},
		{"integer cores", ptr.To(resource.MustParse("1")), false},
		{"decimal cores", ptr.To(resource.MustParse("1.5")), false},
		{"decimal cores 2", ptr.To(resource.MustParse("2.5")), false},
		{"large millicores", ptr.To(resource.MustParse("4000m")), false},
		{"large decimal", ptr.To(resource.MustParse("8.5")), false},

		// Invalid cases - zero, negative, or nil values are not acceptable CPU
		// resource quantities for request/limit fields.
		{"nil pointer", nil, true},
		{"zero integer", ptr.To(resource.MustParse("0")), true},
		{"zero with millis", ptr.To(resource.MustParse("0m")), true},
		{"negative millicores", ptr.To(resource.MustParse("-500m")), true},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCPUQuantity(tt.cpu)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateCPUQuantity(%v) wantErr=%v, got err=%v", tt.cpu, tt.wantErr, err)
			}
		})
	}
}

func TestValidateCPURequestLELimit(t *testing.T) {
	testCases := []struct {
		name    string
		request *resource.Quantity
		limit   *resource.Quantity
		wantErr bool
	}{
		{"equal integers", ptr.To(resource.MustParse("1")), ptr.To(resource.MustParse("1")), false},
		{"equal millis", ptr.To(resource.MustParse("500m")), ptr.To(resource.MustParse("500m")), false},
		{"request less than limit", ptr.To(resource.MustParse("500m")), ptr.To(resource.MustParse("1")), false},
		{"request less than limit decimal", ptr.To(resource.MustParse("1.5")), ptr.To(resource.MustParse("2.5")), false},
		{"request greater than limit", ptr.To(resource.MustParse("2")), ptr.To(resource.MustParse("1")), true},
		{"request greater than limit decimal", ptr.To(resource.MustParse("2.5")), ptr.To(resource.MustParse("1.5")), true},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCPURequestLELimit(tt.request, tt.limit)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateCPURequestLELimit(%v, %v) wantErr=%v, got err=%v", tt.request, tt.limit, tt.wantErr, err)
			}
		})
	}
}

func TestSparkConnectValidatorValidateCreate_ServerCoreRequestExceedsLimit(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	sc.Spec.Server.CoreRequest = ptr.To(resource.MustParse("2"))
	sc.Spec.Server.CoreLimit = ptr.To(resource.MustParse("1"))

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "coreRequest") {
		t.Fatalf("expected server coreRequest/coreLimit validation error, got %v", err)
	}
}

// The request/limit cross-validation must use the effective values: a CRD field wins, and a
// missing CRD field falls back to the pod template container resources. A request that only
// exceeds the template's limit (not a CRD limit) must still be rejected.
func TestSparkConnectValidatorValidateCreate_ServerCoreRequestExceedsTemplateLimit(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	sc.Spec.Server.CoreRequest = ptr.To(resource.MustParse("2"))
	sc.Spec.Server.Template = &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  common.SparkDriverContainerName,
					Image: "spark:3.5.0",
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "coreRequest") {
		t.Fatalf("expected server coreRequest vs template coreLimit validation error, got %v", err)
	}
}

func TestSparkConnectValidatorValidateCreate_ExecutorCoreRequestExceedsTemplateLimit(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	sc.Spec.Executor.CoreRequest = ptr.To(resource.MustParse("2"))
	sc.Spec.Executor.Template = &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  common.Spark3DefaultExecutorContainerName,
					Image: "spark:3.5.0",
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "coreRequest") {
		t.Fatalf("expected executor coreRequest vs template coreLimit validation error, got %v", err)
	}
}

// A CRD limit that is lower than the template's request must also be rejected: the CRD limit
// wins over the template, so the effective request comes from the template.
func TestSparkConnectValidatorValidateCreate_ServerCoreLimitBelowTemplateRequest(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	sc.Spec.Server.CoreLimit = ptr.To(resource.MustParse("1"))
	sc.Spec.Server.Template = &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  common.SparkDriverContainerName,
					Image: "spark:3.5.0",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
			},
		},
	}

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "coreRequest") {
		t.Fatalf("expected server template coreRequest vs CRD coreLimit validation error, got %v", err)
	}
}

// When both effective values come from the template, the cross-validation still applies.
func TestSparkConnectValidatorValidateCreate_TemplateRequestExceedsTemplateLimit(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	sc.Spec.Server.Template = &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  common.SparkDriverContainerName,
					Image: "spark:3.5.0",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("2"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "coreRequest") {
		t.Fatalf("expected template-only coreRequest/coreLimit validation error, got %v", err)
	}
}

// Missing effective values are skipped, and a valid combination of CRD and template values
// passes.
func TestSparkConnectValidatorValidateCreate_ValidEffectiveCPUCombination(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	sc.Spec.Server.CoreRequest = ptr.To(resource.MustParse("500m"))
	sc.Spec.Server.Template = &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  common.SparkDriverContainerName,
					Image: "spark:3.5.0",
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	if _, err := validator.ValidateCreate(context.Background(), sc); err != nil {
		t.Fatalf("expected success for CRD request below template limit, got %v", err)
	}
}

func TestSparkConnectValidatorValidateCreate_ExecutorCoreRequestExceedsLimit(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	sc.Spec.Executor.CoreRequest = ptr.To(resource.MustParse("2"))
	sc.Spec.Executor.CoreLimit = ptr.To(resource.MustParse("1"))

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "coreRequest") {
		t.Fatalf("expected executor coreRequest/coreLimit validation error, got %v", err)
	}
}

func TestSparkConnectValidatorValidateCreate_ServerZeroCoreRequest(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	zero := resource.MustParse("0")
	sc.Spec.Server.CoreRequest = &zero

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "greater than zero") {
		t.Fatalf("expected server.coreRequest zero-value validation error, got %v", err)
	}
}

func TestSparkConnectValidatorValidateCreate_ExecutorNegativeCoreLimit(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	neg := resource.NewMilliQuantity(-500, resource.DecimalSI)
	sc.Spec.Executor.CoreLimit = neg

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "greater than zero") {
		t.Fatalf("expected negative coreLimit validation error, got %v", err)
	}
}

// effectiveExecutorCPUResources must model what Spark actually applies to the executor container,
// which is not the same rule as the operator-created server pod.
//
// Spark overwrites the executor container's CPU request unconditionally, so a request set on the
// executor pod template is never honoured. The CPU limit is only set when limit.cores is
// configured, so the template's limit survives when the conf is unset.
func TestEffectiveExecutorCPUResources_Request(t *testing.T) {
	testCases := []struct {
		name           string
		coreRequest    *resource.Quantity
		cores          *int32
		sparkConf      map[string]string
		templateCPUReq string
		wantRequest    string
	}{
		{
			name:        "CRD coreRequest wins over every fallback",
			coreRequest: ptr.To(resource.MustParse("500m")),
			cores:       ptr.To[int32](4),
			sparkConf: map[string]string{
				common.SparkKubernetesExecutorRequestCores: "2",
				common.SparkExecutorCores:                  "3",
			},
			templateCPUReq: "3",
			wantRequest:    "500m",
		},
		{
			name:  "sparkConf request.cores wins over cores and spark.executor.cores",
			cores: ptr.To[int32](4),
			sparkConf: map[string]string{
				common.SparkKubernetesExecutorRequestCores: "2",
				common.SparkExecutorCores:                  "3",
			},
			templateCPUReq: "3",
			wantRequest:    "2",
		},
		{
			name:  "CRD executor.cores wins over sparkConf spark.executor.cores",
			cores: ptr.To[int32](4),
			sparkConf: map[string]string{
				common.SparkExecutorCores: "3",
			},
			templateCPUReq: "3",
			wantRequest:    "4",
		},
		{
			name: "sparkConf spark.executor.cores is used when the CRD field is unset",
			sparkConf: map[string]string{
				common.SparkExecutorCores: "3",
			},
			templateCPUReq: "3",
			wantRequest:    "3",
		},
		{
			name:           "template request is ignored and the request defaults to one core",
			templateCPUReq: "3",
			wantRequest:    "1",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			sc := newSparkConnect()
			sc.Spec.Executor.CoreRequest = tt.coreRequest
			sc.Spec.Executor.Cores = tt.cores
			sc.Spec.SparkConf = tt.sparkConf
			if tt.templateCPUReq != "" {
				sc.Spec.Executor.Template = executorTemplateWithCPU(tt.templateCPUReq, "")
			}

			request, _ := effectiveExecutorCPUResources(sc)
			if request == nil {
				t.Fatalf("expected an effective request, got nil")
			}
			if want := resource.MustParse(tt.wantRequest); !request.Equal(want) {
				t.Fatalf("expected effective request %s, got %s", tt.wantRequest, request.String())
			}
		})
	}
}

// Unlike the request, the executor template's CPU limit IS honoured when limit.cores is unset.
func TestEffectiveExecutorCPUResources_Limit(t *testing.T) {
	testCases := []struct {
		name           string
		coreLimit      *resource.Quantity
		sparkConf      map[string]string
		templateCPULim string
		wantLimit      string
	}{
		{
			name:      "CRD coreLimit wins over every fallback",
			coreLimit: ptr.To(resource.MustParse("1500m")),
			sparkConf: map[string]string{
				common.SparkKubernetesExecutorLimitCores: "2",
			},
			templateCPULim: "3",
			wantLimit:      "1500m",
		},
		{
			name: "sparkConf limit.cores wins over the template limit",
			sparkConf: map[string]string{
				common.SparkKubernetesExecutorLimitCores: "2",
			},
			templateCPULim: "3",
			wantLimit:      "2",
		},
		{
			name:           "template limit is honoured when limit.cores is unset",
			templateCPULim: "3",
			wantLimit:      "3",
		},
		{
			name:      "no effective limit when nothing sets one",
			wantLimit: "",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			sc := newSparkConnect()
			sc.Spec.Executor.CoreLimit = tt.coreLimit
			sc.Spec.SparkConf = tt.sparkConf
			if tt.templateCPULim != "" {
				sc.Spec.Executor.Template = executorTemplateWithCPU("", tt.templateCPULim)
			}

			_, limit := effectiveExecutorCPUResources(sc)
			if tt.wantLimit == "" {
				if limit != nil {
					t.Fatalf("expected no effective limit, got %s", limit.String())
				}
				return
			}
			if limit == nil {
				t.Fatalf("expected an effective limit of %s, got nil", tt.wantLimit)
			}
			if want := resource.MustParse(tt.wantLimit); !limit.Equal(want) {
				t.Fatalf("expected effective limit %s, got %s", tt.wantLimit, limit.String())
			}
		})
	}
}

// A sparkConf limit lower than the CRD coreRequest must not pass validation. This is the case the
// reviewer called out: the executor CPU limit is derivable from sparkConf, so it has to take part
// in the cross-validation.
func TestSparkConnectValidatorValidateCreate_SparkConfLimitCoresBelowCoreRequest(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	sc.Spec.Executor.CoreRequest = ptr.To(resource.MustParse("2"))
	sc.Spec.SparkConf = map[string]string{
		common.SparkKubernetesExecutorLimitCores: "1",
	}

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "coreRequest") {
		t.Fatalf("expected executor CPU request/limit validation error, got %v", err)
	}
}

// The mirror case: a sparkConf request higher than the CRD coreLimit.
func TestSparkConnectValidatorValidateCreate_SparkConfRequestCoresAboveCoreLimit(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	sc.Spec.Executor.CoreLimit = ptr.To(resource.MustParse("1"))
	sc.Spec.SparkConf = map[string]string{
		common.SparkKubernetesExecutorRequestCores: "2",
	}

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "coreRequest") {
		t.Fatalf("expected executor CPU request/limit validation error, got %v", err)
	}
}

// Spark always sets an executor CPU request, so a limit below the implied single core cannot be
// satisfied even though no request field is set on the spec.
func TestSparkConnectValidatorValidateCreate_ExecutorCoreLimitBelowDefaultRequest(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	sc.Spec.Executor.Cores = nil
	sc.Spec.Executor.CoreLimit = ptr.To(resource.MustParse("500m"))

	if _, err := validator.ValidateCreate(context.Background(), sc); err == nil || !strings.Contains(err.Error(), "coreRequest") {
		t.Fatalf("expected executor CPU request/limit validation error, got %v", err)
	}
}

// A CPU request on the executor pod template is overwritten by Spark, so it must not be treated as
// the effective request. Here the template asks for 2 cores while the CRD limit is 1: the effective
// request is executor.cores (1), so this is valid and must not be rejected.
func TestSparkConnectValidatorValidateCreate_ExecutorTemplateRequestIgnored(t *testing.T) {
	validator := newTestSparkConnectValidator(t)

	sc := newSparkConnect()
	sc.Spec.Executor.CoreLimit = ptr.To(resource.MustParse("1"))
	sc.Spec.Executor.Template = executorTemplateWithCPU("2", "")

	if _, err := validator.ValidateCreate(context.Background(), sc); err != nil {
		t.Fatalf("expected success: the template CPU request is overwritten by Spark, got %v", err)
	}
}

// A sparkConf CPU value that is unparseable, zero, or negative would make the effective CPU
// computation above meaningless, so it must be rejected with the offending key and value named.
func TestSparkConnectValidatorValidateCreate_InvalidSparkConfCPUKeys(t *testing.T) {
	testCases := []struct {
		name      string
		key       string
		value     string
		wantError bool
	}{
		{name: "valid millicores", key: common.SparkKubernetesExecutorRequestCores, value: "500m"},
		{name: "valid integer", key: common.SparkExecutorCores, value: "2"},
		{name: "unparseable", key: common.SparkKubernetesExecutorLimitCores, value: "nonsense", wantError: true},
		{name: "negative", key: common.SparkKubernetesExecutorLimitCores, value: "-1", wantError: true},
		{name: "zero", key: common.SparkKubernetesExecutorRequestCores, value: "0", wantError: true},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			validator := newTestSparkConnectValidator(t)

			sc := newSparkConnect()
			sc.Spec.SparkConf = map[string]string{tt.key: tt.value}

			_, err := validator.ValidateCreate(context.Background(), sc)
			if (err != nil) != tt.wantError {
				t.Fatalf("ValidateCreate() with sparkConf %s=%q wantErr=%v, got err=%v", tt.key, tt.value, tt.wantError, err)
			}
			if tt.wantError && !strings.Contains(err.Error(), tt.key) {
				t.Fatalf("expected the error to name %s, got %v", tt.key, err)
			}
		})
	}
}

// Setting both a CRD field and its equivalent sparkConf key is permitted -- the CRD field wins at
// runtime, so the duplication is not an error. The sparkConf value is still validated, because it
// is inert only while the CRD field is set: drop that field later and an unparseable value becomes
// the effective one, failing at executor-pod creation instead of at admission. This pins the
// decision recorded on validateSparkConfCPUKeys.
func TestSparkConnectValidatorValidateCreate_DuplicateSparkConfCPUKeyStillValidated(t *testing.T) {
	testCases := []struct {
		name      string
		mutate    func(sc *v1alpha1.SparkConnect)
		wantError bool
	}{
		{
			name: "garbage request.cores behind a CRD coreRequest is still rejected",
			mutate: func(sc *v1alpha1.SparkConnect) {
				sc.Spec.Executor.CoreRequest = ptr.To(resource.MustParse("2"))
				sc.Spec.SparkConf = map[string]string{
					common.SparkKubernetesExecutorRequestCores: "garbage",
				}
			},
			wantError: true,
		},
		{
			name: "garbage limit.cores behind a CRD coreLimit is still rejected",
			mutate: func(sc *v1alpha1.SparkConnect) {
				sc.Spec.Executor.CoreLimit = ptr.To(resource.MustParse("1"))
				sc.Spec.SparkConf = map[string]string{
					common.SparkKubernetesExecutorLimitCores: "garbage",
				}
			},
			wantError: true,
		},
		{
			name: "garbage spark.executor.cores behind a CRD executor.cores is still rejected",
			mutate: func(sc *v1alpha1.SparkConnect) {
				sc.Spec.Executor.Cores = ptr.To[int32](4)
				sc.Spec.SparkConf = map[string]string{
					common.SparkExecutorCores: "garbage",
				}
			},
			wantError: true,
		},
		{
			name: "a well-formed duplicate is accepted, the CRD field winning",
			mutate: func(sc *v1alpha1.SparkConnect) {
				sc.Spec.Executor.CoreRequest = ptr.To(resource.MustParse("2"))
				sc.Spec.SparkConf = map[string]string{
					common.SparkKubernetesExecutorRequestCores: "1",
				}
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			validator := newTestSparkConnectValidator(t)

			sc := newSparkConnect()
			tt.mutate(sc)

			_, err := validator.ValidateCreate(context.Background(), sc)
			if (err != nil) != tt.wantError {
				t.Fatalf("ValidateCreate() wantErr=%v, got err=%v", tt.wantError, err)
			}
		})
	}
}

// executorTemplateWithCPU builds an executor pod template whose container sets the given CPU
// request and/or limit. An empty string leaves the corresponding value unset.
func executorTemplateWithCPU(cpuRequest, cpuLimit string) *corev1.PodTemplateSpec {
	resources := corev1.ResourceRequirements{}
	if cpuRequest != "" {
		resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpuRequest)}
	}
	if cpuLimit != "" {
		resources.Limits = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpuLimit)}
	}

	return &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:      common.Spark3DefaultExecutorContainerName,
					Image:     "spark:3.5.0",
					Resources: resources,
				},
			},
		},
	}
}

func newTestSparkConnectValidator(t *testing.T) *SparkConnectValidator {
	t.Helper()
	return NewSparkConnectValidator()
}

func newSparkConnect() *v1alpha1.SparkConnect {
	return &v1alpha1.SparkConnect{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-sc",
			Namespace: "default",
		},
		Spec: v1alpha1.SparkConnectSpec{
			Image:        ptr.To("spark:3.5.0"),
			SparkVersion: "3.5.0",
			Server: v1alpha1.ServerSpec{
				SparkPodSpec: v1alpha1.SparkPodSpec{
					Cores:  ptr.To[int32](1),
					Memory: ptr.To("1g"),
				},
			},
			Executor: v1alpha1.ExecutorSpec{
				SparkPodSpec: v1alpha1.SparkPodSpec{
					Cores:  ptr.To[int32](1),
					Memory: ptr.To("1g"),
				},
				Instances: ptr.To[int32](1),
			},
		},
	}
}
