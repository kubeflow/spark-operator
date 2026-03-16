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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
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
