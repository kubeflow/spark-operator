/*
Copyright 2025 The Kubeflow authors.

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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

func TestSparkApplicationValidatorValidateCreate_NodeSelectorConflict(t *testing.T) {
	validator := newTestValidator(t, false)

	app := newSparkApplication()
	app.Spec.NodeSelector = map[string]string{"role": "shared"}
	app.Spec.Driver.NodeSelector = map[string]string{"role": "driver"}

	if _, err := validator.ValidateCreate(context.Background(), app); err == nil || !strings.Contains(err.Error(), "node selector cannot be defined") {
		t.Fatalf("expected node selector validation error, got %v", err)
	}
}

func TestSparkApplicationValidatorValidateCreate_Success(t *testing.T) {
	validator := newTestValidator(t, false)

	if _, err := validator.ValidateCreate(context.Background(), newSparkApplication()); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
}

func TestSparkApplicationValidatorValidateCreate_DriverIngressDuplicatePort(t *testing.T) {
	validator := newTestValidator(t, false)

	app := newSparkApplication()
	app.Spec.DriverIngressOptions = []v1beta2.DriverIngressConfiguration{
		{
			ServicePort:      ptr.To[int32](4040),
			IngressURLFormat: "http://spark-a",
		},
		{
			ServicePort:      ptr.To[int32](4040),
			IngressURLFormat: "http://spark-b",
		},
	}

	if _, err := validator.ValidateCreate(context.Background(), app); err == nil || !strings.Contains(err.Error(), "duplicate ServicePort") {
		t.Fatalf("expected duplicate service port error, got %v", err)
	}
}

func TestSparkApplicationValidatorValidateCreate_PodTemplateRequiresSpark3(t *testing.T) {
	validator := newTestValidator(t, false)

	app := newSparkApplication()
	app.Spec.SparkVersion = "2.4.0"
	app.Spec.Driver.Template = &corev1.PodTemplateSpec{}

	if _, err := validator.ValidateCreate(context.Background(), app); err == nil || !strings.Contains(err.Error(), "requires Spark version 3.0.0 or higher") {
		t.Fatalf("expected spark version validation error, got %v", err)
	}
}

func TestSparkApplicationValidatorValidateCreate_ResourceQuotaSatisfied(t *testing.T) {
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ample",
			Namespace: "default",
		},
		Spec: corev1.ResourceQuotaSpec{
			Hard: corev1.ResourceList{
				corev1.ResourceCPU:         resource.MustParse("20"),
				corev1.ResourceRequestsCPU: resource.MustParse("20"),
				corev1.ResourceLimitsCPU:   resource.MustParse("20"),
			},
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceCPU:         resource.MustParse("20"),
				corev1.ResourceRequestsCPU: resource.MustParse("20"),
				corev1.ResourceLimitsCPU:   resource.MustParse("20"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceCPU:         resource.MustParse("0"),
				corev1.ResourceRequestsCPU: resource.MustParse("0"),
				corev1.ResourceLimitsCPU:   resource.MustParse("0"),
			},
		},
	}

	validator := newTestValidator(t, true, quota)

	if _, err := validator.ValidateCreate(context.Background(), newSparkApplication()); err != nil {
		t.Fatalf("expected quota satisfied, got %v", err)
	}
}

func TestSparkApplicationValidatorValidateUpdate_SameSpecSkipsValidation(t *testing.T) {
	validator := newTestValidator(t, true)

	base := newSparkApplication()
	base.Spec.NodeSelector = map[string]string{"role": "shared"}
	base.Spec.Driver.NodeSelector = map[string]string{"role": "driver"}

	oldApp := base.DeepCopy()
	newApp := base.DeepCopy()

	if _, err := validator.ValidateUpdate(context.Background(), oldApp, newApp); err != nil {
		t.Fatalf("expected no error when spec unchanged, got %v", err)
	}
}

func TestSparkApplicationValidatorValidateUpdate_SpecChangedTriggersValidation(t *testing.T) {
	validator := newTestValidator(t, false)

	oldApp := newSparkApplication()
	newApp := oldApp.DeepCopy()
	newApp.Spec.NodeSelector = map[string]string{"role": "shared"}
	newApp.Spec.Driver.NodeSelector = map[string]string{"role": "driver"}

	if _, err := validator.ValidateUpdate(context.Background(), oldApp, newApp); err == nil || !strings.Contains(err.Error(), "node selector cannot be defined") {
		t.Fatalf("expected node selector validation error, got %v", err)
	}
}

func TestSparkApplicationValidatorValidateUpdate_SuccessWithSpecChange(t *testing.T) {
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ample",
			Namespace: "default",
		},
		Spec: corev1.ResourceQuotaSpec{
			Hard: corev1.ResourceList{
				corev1.ResourceCPU:         resource.MustParse("20"),
				corev1.ResourceRequestsCPU: resource.MustParse("20"),
				corev1.ResourceLimitsCPU:   resource.MustParse("20"),
			},
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceCPU:         resource.MustParse("20"),
				corev1.ResourceRequestsCPU: resource.MustParse("20"),
				corev1.ResourceLimitsCPU:   resource.MustParse("20"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceCPU:         resource.MustParse("1"),
				corev1.ResourceRequestsCPU: resource.MustParse("1"),
				corev1.ResourceLimitsCPU:   resource.MustParse("1"),
			},
		},
	}

	validator := newTestValidator(t, true, quota)

	oldApp := newSparkApplication()
	newApp := oldApp.DeepCopy()
	newApp.Spec.Arguments = []string{"--foo"}

	if _, err := validator.ValidateUpdate(context.Background(), oldApp, newApp); err != nil {
		t.Fatalf("expected successful update validation, got %v", err)
	}
}

func TestSparkApplicationValidatorValidateCreate_ResourceQuotaExceeded(t *testing.T) {
	quota := &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "strict",
			Namespace: "default",
		},
		Spec: corev1.ResourceQuotaSpec{
			Hard: corev1.ResourceList{
				corev1.ResourceLimitsCPU: resource.MustParse("1"),
			},
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{
				corev1.ResourceLimitsCPU: resource.MustParse("1"),
			},
			Used: corev1.ResourceList{
				corev1.ResourceLimitsCPU: resource.MustParse("0"),
			},
		},
	}

	validator := newTestValidator(t, true, quota)

	if _, err := validator.ValidateCreate(context.Background(), newSparkApplication()); err == nil || !strings.Contains(err.Error(), "failed to validate resource quota") {
		t.Fatalf("expected resource quota validation error, got %v", err)
	}
}

func TestSparkApplicationValidatorValidateDelete_Success(t *testing.T) {
	validator := newTestValidator(t, false)

	if _, err := validator.ValidateDelete(context.Background(), newSparkApplication()); err != nil {
		t.Fatalf("expected successful delete validation, got %v", err)
	}
}

func newTestValidator(t *testing.T, enforceQuota bool, objs ...client.Object) *SparkApplicationValidator {
	t.Helper()

	scheme := newTestScheme(t)

	builder := fake.NewClientBuilder().WithScheme(scheme)
	if len(objs) > 0 {
		builder = builder.WithObjects(objs...)
	}

	return NewSparkApplicationValidator(builder.Build(), enforceQuota)
}

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add corev1 to scheme: %v", err)
	}
	if err := v1beta2.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add v1beta2 to scheme: %v", err)
	}
	return scheme
}

func TestSparkApplicationValidatorValidateName(t *testing.T) {
	validator := newTestValidator(t, false)

	tests := []struct {
		name      string
		appName   string
		wantError bool
	}{
		// Valid names
		{"valid simple name", "test-app", false},
		{"valid name with numbers", "test-app-123", false},
		{"valid single letter", "a", false},
		{"valid name ending with number", "my-app-1", false},
		{"valid name with multiple hyphens", "my-test-app-123", false},
		{"valid 63 char name", strings.Repeat("a", 63), false},
		{"valid name with hyphens in middle", "a-b-c-d-e", false},

		// Invalid names
		{"name starting with number", "123test-app", true},
		{"name with uppercase", "Test-App", true},
		{"name with uppercase at start", "TestApp", true},
		{"name with uppercase in middle", "test-App", true},
		{"name starting with hyphen", "-test-app", true},
		{"name ending with hyphen", "test-app-", true},
		{"name with consecutive hyphens", "test--app", false}, // Kubernetes validation allows consecutive hyphens
		{"empty name", "", true},
		{"name too long", strings.Repeat("a", 64), true},
		{"name with special characters", "test@app", true},
		{"name with underscore", "test_app", true},
		{"name with spaces", "test app", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := newSparkApplication()
			app.Name = tt.appName

			_, err := validator.ValidateCreate(context.Background(), app)
			hasError := err != nil

			if hasError != tt.wantError {
				t.Errorf("validateName(%q) = error %v, wantError %v, got error: %v", tt.appName, hasError, tt.wantError, err)
			}

			if hasError && err.Error() == "" {
				t.Errorf("validateName(%q) should return a non-empty error message, got: %v", tt.appName, err)
			}
		})
	}
}

func newSparkApplication() *v1beta2.SparkApplication {
	mainFile := "local:///app.py"
	return &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "default",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Type:                v1beta2.SparkApplicationTypeScala,
			SparkVersion:        "3.5.0",
			Mode:                v1beta2.DeployModeCluster,
			MainApplicationFile: &mainFile,
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores:  ptr.To[int32](1),
					Memory: ptr.To("1g"),
				},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores:  ptr.To[int32](1),
					Memory: ptr.To("1g"),
				},
				Instances: ptr.To[int32](1),
			},
		},
	}
}
