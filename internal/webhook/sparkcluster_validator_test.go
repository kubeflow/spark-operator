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

	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newSparkCluster() *v1alpha1.SparkCluster {
	return &v1alpha1.SparkCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
		Spec: v1alpha1.SparkClusterSpec{
			SparkVersion: "3.5.0",
			Image:        ptr.To("apache/spark:3.5.0"),
			Master:       v1alpha1.MasterSpec{},
			WorkerGroups: []v1alpha1.WorkerGroupSpec{
				{
					Name:     "default",
					Replicas: ptr.To[int32](2),
				},
			},
		},
	}
}

func TestSparkClusterValidatorValidateCreate_Success(t *testing.T) {
	validator := NewSparkClusterValidator()

	if _, err := validator.ValidateCreate(context.Background(), newSparkCluster()); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
}

func TestSparkClusterValidatorValidateCreate_InvalidName(t *testing.T) {
	validator := NewSparkClusterValidator()

	cluster := newSparkCluster()
	cluster.Name = "Invalid-Name"

	if _, err := validator.ValidateCreate(context.Background(), cluster); err == nil || !strings.Contains(err.Error(), "invalid SparkCluster name") {
		t.Fatalf("expected name validation error, got %v", err)
	}
}

func TestSparkClusterValidatorValidateCreate_NameTooLong(t *testing.T) {
	validator := NewSparkClusterValidator()

	cluster := newSparkCluster()
	cluster.Name = strings.Repeat("a", 57) // 57 + len("-master") = 64 > 63

	if _, err := validator.ValidateCreate(context.Background(), cluster); err == nil || !strings.Contains(err.Error(), "DNS-1035 label length limit") {
		t.Fatalf("expected name length error, got %v", err)
	}
}

func TestSparkClusterValidatorValidateCreate_MissingSparkVersion(t *testing.T) {
	validator := NewSparkClusterValidator()

	cluster := newSparkCluster()
	cluster.Spec.SparkVersion = ""

	if _, err := validator.ValidateCreate(context.Background(), cluster); err == nil || !strings.Contains(err.Error(), "sparkVersion is required") {
		t.Fatalf("expected sparkVersion error, got %v", err)
	}
}

func TestSparkClusterValidatorValidateCreate_MissingImage(t *testing.T) {
	validator := NewSparkClusterValidator()

	cluster := newSparkCluster()
	cluster.Spec.Image = nil

	if _, err := validator.ValidateCreate(context.Background(), cluster); err == nil || !strings.Contains(err.Error(), "image must be specified") {
		t.Fatalf("expected image validation error, got %v", err)
	}
}

func TestSparkClusterValidatorValidateCreate_DuplicateWorkerGroupName(t *testing.T) {
	validator := NewSparkClusterValidator()

	cluster := newSparkCluster()
	cluster.Spec.WorkerGroups = []v1alpha1.WorkerGroupSpec{
		{Name: "gpu", Replicas: ptr.To[int32](1)},
		{Name: "gpu", Replicas: ptr.To[int32](2)},
	}

	if _, err := validator.ValidateCreate(context.Background(), cluster); err == nil || !strings.Contains(err.Error(), "duplicate worker group name") {
		t.Fatalf("expected duplicate worker group name error, got %v", err)
	}
}

func TestSparkClusterValidatorValidateCreate_EmptyWorkerGroupName(t *testing.T) {
	validator := NewSparkClusterValidator()

	cluster := newSparkCluster()
	cluster.Spec.WorkerGroups = []v1alpha1.WorkerGroupSpec{
		{Name: "", Replicas: ptr.To[int32](1)},
	}

	if _, err := validator.ValidateCreate(context.Background(), cluster); err == nil || !strings.Contains(err.Error(), "worker group name must not be empty") {
		t.Fatalf("expected empty worker group name error, got %v", err)
	}
}

func TestSparkClusterValidatorValidateUpdate_SameSpecSkipsValidation(t *testing.T) {
	validator := NewSparkClusterValidator()

	cluster := newSparkCluster()
	oldCluster := cluster.DeepCopy()
	newCluster := cluster.DeepCopy()

	if _, err := validator.ValidateUpdate(context.Background(), oldCluster, newCluster); err != nil {
		t.Fatalf("expected no error when spec unchanged, got %v", err)
	}
}

func TestSparkClusterValidatorValidateUpdate_SpecChangedTriggersValidation(t *testing.T) {
	validator := NewSparkClusterValidator()

	oldCluster := newSparkCluster()
	newCluster := oldCluster.DeepCopy()
	newCluster.Spec.Image = nil // Remove image to trigger validation failure

	if _, err := validator.ValidateUpdate(context.Background(), oldCluster, newCluster); err == nil || !strings.Contains(err.Error(), "image must be specified") {
		t.Fatalf("expected image validation error on update, got %v", err)
	}
}

func TestSparkClusterValidatorValidateDelete_Success(t *testing.T) {
	validator := NewSparkClusterValidator()

	if _, err := validator.ValidateDelete(context.Background(), newSparkCluster()); err != nil {
		t.Fatalf("expected successful delete validation, got %v", err)
	}
}

func TestSparkClusterValidatorValidateCreate_NoWorkerGroups(t *testing.T) {
	validator := NewSparkClusterValidator()

	cluster := newSparkCluster()
	cluster.Spec.WorkerGroups = nil

	// No worker groups is valid - master-only cluster.
	if _, err := validator.ValidateCreate(context.Background(), cluster); err != nil {
		t.Fatalf("expected success for master-only cluster, got %v", err)
	}
}
