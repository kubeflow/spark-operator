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

package sparkapplication

import (
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/features"
)

func recoveryEnvTestApp() *v1beta2.SparkApplication {
	return &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{Name: "resumable-etl", Namespace: "default"},
		Spec: v1beta2.SparkApplicationSpec{
			RestartPolicy: v1beta2.RestartPolicy{
				Type:     v1beta2.RestartPolicyOnFailure,
				Recovery: &v1beta2.RecoveryPolicy{StoreProfile: "default"},
			},
		},
		Status: v1beta2.SparkApplicationStatus{
			RecoveryStatus: &v1beta2.RecoveryStatus{
				Epoch:             3,
				RestoredFromEpoch: ptr.To(int64(2)),
			},
		},
	}
}

func TestRecoveryEnvOption(t *testing.T) {
	t.Run("injects the fenced-restart contract", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.FencedRestart, true)
		args, err := recoveryEnvOption(recoveryEnvTestApp())
		if err != nil {
			t.Fatalf("recoveryEnvOption: %v", err)
		}
		want := []string{
			"--conf", "spark.kubernetes.driverEnv.SPARK_RECOVERY_AGENT_URL=http://localhost:7691",
			"--conf", "spark.kubernetes.driverEnv.SPARK_RECOVERY_ENABLED=true",
			"--conf", "spark.kubernetes.driverEnv.SPARK_RECOVERY_EPOCH=3",
			"--conf", "spark.kubernetes.driverEnv.SPARK_RECOVERY_RESTORE_EPOCH=2",
		}
		if !slices.Equal(args, want) {
			t.Errorf("unexpected args:\n got  %v\n want %v", args, want)
		}
	})

	t.Run("no restore epoch on first run", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.FencedRestart, true)
		app := recoveryEnvTestApp()
		app.Status.RecoveryStatus = &v1beta2.RecoveryStatus{Epoch: 0}
		args, err := recoveryEnvOption(app)
		if err != nil {
			t.Fatalf("recoveryEnvOption: %v", err)
		}
		for _, arg := range args {
			if arg == "--conf" {
				continue
			}
			if slices.Contains([]string{"SPARK_RECOVERY_RESTORE_EPOCH"}, arg) {
				t.Errorf("restore epoch must not be set on first run: %v", args)
			}
		}
	})

	t.Run("no-op when the gate is disabled", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.FencedRestart, false)
		args, err := recoveryEnvOption(recoveryEnvTestApp())
		if err != nil {
			t.Fatalf("recoveryEnvOption: %v", err)
		}
		if len(args) != 0 {
			t.Errorf("expected no args when gate disabled, got %v", args)
		}
	})

	t.Run("no-op without recovery policy", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.FencedRestart, true)
		app := recoveryEnvTestApp()
		app.Spec.RestartPolicy.Recovery = nil
		args, err := recoveryEnvOption(app)
		if err != nil {
			t.Fatalf("recoveryEnvOption: %v", err)
		}
		if len(args) != 0 {
			t.Errorf("expected no args without recovery policy, got %v", args)
		}
	})
}
