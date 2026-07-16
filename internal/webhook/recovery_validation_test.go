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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/features"
)

func validatorTestApp(policyType v1beta2.RestartPolicyType, recovery *v1beta2.RecoveryPolicy) *v1beta2.SparkApplication {
	return &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{Name: "resumable-etl", Namespace: "default"},
		Spec: v1beta2.SparkApplicationSpec{
			SparkVersion: "4.0.1",
			RestartPolicy: v1beta2.RestartPolicy{
				Type:     policyType,
				Recovery: recovery,
			},
		},
	}
}

func TestValidateRecoveryPolicy(t *testing.T) {
	validator := NewSparkApplicationValidator(nil, false)

	t.Run("rejected when the gate is disabled", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.FencedRestart, false)
		app := validatorTestApp(v1beta2.RestartPolicyOnFailure, &v1beta2.RecoveryPolicy{StoreProfile: "default"})
		if err := validator.validateRecoveryPolicy(app); err == nil {
			t.Error("expected rejection when FencedRestart gate is disabled")
		}
	})

	t.Run("rejected with restartPolicy Never", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.FencedRestart, true)
		app := validatorTestApp(v1beta2.RestartPolicyNever, &v1beta2.RecoveryPolicy{StoreProfile: "default"})
		if err := validator.validateRecoveryPolicy(app); err == nil {
			t.Error("expected rejection with restartPolicy Never")
		}
	})

	t.Run("rejected with inconsistent heartbeat timing", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.FencedRestart, true)
		app := validatorTestApp(v1beta2.RestartPolicyOnFailure, &v1beta2.RecoveryPolicy{
			StoreProfile:      "default",
			HeartbeatInterval: &metav1.Duration{Duration: 10 * time.Second},
			HeartbeatTTL:      &metav1.Duration{Duration: 15 * time.Second},
		})
		if err := validator.validateRecoveryPolicy(app); err == nil {
			t.Error("expected rejection when heartbeatTTL <= 2x heartbeatInterval")
		}
	})

	t.Run("accepted with a valid policy", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.FencedRestart, true)
		app := validatorTestApp(v1beta2.RestartPolicyOnFailure, &v1beta2.RecoveryPolicy{
			StoreProfile:      "default",
			HeartbeatInterval: &metav1.Duration{Duration: 10 * time.Second},
			HeartbeatTTL:      &metav1.Duration{Duration: 30 * time.Second},
		})
		if err := validator.validateRecoveryPolicy(app); err != nil {
			t.Errorf("expected valid policy to be accepted, got %v", err)
		}
	})

	t.Run("no-op without recovery", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.FencedRestart, false)
		app := validatorTestApp(v1beta2.RestartPolicyNever, nil)
		if err := validator.validateRecoveryPolicy(app); err != nil {
			t.Errorf("expected nil recovery to validate, got %v", err)
		}
	})
}
