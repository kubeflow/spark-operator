/*
Copyright 2026 Kubeflow Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package webhook

import (
	"context"
	"testing"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
)

func TestSparkConnectDefaulterAppliesSchemeDefaults(t *testing.T) {
	conn := &v1alpha1.SparkConnect{}

	if err := NewSparkConnectDefaulter().Default(context.Background(), conn); err != nil {
		t.Fatalf("failed to default SparkConnect: %v", err)
	}

	if conn.Spec.RestartConfig.RestartPolicy != v1alpha1.SparkConnectRestartPolicyAlways {
		t.Fatalf("restart policy = %q, want %q", conn.Spec.RestartConfig.RestartPolicy, v1alpha1.SparkConnectRestartPolicyAlways)
	}
	if conn.Spec.RestartConfig.RestartBackoffMillis == nil {
		t.Fatal("restart backoff millis was not defaulted")
	}
}
