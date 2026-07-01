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

package v1alpha1

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
)

func TestSetSparkConnectDefaultsSetsRestartConfig(t *testing.T) {
	conn := &SparkConnect{}

	SetSparkConnectDefaults(conn)

	if conn.Spec.RestartConfig.RestartPolicy != SparkConnectRestartPolicyAlways {
		t.Fatalf("restart policy = %q, want %q", conn.Spec.RestartConfig.RestartPolicy, SparkConnectRestartPolicyAlways)
	}
	if conn.Spec.RestartConfig.MaxRestartAttempts != nil {
		t.Fatalf("max restart attempts = %v, want nil for unlimited", *conn.Spec.RestartConfig.MaxRestartAttempts)
	}
	if conn.Spec.RestartConfig.RestartBackoffMillis == nil {
		t.Fatal("restart backoff millis = nil, want default")
	}
	if *conn.Spec.RestartConfig.RestartBackoffMillis != 5000 {
		t.Fatalf("restart backoff millis = %d, want 5000", *conn.Spec.RestartConfig.RestartBackoffMillis)
	}
}

func TestAddToSchemeRegistersSparkConnectDefaults(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add v1alpha1 to scheme: %v", err)
	}

	conn := &SparkConnect{}
	scheme.Default(conn)

	if conn.Spec.RestartConfig.RestartPolicy != SparkConnectRestartPolicyAlways {
		t.Fatalf("restart policy = %q, want %q", conn.Spec.RestartConfig.RestartPolicy, SparkConnectRestartPolicyAlways)
	}
	if conn.Spec.RestartConfig.RestartBackoffMillis == nil {
		t.Fatal("restart backoff millis was not defaulted")
	}
}
