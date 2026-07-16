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

func TestSetSparkConnectDefaultsSetsRestartPolicy(t *testing.T) {
	conn := &SparkConnect{}

	SetSparkConnectDefaults(conn)

	if conn.Spec.RestartPolicy.RestartPolicyType != RestartPolicyTypeOnFailure {
		t.Fatalf("restart policy type = %q, want %q", conn.Spec.RestartPolicy.RestartPolicyType, RestartPolicyTypeOnFailure)
	}
	if conn.Spec.RestartPolicy.OnFailureRetries == nil {
		t.Fatal("on failure retries = nil, want default")
	}
	if *conn.Spec.RestartPolicy.OnFailureRetries != DefaultSparkConnectOnFailureRetries {
		t.Fatalf("on failure retries = %d, want %d", *conn.Spec.RestartPolicy.OnFailureRetries, DefaultSparkConnectOnFailureRetries)
	}
	if conn.Spec.RestartPolicy.OnFailureRetryInterval == nil {
		t.Fatal("on failure retry interval = nil, want default")
	}
	if *conn.Spec.RestartPolicy.OnFailureRetryInterval != 5 {
		t.Fatalf("on failure retry interval = %d, want 5", *conn.Spec.RestartPolicy.OnFailureRetryInterval)
	}
}

func TestAddToSchemeRegistersSparkConnectDefaults(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add v1alpha1 to scheme: %v", err)
	}

	conn := &SparkConnect{}
	scheme.Default(conn)

	if conn.Spec.RestartPolicy.RestartPolicyType != RestartPolicyTypeOnFailure {
		t.Fatalf("restart policy type = %q, want %q", conn.Spec.RestartPolicy.RestartPolicyType, RestartPolicyTypeOnFailure)
	}
	if conn.Spec.RestartPolicy.OnFailureRetries == nil {
		t.Fatal("on failure retries was not defaulted")
	}
	if conn.Spec.RestartPolicy.OnFailureRetryInterval == nil {
		t.Fatal("on failure retry interval was not defaulted")
	}
}

func TestSetSparkConnectDefaultsPreservesOnFailureRetries(t *testing.T) {
	retries := int32(0)
	conn := &SparkConnect{
		Spec: SparkConnectSpec{
			RestartPolicy: RestartPolicy{
				OnFailureRetries: &retries,
			},
		},
	}

	SetSparkConnectDefaults(conn)

	if conn.Spec.RestartPolicy.OnFailureRetries == nil {
		t.Fatal("on failure retries = nil, want explicit value preserved")
	}
	if *conn.Spec.RestartPolicy.OnFailureRetries != 0 {
		t.Fatalf("on failure retries = %d, want 0", *conn.Spec.RestartPolicy.OnFailureRetries)
	}
}
