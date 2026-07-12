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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/features"
	"github.com/kubeflow/spark-operator/v2/pkg/statestore"
)

func recoveryTestApp() *v1beta2.SparkApplication {
	return &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{Name: "resumable-etl", Namespace: "default"},
		Spec: v1beta2.SparkApplicationSpec{
			RestartPolicy: v1beta2.RestartPolicy{
				Type:     v1beta2.RestartPolicyOnFailure,
				Recovery: &v1beta2.RecoveryPolicy{StoreProfile: "default"},
			},
		},
		Status: v1beta2.SparkApplicationStatus{
			RecoveryStatus: &v1beta2.RecoveryStatus{Epoch: 2},
		},
	}
}

func recoveryTestDriverPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "resumable-etl-driver",
			Namespace: "default",
			Labels: map[string]string{
				common.LabelSparkRole:    common.SparkRoleDriver,
				common.LabelSparkAppName: "resumable-etl",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  common.SparkDriverContainerName,
					Image: "spark:4.0.1",
					// Set by submission.go's recoveryEnvOption before the
					// driver pod is created; the webhook reads the epoch
					// from here rather than status.recoveryStatus.
					Env: []corev1.EnvVar{
						{Name: common.EnvRecoveryEpoch, Value: "2"},
					},
				},
			},
		},
	}
}

func setupRecoveryConfig(t *testing.T) {
	t.Helper()
	registry, err := statestore.ParseProfiles("default=redis:redis.spark-operator.svc:6379")
	if err != nil {
		t.Fatalf("ParseProfiles: %v", err)
	}
	SetRecoveryDefaulterConfig("spark-operator:test", registry)
	t.Cleanup(func() { SetRecoveryDefaulterConfig("", nil) })
}

func TestAddRecoveryAgentSidecarInjectsIntoDriver(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.FencedRestart, true)
	setupRecoveryConfig(t)

	pod := recoveryTestDriverPod()
	app := recoveryTestApp()
	if err := addRecoveryAgentSidecar(pod, app); err != nil {
		t.Fatalf("addRecoveryAgentSidecar: %v", err)
	}

	var agent *corev1.Container
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == common.RecoveryAgentContainerName {
			agent = &pod.Spec.Containers[i]
		}
	}
	if agent == nil {
		t.Fatal("expected recovery agent container to be injected")
	}
	if agent.Image != "spark-operator:test" {
		t.Errorf("unexpected agent image %q", agent.Image)
	}

	env := map[string]string{}
	for _, e := range agent.Env {
		env[e.Name] = e.Value
	}
	if env[common.EnvRecoveryEpoch] != "2" {
		t.Errorf("expected agent epoch env 2, got %q", env[common.EnvRecoveryEpoch])
	}
	if env[common.EnvRecoveryJobName] != "resumable-etl" || env[common.EnvRecoveryJobNamespace] != "default" {
		t.Errorf("job key env not set: %v", env)
	}
	if env[common.EnvRecoveryStoreAddress] != "redis.spark-operator.svc:6379" {
		t.Errorf("store address env not set: %v", env)
	}

	// The shared volume must exist and be mounted in both containers.
	foundVolume := false
	for _, v := range pod.Spec.Volumes {
		if v.Name == common.RecoveryVolumeName {
			foundVolume = true
		}
	}
	if !foundVolume {
		t.Error("expected shared recovery volume on the pod")
	}
	for _, c := range pod.Spec.Containers {
		mounted := false
		for _, m := range c.VolumeMounts {
			if m.Name == common.RecoveryVolumeName && m.MountPath == common.RecoveryVolumeMountPath {
				mounted = true
			}
		}
		if !mounted {
			t.Errorf("expected container %q to mount the recovery volume", c.Name)
		}
	}

	// Idempotency: a second invocation must not duplicate the container.
	if err := addRecoveryAgentSidecar(pod, app); err != nil {
		t.Fatalf("addRecoveryAgentSidecar (second call): %v", err)
	}
	count := 0
	for _, c := range pod.Spec.Containers {
		if c.Name == common.RecoveryAgentContainerName {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly one agent container after reinvocation, got %d", count)
	}
}

func TestAddRecoveryAgentSidecarSkips(t *testing.T) {
	t.Run("gate disabled", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.FencedRestart, false)
		pod := recoveryTestDriverPod()
		if err := addRecoveryAgentSidecar(pod, recoveryTestApp()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(pod.Spec.Containers) != 1 {
			t.Error("expected no injection when the gate is disabled")
		}
	})

	t.Run("recovery not configured", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.FencedRestart, true)
		setupRecoveryConfig(t)
		app := recoveryTestApp()
		app.Spec.RestartPolicy.Recovery = nil
		pod := recoveryTestDriverPod()
		if err := addRecoveryAgentSidecar(pod, app); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(pod.Spec.Containers) != 1 {
			t.Error("expected no injection when spec.restartPolicy.recovery is unset")
		}
	})

	t.Run("executor pod", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.FencedRestart, true)
		setupRecoveryConfig(t)
		pod := recoveryTestDriverPod()
		pod.Labels[common.LabelSparkRole] = common.SparkRoleExecutor
		if err := addRecoveryAgentSidecar(pod, recoveryTestApp()); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(pod.Spec.Containers) != 1 {
			t.Error("expected no injection into executor pods")
		}
	})

	t.Run("unknown store profile", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.FencedRestart, true)
		setupRecoveryConfig(t)
		app := recoveryTestApp()
		app.Spec.RestartPolicy.Recovery.StoreProfile = "missing"
		pod := recoveryTestDriverPod()
		if err := addRecoveryAgentSidecar(pod, app); err == nil {
			t.Error("expected error for unknown store profile")
		}
	})

	t.Run("driver container missing recovery epoch env", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.FencedRestart, true)
		setupRecoveryConfig(t)
		pod := recoveryTestDriverPod()
		pod.Spec.Containers[0].Env = nil
		if err := addRecoveryAgentSidecar(pod, recoveryTestApp()); err == nil {
			t.Error("expected error when the driver container carries no recovery epoch env")
		}
	})
}
