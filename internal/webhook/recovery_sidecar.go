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
	"fmt"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/features"
	"github.com/kubeflow/spark-operator/v2/pkg/statestore"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// recoveryDefaulterConfig holds the webhook-side configuration for the
// FencedRestart feature, set once at startup from flags.
type recoveryDefaulterConfig struct {
	agentImage string
	profiles   *statestore.ProfileRegistry
}

var recoveryConfig recoveryDefaulterConfig

// SetRecoveryDefaulterConfig configures recovery agent injection. Called by
// the webhook start command when the FencedRestart feature gate is enabled.
func SetRecoveryDefaulterConfig(agentImage string, profiles *statestore.ProfileRegistry) {
	recoveryConfig = recoveryDefaulterConfig{
		agentImage: agentImage,
		profiles:   profiles,
	}
}

// addRecoveryAgentSidecar injects the spark-recovery-agent sidecar into
// driver pods of applications with spec.restartPolicy.recovery set
// (FencedRestart feature gate). The agent heartbeats to the state store,
// serves the progress-marker API to the driver on localhost, and restores
// the latest committed marker onto a shared emptyDir before the driver
// starts. Idempotent: pods already carrying the agent container are left
// untouched.
func addRecoveryAgentSidecar(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	if !features.Enabled(features.FencedRestart) || app.Spec.RestartPolicy.Recovery == nil {
		return nil
	}
	if !util.IsDriverPod(pod) {
		return nil
	}
	for _, container := range pod.Spec.Containers {
		if container.Name == common.RecoveryAgentContainerName {
			return nil
		}
	}
	if recoveryConfig.agentImage == "" {
		return fmt.Errorf("FencedRestart is enabled for SparkApplication %s/%s but the webhook was started without --recovery-agent-image", app.Namespace, app.Name)
	}
	if recoveryConfig.profiles == nil {
		return fmt.Errorf("FencedRestart is enabled for SparkApplication %s/%s but the webhook was started without --recovery-store-profiles", app.Namespace, app.Name)
	}
	recovery := app.Spec.RestartPolicy.Recovery
	profile, ok := recoveryConfig.profiles.Get(recovery.StoreProfile)
	if !ok {
		return fmt.Errorf("SparkApplication %s/%s references unknown recovery store profile %q", app.Namespace, app.Name, recovery.StoreProfile)
	}

	// The epoch must come from the driver container's own env, not from
	// status.recoveryStatus: submitSparkApplication creates the driver pod
	// (triggering this webhook) before its status update persisting the
	// freshly-advanced epoch reaches the API server. A webhook read of
	// status can therefore observe the *previous* epoch, inject a
	// mismatched sidecar, and have the agent self-fence a healthy driver
	// the moment its first heartbeat sees the real (newer) epoch in the
	// store. The driver container in this same pod already carries the
	// correct value — submission.go set it from the identical in-memory
	// epoch used to fence — so reading it here is race-free by construction.
	driverIdx := findContainer(pod)
	if driverIdx < 0 {
		return fmt.Errorf("FencedRestart is enabled for SparkApplication %s/%s but no Spark driver container was found in pod %s", app.Namespace, app.Name, pod.Name)
	}
	driverEnv := pod.Spec.Containers[driverIdx].Env
	epochValue, ok := lookupEnvValue(driverEnv, common.EnvRecoveryEpoch)
	if !ok {
		return fmt.Errorf("FencedRestart is enabled for SparkApplication %s/%s but the driver container is missing %s; expected the operator to set it before submission", app.Namespace, app.Name, common.EnvRecoveryEpoch)
	}

	env := []corev1.EnvVar{
		{Name: common.EnvRecoveryJobNamespace, Value: app.Namespace},
		{Name: common.EnvRecoveryJobName, Value: app.Name},
		{Name: common.EnvRecoveryEpoch, Value: epochValue},
		{Name: common.EnvRecoveryStoreAddress, Value: profile.Address},
	}
	if restoreEpochValue, ok := lookupEnvValue(driverEnv, common.EnvRecoveryRestoreEpoch); ok {
		env = append(env, corev1.EnvVar{Name: common.EnvRecoveryRestoreEpoch, Value: restoreEpochValue})
	}
	if profile.Password != "" {
		env = append(env, corev1.EnvVar{Name: common.EnvRecoveryStorePassword, Value: profile.Password})
	}
	if recovery.HeartbeatInterval != nil {
		env = append(env, corev1.EnvVar{
			Name:  common.EnvRecoveryHeartbeatIntvl,
			Value: recovery.HeartbeatInterval.Duration.String(),
		})
	}
	if recovery.SnapshotMaxSizeBytes != nil {
		env = append(env, corev1.EnvVar{
			Name:  common.EnvRecoverySnapshotMaxSz,
			Value: strconv.FormatInt(*recovery.SnapshotMaxSizeBytes, 10),
		})
	}

	volumeMount := corev1.VolumeMount{
		Name:      common.RecoveryVolumeName,
		MountPath: common.RecoveryVolumeMountPath,
	}

	agent := corev1.Container{
		Name:  common.RecoveryAgentContainerName,
		Image: recoveryConfig.agentImage,
		Args:  []string{"agent", "start"},
		Env:   env,
		Ports: []corev1.ContainerPort{
			{Name: "recovery", ContainerPort: common.RecoveryAgentPort, Protocol: corev1.ProtocolTCP},
		},
		VolumeMounts: []corev1.VolumeMount{volumeMount},
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: ptr.To(false),
			ReadOnlyRootFilesystem:   ptr.To(true),
			RunAsNonRoot:             ptr.To(true),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
	}

	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
		Name:         common.RecoveryVolumeName,
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	})
	pod.Spec.Containers = append(pod.Spec.Containers, agent)

	// Mount the shared volume into the driver container so the application
	// can read the restored marker from a file as an alternative to the
	// localhost HTTP API.
	pod.Spec.Containers[driverIdx].VolumeMounts = append(pod.Spec.Containers[driverIdx].VolumeMounts, volumeMount)

	return nil
}

// lookupEnvValue returns the value of the named environment variable and
// whether it was present.
func lookupEnvValue(env []corev1.EnvVar, name string) (string, bool) {
	for _, e := range env {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}
