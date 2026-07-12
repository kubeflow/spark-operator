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

package common

// Constants for fenced, progress-preserving restarts (FencedRestart gate).
const (
	// RecoveryAgentContainerName is the name of the sidecar injected into
	// driver pods of applications with spec.restartPolicy.recovery set.
	RecoveryAgentContainerName = "spark-recovery-agent"

	// RecoveryVolumeName is the shared emptyDir through which the restored
	// progress marker is handed to the driver container.
	RecoveryVolumeName = "spark-recovery"

	// RecoveryVolumeMountPath is where RecoveryVolumeName is mounted in both
	// the driver container and the recovery agent.
	RecoveryVolumeMountPath = "/var/run/spark-recovery"

	// RecoverySnapshotFileName is the file inside RecoveryVolumeMountPath
	// containing the restored marker, present only when one was restored.
	RecoverySnapshotFileName = "snapshot"

	// RecoveryAgentPort is the localhost port on which the recovery agent
	// serves the snapshot API to the driver.
	RecoveryAgentPort = 7691

	// Environment variables forming the recovery contract with the driver.
	EnvRecoveryEnabled      = "SPARK_RECOVERY_ENABLED"
	EnvRecoveryEpoch        = "SPARK_RECOVERY_EPOCH"
	EnvRecoveryRestoreEpoch = "SPARK_RECOVERY_RESTORE_EPOCH"
	EnvRecoveryAgentURL     = "SPARK_RECOVERY_AGENT_URL"

	// Environment variables consumed by the recovery agent sidecar.
	EnvRecoveryStoreAddress   = "SPARK_RECOVERY_STORE_ADDRESS"
	EnvRecoveryStorePassword  = "SPARK_RECOVERY_STORE_PASSWORD"
	EnvRecoveryJobNamespace   = "SPARK_RECOVERY_JOB_NAMESPACE"
	EnvRecoveryJobName        = "SPARK_RECOVERY_JOB_NAME"
	EnvRecoveryHeartbeatIntvl = "SPARK_RECOVERY_HEARTBEAT_INTERVAL"
	EnvRecoverySnapshotMaxSz  = "SPARK_RECOVERY_SNAPSHOT_MAX_BYTES"
)

// Event reasons for fenced restarts.
const (
	EventSparkApplicationEpochAdvanced      = "SparkApplicationEpochAdvanced"
	EventSparkApplicationMarkerRestored     = "SparkApplicationMarkerRestored"
	EventSparkApplicationNoMarkerObserved   = "SparkApplicationNoMarkerObserved"
	EventSparkApplicationRecoveryStoreError = "SparkApplicationRecoveryStoreError"
)
