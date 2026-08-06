/*
Copyright 2026 Kubeflow

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

package util

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
)

// ShouldRestartSparkConnectServerPod returns true when the SparkConnect restart policy allows
// the controller to restart the given server pod for its current phase.
func ShouldRestartSparkConnectServerPod(conn *v1alpha1.SparkConnect, pod *corev1.Pod) bool {
	policy := conn.Spec.RestartPolicy.RestartPolicyType
	if policy == "" {
		policy = v1alpha1.RestartPolicyTypeOnFailure
	}

	switch policy {
	case v1alpha1.RestartPolicyTypeNever:
		return false
	case v1alpha1.RestartPolicyTypeOnFailure:
		return pod.Status.Phase == corev1.PodFailed
	case v1alpha1.RestartPolicyTypeAlways:
		return pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded
	default:
		return false
	}
}

// HasSparkConnectRestartAttemptsRemaining returns true when the OnFailure retry budget has remaining retries.
func HasSparkConnectRestartAttemptsRemaining(conn *v1alpha1.SparkConnect) bool {
	if conn.Spec.RestartPolicy.OnFailureRetries == nil {
		return false
	}
	return conn.Status.RestartAttempts < *conn.Spec.RestartPolicy.OnFailureRetries
}

// SparkConnectRestartBackoffStartTime returns the time when restart backoff started.
func SparkConnectRestartBackoffStartTime(conn *v1alpha1.SparkConnect) (time.Time, bool) {
	condition := meta.FindStatusCondition(conn.Status.Conditions, string(v1alpha1.SparkConnectConditionServerPodReady))
	if condition == nil ||
		condition.Status != metav1.ConditionFalse ||
		condition.Reason != string(v1alpha1.SparkConnectConditionReasonRestarting) ||
		condition.LastTransitionTime.IsZero() {
		return time.Time{}, false
	}
	return condition.LastTransitionTime.Time, true
}

// SparkConnectRestartBackoff returns the restart backoff duration.
func SparkConnectRestartBackoff(conn *v1alpha1.SparkConnect) time.Duration {
	if conn.Spec.RestartPolicy.OnFailureRetryInterval == nil {
		return time.Duration(v1alpha1.DefaultSparkConnectOnFailureRetryInterval) * time.Second
	}
	return time.Duration(*conn.Spec.RestartPolicy.OnFailureRetryInterval) * time.Second
}
