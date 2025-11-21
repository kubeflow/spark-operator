/*
Copyright 2024 The Kubeflow authors.

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

package util

import (
	corev1 "k8s.io/api/core/v1"

	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

// IsLaunchedBySparkOperator returns whether the given pod is launched by the Spark Operator.
func IsLaunchedBySparkOperator(pod *corev1.Pod) bool {
	return pod.Labels[common.LabelLaunchedBySparkOperator] == "true"
}

// IsDriverPod returns whether the given pod is a Spark driver Pod.
func IsDriverPod(pod *corev1.Pod) bool {
	return pod.Labels[common.LabelSparkRole] == common.SparkRoleDriver
}

// IsExecutorPod returns whether the given pod is a Spark executor Pod.
func IsExecutorPod(pod *corev1.Pod) bool {
	return pod.Labels[common.LabelSparkRole] == common.SparkRoleExecutor
}

// GetSparkExecutorID returns the Spark executor ID by checking out pod labels.
func GetSparkExecutorID(pod *corev1.Pod) string {
	return pod.Labels[common.LabelSparkExecutorID]
}

// GetAppName returns the spark application name by checking out pod labels.
func GetAppName(pod *corev1.Pod) string {
	return pod.Labels[common.LabelSparkAppName]
}

// GetConnName returns the spark connection name by checking out pod labels.
func GetConnName(pod *corev1.Pod) string {
	return pod.Labels[common.LabelSparkConnectName]
}

// GetSparkApplicationID returns the spark application ID by checking out pod labels.
func GetSparkApplicationID(pod *corev1.Pod) string {
	return pod.Labels[common.LabelSparkApplicationSelector]
}

func IsPodReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func ShouldProcessPodUpdate(oldPod, newPod *corev1.Pod) bool {
	if newPod.Status.Phase != oldPod.Status.Phase {
		return true
	}

	// Only driver pod transitions can unblock the application status while the phase is pending.
	if !IsDriverPod(newPod) {
		return false
	}

	if DriverFailureReasonChanged(oldPod, newPod) {
		return true
	}

	return BecameUnschedulable(oldPod, newPod)
}

func DriverFailureReasonChanged(oldPod, newPod *corev1.Pod) bool {
	oldReason := GetPodFailureReason(oldPod)
	newReason := GetPodFailureReason(newPod)

	return newReason != "" && newReason != oldReason
}

func GetPodFailureReason(pod *corev1.Pod) string {
	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Waiting == nil {
			continue
		}

		switch status.State.Waiting.Reason {
		case common.ReasonImagePullBackOff, common.ReasonErrImagePull:
			return status.State.Waiting.Reason
		}
	}

	return ""
}

func BecameUnschedulable(oldPod, newPod *corev1.Pod) bool {
	return !IsPodUnschedulable(oldPod) && IsPodUnschedulable(newPod)
}

func IsPodUnschedulable(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodScheduled &&
			condition.Status == corev1.ConditionFalse &&
			condition.Reason == corev1.PodReasonUnschedulable {
			return true
		}
	}

	return false
}
