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

	"github.com/kubeflow/spark-operator/pkg/common"
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

// GetSparkApplicationID returns the spark application ID by checking out pod labels.
func GetSparkApplicationID(pod *corev1.Pod) string {
	return pod.Labels[common.LabelSparkApplicationSelector]
}
