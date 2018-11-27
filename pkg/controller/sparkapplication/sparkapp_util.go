/*
Copyright 2018 Google LLC

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

package sparkapplication

import (
	"fmt"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta1"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	apiv1 "k8s.io/api/core/v1"
)

// Helper method to create a key with namespace and appName
func createMetaNamespaceKey(pod *apiv1.Pod) (string, bool) {
	if appName, ok := getAppName(pod); ok {
		return fmt.Sprintf("%s/%s", pod.GetNamespace(), appName), true
	}
	return "", false
}

func getAppName(pod *apiv1.Pod) (string, bool) {
	appName, ok := pod.Labels[config.SparkAppNameLabel]
	return appName, ok
}

func isDriverPod(pod *apiv1.Pod) bool {
	return pod.Labels[config.SparkRoleLabel] == sparkDriverRole
}

func isExecutorPod(pod *apiv1.Pod) bool {
	return pod.Labels[config.SparkRoleLabel] == sparkExecutorRole
}

func getSparkApplicationID(pod *apiv1.Pod) string {
	return pod.Labels[config.SparkApplicationSelectorLabel]
}

func getDefaultDriverPodName(app *v1beta1.SparkApplication) string {
	return fmt.Sprintf("%s-driver", app.Name)
}

func getDefaultUIServiceName(app *v1beta1.SparkApplication) string {
	return fmt.Sprintf("%s-ui-svc", app.Name)
}

func getDefaultUIIngressName(app *v1beta1.SparkApplication) string {
	return fmt.Sprintf("%s-ui-ingress", app.Name)
}

func podPhaseToExecutorState(podPhase apiv1.PodPhase) v1beta1.ExecutorState {
	switch podPhase {
	case apiv1.PodPending:
		return v1beta1.ExecutorPendingState
	case apiv1.PodRunning:
		return v1beta1.ExecutorRunningState
	case apiv1.PodSucceeded:
		return v1beta1.ExecutorCompletedState
	case apiv1.PodFailed:
		return v1beta1.ExecutorFailedState
	default:
		return v1beta1.ExecutorUnknownState
	}
}

func isExecutorTerminated(executorState v1beta1.ExecutorState) bool {
	return executorState == v1beta1.ExecutorCompletedState || executorState == v1beta1.ExecutorFailedState
}

func driverPodPhaseToApplicationState(podPhase apiv1.PodPhase) v1beta1.ApplicationStateType {
	switch podPhase {
	case apiv1.PodPending:
		return v1beta1.SubmittedState
	case apiv1.PodRunning:
		return v1beta1.RunningState
	case apiv1.PodSucceeded:
		return v1beta1.SucceedingState
	case apiv1.PodFailed:
		return v1beta1.FailingState
	default:
		return v1beta1.UnknownState
	}
}
