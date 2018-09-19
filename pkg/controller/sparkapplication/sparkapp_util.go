package sparkapplication

import (
	"errors"
	"fmt"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
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
	return pod.Labels[config.SparkApplicationIDLabel]
}

func podPhaseToExecutorState(podPhase apiv1.PodPhase) v1alpha1.ExecutorState {
	switch podPhase {
	case apiv1.PodPending:
		return v1alpha1.ExecutorPendingState
	case apiv1.PodRunning:
		return v1alpha1.ExecutorRunningState
	case apiv1.PodSucceeded:
		return v1alpha1.ExecutorCompletedState
	case apiv1.PodFailed:
		return v1alpha1.ExecutorFailedState
	default:
		return v1alpha1.ExecutorUnknownState
	}
}

func getApplicationKey(namespace, name string) (string, error) {
	return cache.MetaNamespaceKeyFunc(&metav1.ObjectMeta{
		Namespace: namespace,
		Name:      name,
	})
}

func isAppTerminated(appState v1alpha1.ApplicationStateType) bool {
	return appState == v1alpha1.CompletedState || appState == v1alpha1.FailedState
}

func isExecutorTerminated(executorState v1alpha1.ExecutorState) bool {
	return executorState == v1alpha1.ExecutorCompletedState || executorState == v1alpha1.ExecutorFailedState
}

func driverPodPhaseToApplicationState(podPhase apiv1.PodPhase) (v1alpha1.ApplicationStateType, error) {
	switch podPhase {
	case apiv1.PodPending:
		return v1alpha1.SubmittedState, nil
	case apiv1.PodRunning:
		return v1alpha1.RunningState, nil
	case apiv1.PodSucceeded:
		return v1alpha1.CompletedState, nil
	case apiv1.PodFailed:
		return v1alpha1.FailedState, nil
	}
	return "", errors.New(fmt.Sprintf("Invalid driver Pod Phase found: %s", podPhase))
}
