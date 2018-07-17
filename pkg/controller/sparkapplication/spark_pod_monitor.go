/*
Copyright 2017 Google LLC

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
	"time"

	"github.com/golang/glog"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	"k8s.io/spark-on-k8s-operator/pkg/config"
)

// sparkPodMonitor monitors Spark executor pods and update the SparkApplication objects accordingly.
type sparkPodMonitor struct {
	// Client to the Kubernetes API.
	kubeClient clientset.Interface
	// sparkPodInformer is a controller for listing uninitialized Spark Pods.
	sparkPodInformer cache.Controller
	// podStateReportingChan is a channel used to notify the controller of Spark pod state updates.
	podStateReportingChan chan<- interface{}
}

// driverStateUpdate encapsulates state update of the driver.
type driverStateUpdate struct {
	appNamespace   string         // Namespace in which the application and driver pod run.
	appName        string         // Name of the application.
	appID          string         // Application ID.
	podName        string         // Name of the driver pod.
	nodeName       string         // Name of the node the driver pod runs on.
	podPhase       apiv1.PodPhase // Driver pod phase.
	completionTime metav1.Time    // Time the driver completes.
}

// executorStateUpdate encapsulates state update of an executor.
type executorStateUpdate struct {
	appNamespace string                 // Namespace in which the application and executor pods run.
	appName      string                 // Name of the application.
	appID        string                 // Application ID.
	podName      string                 // Name of the executor pod.
	executorID   string                 // Spark executor ID.
	state        v1alpha1.ExecutorState // Executor state.
}

// newSparkPodMonitor creates a new sparkPodMonitor instance.
func newSparkPodMonitor(
	kubeClient clientset.Interface,
	namespace string,
	podStateReportingChan chan<- interface{}) *sparkPodMonitor {
	monitor := &sparkPodMonitor{
		kubeClient:            kubeClient,
		podStateReportingChan: podStateReportingChan,
	}

	podInterface := kubeClient.CoreV1().Pods(namespace)
	sparkPodWatchList := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			options.LabelSelector = fmt.Sprintf("%s,%s", sparkRoleLabel, config.LaunchedBySparkOperatorLabel)
			return podInterface.List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.LabelSelector = fmt.Sprintf("%s,%s", sparkRoleLabel, config.LaunchedBySparkOperatorLabel)
			return podInterface.Watch(options)
		},
	}

	_, monitor.sparkPodInformer = cache.NewInformer(
		sparkPodWatchList,
		&apiv1.Pod{},
		// resyncPeriod. Every resyncPeriod, all resources in the cache will retrigger events.
		// Set to 0 to disable the resync.
		60*time.Second,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    monitor.onPodAdded,
			UpdateFunc: monitor.onPodUpdated,
			DeleteFunc: monitor.onPodDeleted,
		},
	)

	return monitor
}

func (s *sparkPodMonitor) run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()

	glog.Info("Starting the Spark Pod monitor")
	defer glog.Info("Stopping the Spark Pod monitor")

	go s.sparkPodInformer.Run(stopCh)

	// Wait for all involved caches to be synced, before processing items from the queue is started
	if !cache.WaitForCacheSync(stopCh, s.sparkPodInformer.HasSynced) {
		utilruntime.HandleError(fmt.Errorf("timed out waiting for cache to sync"))
		return
	}

	<-stopCh
	close(s.podStateReportingChan)
}

func (s *sparkPodMonitor) onPodAdded(obj interface{}) {
	pod := obj.(*apiv1.Pod)
	if isDriverPod(pod) {
		s.updateDriverState(pod)
	} else if isExecutorPod(pod) {
		s.updateExecutorState(pod)
	}
}

func (s *sparkPodMonitor) onPodUpdated(old, updated interface{}) {
	oldPod := old.(*apiv1.Pod)
	updatedPod := updated.(*apiv1.Pod)

	if updatedPod.ResourceVersion == oldPod.ResourceVersion {
		return
	}

	if isDriverPod(updatedPod) {
		s.updateDriverState(updatedPod)
	} else if isExecutorPod(updatedPod) {
		s.updateExecutorState(updatedPod)
	}
}

func (s *sparkPodMonitor) onPodDeleted(obj interface{}) {
	var deletedPod *apiv1.Pod

	switch obj.(type) {
	case *apiv1.Pod:
		deletedPod = obj.(*apiv1.Pod)
	case cache.DeletedFinalStateUnknown:
		deletedObj := obj.(cache.DeletedFinalStateUnknown).Obj
		deletedPod = deletedObj.(*apiv1.Pod)
	}

	if deletedPod == nil {
		return
	}

	deletedPod = deletedPod.DeepCopy()
	if isDriverPod(deletedPod) {
		if deletedPod.Status.Phase != apiv1.PodSucceeded && deletedPod.Status.Phase != apiv1.PodFailed {
			// The driver pod was deleted before it succeeded or failed. Treat deletion as failure in this case so the
			// application gets restarted if the RestartPolicy is Always or OnFailure.
			deletedPod.Status.Phase = apiv1.PodFailed
			// No update is reported if the deleted driver pod already terminated.
			s.updateDriverState(deletedPod)
		}
	} else if isExecutorPod(deletedPod) {
		s.updateExecutorState(deletedPod)
	}
}

func (s *sparkPodMonitor) updateDriverState(pod *apiv1.Pod) {
	if appName, ok := getAppName(pod); ok {
		update := driverStateUpdate{
			appNamespace: pod.Namespace,
			appName:      appName,
			appID:        getAppID(pod),
			podName:      pod.Name,
			nodeName:     pod.Spec.NodeName,
			podPhase:     pod.Status.Phase,
		}
		if pod.Status.Phase == apiv1.PodSucceeded {
			update.completionTime = metav1.Now()
		}

		s.podStateReportingChan <- &update
	}
}

func (s *sparkPodMonitor) updateExecutorState(pod *apiv1.Pod) {
	if appName, ok := getAppName(pod); ok {
		s.podStateReportingChan <- &executorStateUpdate{
			appNamespace: pod.Namespace,
			appName:      appName,
			appID:        getAppID(pod),
			podName:      pod.Name,
			executorID:   getExecutorID(pod),
			state:        podPhaseToExecutorState(pod.Status.Phase),
		}
	}
}

func getAppName(pod *apiv1.Pod) (string, bool) {
	appName, ok := pod.Labels[config.SparkAppNameLabel]
	return appName, ok
}

func getAppID(pod *apiv1.Pod) string {
	return pod.Labels[config.SparkAppIDLabel]
}

func isDriverPod(pod *apiv1.Pod) bool {
	return pod.Labels[sparkRoleLabel] == sparkDriverRole
}

func isExecutorPod(pod *apiv1.Pod) bool {
	return pod.Labels[sparkRoleLabel] == sparkExecutorRole
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

func getExecutorID(pod *apiv1.Pod) string {
	return pod.Labels[sparkExecutorIDLabel]
}
