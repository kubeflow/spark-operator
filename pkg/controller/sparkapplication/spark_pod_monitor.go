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
	"k8s.io/client-go/util/workqueue"
	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	"k8s.io/spark-on-k8s-operator/pkg/config"
)

// sparkPodMonitor monitors Spark executor pods and update the SparkApplication objects accordingly.
type sparkPodMonitor struct {
	// Client to the Kubernetes API.
	kubeClient clientset.Interface
	// sparkPodInformer is a controller for listing uninitialized Spark Pods.
	sparkPodInformer cache.Controller
	// workQueue is a channel used to notify the controller of SparkApp updates.
	workQueue workqueue.RateLimitingInterface
}

// newSparkPodMonitor creates a new sparkPodMonitor instance.
func newSparkPodMonitor(
	kubeClient clientset.Interface,
	namespace string,
	workQueue workqueue.RateLimitingInterface) *sparkPodMonitor {
	monitor := &sparkPodMonitor{
		kubeClient: kubeClient,
		workQueue:  workQueue,
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
}

func (s *sparkPodMonitor) onPodAdded(obj interface{}) {
	pod := obj.(*apiv1.Pod)
	s.enqueueSparkAppForUpdate(pod)
}

func (s *sparkPodMonitor) onPodUpdated(old, updated interface{}) {
	oldPod := old.(*apiv1.Pod)
	updatedPod := updated.(*apiv1.Pod)

	if updatedPod.ResourceVersion == oldPod.ResourceVersion {
		return
	}

	s.enqueueSparkAppForUpdate(updatedPod)

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
	s.enqueueSparkAppForUpdate(deletedPod)
}

func (s *sparkPodMonitor) enqueueSparkAppForUpdate(pod *apiv1.Pod) {

	if appKey, ok := createMetaNamespaceKey(pod); ok {
		glog.V(2).Infof("Enqueuing SparkApp %s", appKey)
		s.workQueue.AddRateLimited(appKey)
	}
}

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
