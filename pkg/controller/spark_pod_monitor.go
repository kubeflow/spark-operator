package controller

import (
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/liyinan926/spark-operator/pkg/apis/v1alpha1"
	"github.com/liyinan926/spark-operator/pkg/config"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

// SparkPodMonitor monitors Spark executor pods and update the SparkAppliation objects accordingly.
type SparkPodMonitor struct {
	// Client to the Kubernetes API.
	kubeClient clientset.Interface
	// sparkPodController is a controller for listing uninitialized Spark Pods.
	sparkPodController cache.Controller
	// A queue of uninitialized Pods that need to be processed by this initializer controller.
	queue workqueue.RateLimitingInterface
	// executorStateUpdateChan is a channel used to notify the controller of executor state updates.
	executorStateReportingChan chan<- executorStateUpdate
}

// executorStateUpdate encapsulates state update of an executor.
type executorStateUpdate struct {
	appID      string
	podName    string
	executorID string
	state      v1alpha1.ExecutorState
}

// newSparkPodMonitor creates a new SparkPodMonitor instance.
func newSparkPodMonitor(kubeClient clientset.Interface, executorStateReportingChan chan executorStateUpdate) *SparkPodMonitor {
	monitor := &SparkPodMonitor{
		kubeClient: kubeClient,
		queue:      workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "spark-pod-monitor"),
		executorStateReportingChan: executorStateReportingChan,
	}

	restClient := kubeClient.CoreV1().RESTClient()
	watchlist := cache.NewListWatchFromClient(restClient, "pods", apiv1.NamespaceAll, fields.Everything())
	sparkPodWatchList := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			options.LabelSelector = fmt.Sprintf("%s=%s", sparkRoleLabel, sparkExecutorRole)
			return watchlist.List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.LabelSelector = fmt.Sprintf("%s=%s", sparkRoleLabel, sparkExecutorRole)
			return watchlist.Watch(options)
		},
	}

	_, monitor.sparkPodController = cache.NewInformer(
		sparkPodWatchList,
		&apiv1.Pod{},
		30*time.Second,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    monitor.onPodAdded,
			UpdateFunc: monitor.onPodUpdated,
			DeleteFunc: monitor.onPodDeleted,
		},
	)

	return monitor
}

func (e *SparkPodMonitor) run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer e.queue.ShutDown()

	glog.Info("Starting the Spark executor Pod monitor")
	defer glog.Info("Shutting down the Spark executor Pod monitor")

	glog.Info("Starting the Pod controller")
	go e.sparkPodController.Run(stopCh)

	<-stopCh
	close(e.executorStateReportingChan)
}

func (e *SparkPodMonitor) onPodAdded(obj interface{}) {
	pod := obj.(*apiv1.Pod)
	if appID, ok := getAppID(pod); ok {
		e.executorStateReportingChan <- executorStateUpdate{
			appID:   appID,
			podName: pod.Name,
			state:   podPhaseToExecutorState(pod.Status.Phase),
		}
	}
}

func (e *SparkPodMonitor) onPodUpdated(old, updated interface{}) {
	updatedPod := updated.(*apiv1.Pod)
	if appID, ok := getAppID(updatedPod); ok {
		e.executorStateReportingChan <- executorStateUpdate{
			appID:   appID,
			podName: updatedPod.Name,
			state:   podPhaseToExecutorState(updatedPod.Status.Phase),
		}
	}
}

func (e *SparkPodMonitor) onPodDeleted(obj interface{}) {
	deletedPod := obj.(*apiv1.Pod)
	if appID, ok := getAppID(deletedPod); ok {
		e.executorStateReportingChan <- executorStateUpdate{
			appID:   appID,
			podName: deletedPod.Name,
			state:   podPhaseToExecutorState(deletedPod.Status.Phase),
		}
	}
}

func getAppID(pod *apiv1.Pod) (string, bool) {
	appID, ok := pod.Labels[config.SparkAppIDLabel]
	return appID, ok
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
	}
	return ""
}
