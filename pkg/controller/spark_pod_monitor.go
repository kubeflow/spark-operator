package controller

import (
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
)

// SparkPodMonitor monitors Spark executor pods and update the SparkAppliation objects accordingly.
type SparkPodMonitor struct {
	// Client to the Kubernetes API.
	kubeClient clientset.Interface
	// sparkPodController is a controller for listing uninitialized Spark Pods.
	sparkPodController cache.Controller
	// driverStateReportingChan is a channel used to notify the controller of driver state updates.
	driverStateReportingChan chan<- driverStateUpdate
	// executorStateUpdateChan is a channel used to notify the controller of executor state updates.
	executorStateReportingChan chan<- executorStateUpdate
}

// driverStateUpdate encapsulates state update of the driver.
type driverStateUpdate struct {
	appID   string
	podName string
}

// executorStateUpdate encapsulates state update of an executor.
type executorStateUpdate struct {
	appID      string
	podName    string
	executorID string
	state      v1alpha1.ExecutorState
}

// newSparkPodMonitor creates a new SparkPodMonitor instance.
func newSparkPodMonitor(
	kubeClient clientset.Interface,
	driverStateReportingChan chan<- driverStateUpdate,
	executorStateReportingChan chan<- executorStateUpdate) *SparkPodMonitor {
	monitor := &SparkPodMonitor{
		kubeClient:                 kubeClient,
		driverStateReportingChan:   driverStateReportingChan,
		executorStateReportingChan: executorStateReportingChan,
	}

	restClient := kubeClient.CoreV1().RESTClient()
	watchlist := cache.NewListWatchFromClient(restClient, "pods", apiv1.NamespaceAll, fields.Everything())
	sparkPodWatchList := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			options.LabelSelector = sparkRoleLabel
			return watchlist.List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.LabelSelector = sparkRoleLabel
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

func (s *SparkPodMonitor) run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()

	glog.Info("Starting the Spark executor Pod monitor")
	defer glog.Info("Shutting down the Spark executor Pod monitor")

	glog.Info("Starting the Pod controller")
	go s.sparkPodController.Run(stopCh)

	<-stopCh
	close(s.executorStateReportingChan)
}

func (s *SparkPodMonitor) onPodAdded(obj interface{}) {
	pod := obj.(*apiv1.Pod)
	if isDriverPod(pod) {
		if appID, ok := getAppID(pod); ok {
			s.driverStateReportingChan <- driverStateUpdate{appID: appID, podName: pod.Name}
		}
	} else if isExecutorPod(pod) {
		s.updateExecutorState(pod)
	}
}

func (s *SparkPodMonitor) onPodUpdated(old, updated interface{}) {
	updatedPod := updated.(*apiv1.Pod)
	if !isExecutorPod(updatedPod) {
		return
	}
	s.updateExecutorState(updatedPod)
}

func (s *SparkPodMonitor) onPodDeleted(obj interface{}) {
	deletedPod := obj.(*apiv1.Pod)
	if !isExecutorPod(deletedPod) {
		return
	}
	s.updateExecutorState(deletedPod)
}

func (s *SparkPodMonitor) updateExecutorState(pod *apiv1.Pod) {
	if appID, ok := getAppID(pod); ok {
		s.executorStateReportingChan <- executorStateUpdate{
			appID:   appID,
			podName: pod.Name,
			state:   podPhaseToExecutorState(pod.Status.Phase),
		}
	}
}

func getAppID(pod *apiv1.Pod) (string, bool) {
	appID, ok := pod.Labels[config.SparkAppIDLabel]
	return appID, ok
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
	}
	return ""
}
