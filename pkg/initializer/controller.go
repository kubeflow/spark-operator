package initializer

import (
	"fmt"
	"time"

	"github.com/golang/glog"

	"k8s.io/api/admissionregistration/v1alpha1"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

const (
	InitializerName       = "spark-pod-initializer"
	InitializerConfigName = "spark-pod-initializer-config"
	SparkRoleLabel        = "spark-role"
	SparkDriverRole       = "driver"
	SparkExecutorRole     = "executor"
)

// Controller is an initializer controller that watches for uninitialized Spark driver and executor Pods.
// This initializer controller is responsible for the following initialization tasks:
// 1.
type Controller struct {
	// Client to the Kubernetes API.
	kubeClient clientset.Interface
	// sparkPodController is a controller for listing uninitialized Spark Pods.
	sparkPodController cache.Controller
	// A queue of uninitialized Pods that need to be processed by this initializer controller.
	queue workqueue.RateLimitingInterface
	// To allow injection of syncReplicaSet for testing.
	syncHandler func(pod *v1.Pod) error
}

// NewController creates a new instance of Controller.
func NewController(kubeClient clientset.Interface, podLabelSelector string) *Controller {
	controller := &Controller{
		kubeClient: kubeClient,
		queue:      workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "spark-initializer"),
	}

	controller.syncHandler = controller.syncSparkPods
	_, controller.sparkPodController = cache.NewInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				return kubeClient.CoreV1().Pods("").List(*buildListOptions(&options, podLabelSelector))
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				return kubeClient.CoreV1().Pods("").Watch(*buildListOptions(&options, podLabelSelector))
			},
		},
		&v1.Pod{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc: controller.addSparkPod,
		},
	)

	return controller
}

// Run runs the initializer controller.
func (ic *Controller) Run(threadiness int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer ic.queue.ShutDown()

	glog.Infof("Starting the Spark Pod initializer controller")
	defer glog.Infof("Shutting down the Spark Pod initializer controller")

	if !cache.WaitForCacheSync(stopCh, ic.sparkPodController.HasSynced) {
		return
	}

	ic.addInitializationConfig()

	// Start up worker threads based on threadiness.
	for i := 0; i < threadiness; i++ {
		// runWorker will loop until "something bad" happens. Until will then rekick
		// the worker after one second.
		go wait.Until(ic.runWorker, time.Second, stopCh)
	}

	<-stopCh
}

// runWorker runs a single controller worker.
func (ic *Controller) runWorker() {
	for ic.processNextItem() {
	}
}

// processNextItem processes the next item in the queue.
func (ic *Controller) processNextItem() bool {
	key, quit := ic.queue.Get()
	if quit {
		return false
	}
	defer ic.queue.Done(key)

	err := ic.syncHandler(key.(*v1.Pod))
	if err == nil {
		// Successfully processed the key so tell the queue to stop tracking history for your key.
		// This will reset things like failure counts for per-item rate limiting.
		ic.queue.Forget(key)
		return true
	}

	// There was a failure so be sure to report it. This method allows for pluggable error handling
	// which can be used for things like cluster-monitoring
	utilruntime.HandleError(fmt.Errorf("Sync %q failed with %v", key, err))
	// Since we failed, we should requeue the item to work on later.  This method will add a backoff
	// to avoid hotlooping on particular items (they're probably still not going to work right away)
	// and overall controller protection (everything I've done is broken, this controller needs to
	// calm down or it can starve other useful work) cases.
	ic.queue.AddRateLimited(key)

	return true
}

// syncSparkPods does the actual processing of the given Spark Pod.
func (ic *Controller) syncSparkPods(pod *v1.Pod) error {
	// Remove this initializer from the list of pending intializers.
	remoteInitializer(pod)
	return nil
}

// addSparkPod is the callback function called when an event for a new Pod is informed.
func (ic *Controller) addSparkPod(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		glog.Errorf("received non-pod object: %v", obj)
	}

	if ic.isSparkPod(pod) && ic.isUninitialized(pod) {
		ic.queue.AddRateLimited(pod)
	}
}

func (ic *Controller) isSparkPod(pod *v1.Pod) bool {
	sparkRole, ok := pod.Labels[SparkRoleLabel]
	return ok && (sparkRole == SparkDriverRole || sparkRole == SparkExecutorRole)
}

func (ic *Controller) isUninitialized(pod *v1.Pod) bool {
	unInitialized := false
	for _, condition := range pod.Status.Conditions {
		// We don't care already-initialized Pods.
		if condition.Type == v1.PodInitialized && condition.Status == v1.ConditionFalse {
			unInitialized = true
			break
		}
	}
	return unInitialized
}

func (ic *Controller) addInitializationConfig() {
	sparkPodInitializer := v1alpha1.Initializer{
		Name: InitializerName,
		Rules: []v1alpha1.Rule{
			{
				APIGroups:   []string{"*"},
				APIVersions: []string{"*"},
				Resources:   []string{"pods"},
			},
		},
	}
	sparkPodInitializerConfig := v1alpha1.InitializerConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: InitializerConfigName,
		},
		Initializers: []v1alpha1.Initializer{sparkPodInitializer},
	}

	existingConfig, err := ic.kubeClient.AdmissionregistrationV1alpha1().InitializerConfigurations().Get(InitializerConfigName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			// InitializerConfig wasn't found.
			_, err = ic.kubeClient.AdmissionregistrationV1alpha1().InitializerConfigurations().Create(&sparkPodInitializerConfig)
			if err != nil {
				glog.Errorf("Failed to set InitializerConfig: %v", err)
			}
		} else {
			// API error.
			glog.Errorf("Unable to get InitializerConfig: %v", err)
		}
	} else {
		// InitializerConfig was found, check we are in the list.
		found := false
		for _, initializer := range existingConfig.Initializers {
			if initializer.Name == InitializerName {
				found = true
				break
			}
		}
		if !found {
			existingConfig.Initializers = append(existingConfig.Initializers, sparkPodInitializer)
			_, err = ic.kubeClient.AdmissionregistrationV1alpha1().InitializerConfigurations().Update(existingConfig)
			if err != nil {
				glog.Errorf("Failed to update InitializerConfig: %v", err)
			}
		}
	}
}

// isInitializerApplicable returns if the initializer is applicable to a given Pod.
func isInitializerApplicable(pod *v1.Pod) bool {
	if pod.Initializers == nil {
		return false
	}

	for _, pending := range pod.Initializers.Pending {
		if pending.Name == InitializerName {
			return true
		}
	}
	return false
}

// remoteInitializer removes the initializer from the list of pending initializers of the given Pod.
func remoteInitializer(pod *v1.Pod) {
	if pod.Initializers == nil {
		return
	}

	var updated []metav1.Initializer
	for _, pending := range pod.Initializers.Pending {
		if pending.Name != InitializerName {
			updated = append(updated, pending)
		}
	}
	if len(updated) > 0 && len(updated) == len(pod.Initializers.Pending) {
		return
	}

	if len(updated) == 0 {
		pod.Initializers = nil
	} else {
		pod.Initializers.Pending = updated
	}

	glog.Infof("removed initializer on PersistentVolume %s", pod.Name)
	return
}

func buildListOptions(options *metav1.ListOptions, podLabelSelector string) *metav1.ListOptions {
	options.IncludeUninitialized = true
	if podLabelSelector != "" {
		options.LabelSelector = podLabelSelector
	}
	return options
}
