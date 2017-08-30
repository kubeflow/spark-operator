package initializer

import (
	"fmt"
	"time"

	"github.com/golang/glog"

	"github.com/liyinan926/spark-operator/pkg/config"
	"github.com/liyinan926/spark-operator/pkg/secret"

	"k8s.io/api/admissionregistration/v1alpha1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

const (
	// InitializerName is the name that will appear in the list of pending initializers in Pod spec.
	initializerName = "pod-initializer.spark.apache.k8s.io"
	// InitializerConfigName is the name of the InitializerConfig object.
	initializerConfigName = "spark-pod-initializer-config"
	// SparkRoleLabel is an label we use to distinguish Spark pods for other Pods.
	sparkRoleLabel = "spark-role"
	// SparkDriverRole is the value of the spark-role label assigned to Spark driver Pods.
	sparkDriverRole = "driver"
	// SparkExecutorRole is the value of the spark-role label assigned to Spark executor Pods.
	sparkExecutorRole = "executor"
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
	syncHandler func(pod *apiv1.Pod) error
}

// NewController creates a new instance of Controller.
func NewController(kubeClient clientset.Interface) *Controller {
	controller := &Controller{
		kubeClient: kubeClient,
		queue:      workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "spark-initializer"),
	}
	controller.syncHandler = controller.syncSparkPod
	_, controller.sparkPodController = cache.NewInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				options.IncludeUninitialized = true
				return kubeClient.CoreV1().Pods("").List(options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				options.IncludeUninitialized = true
				return kubeClient.CoreV1().Pods("").Watch(options)
			},
		},
		&apiv1.Pod{},
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

func (ic *Controller) addInitializationConfig() {
	sparkPodInitializer := v1alpha1.Initializer{
		Name: initializerName,
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
			Name: initializerConfigName,
		},
		Initializers: []v1alpha1.Initializer{sparkPodInitializer},
	}

	existingConfig, err := ic.kubeClient.AdmissionregistrationV1alpha1().InitializerConfigurations().Get(initializerConfigName, metav1.GetOptions{})
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
			if initializer.Name == initializerName {
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

	err := ic.syncHandler(key.(*apiv1.Pod))
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

// syncSparkPod does the actual processing of the given Spark Pod.
func (ic *Controller) syncSparkPod(pod *apiv1.Pod) error {
	// Make a copy.
	copy, err := scheme.Scheme.DeepCopy(pod)
	if err != nil {
		return err
	}
	podCopy := copy.(*apiv1.Pod)
	if len(podCopy.Spec.Containers) <= 0 {
		return fmt.Errorf("No container found in Pod %s", podCopy.Name)
	}
	// We assume that the first container is the Spark container.
	appContainer := &podCopy.Spec.Containers[0]

	handleConfigMaps(podCopy, appContainer)
	handleSecrets(podCopy, appContainer)

	// Remove this initializer from the list of pending intializers and update the Pod.
	remoteInitializer(podCopy)
	_, err = ic.kubeClient.Core().Pods(pod.Namespace).Update(podCopy)
	if err != nil {
		return err
	}
	return nil
}

// addSparkPod is the callback function called when an event for a new Pod is informed.
func (ic *Controller) addSparkPod(obj interface{}) {
	pod, ok := obj.(*apiv1.Pod)
	if !ok {
		glog.Errorf("received non-pod object: %v", obj)
	}

	// The presence of the Initializer in the pending list of Initializers in the pod
	// is a sign that the pod is uninitialized.
	if isInitializerPresent(pod) {
		if isSparkPod(pod) {
			ic.queue.AddRateLimited(pod)
		} else {
			// We don't deal with non-Spark pods so simply remove the initializer from the pending list.
			remoteInitializer(pod)
		}
	}
}

// TODO: this seems redundant given that the Controller takes in a labelSelector that
// selects only Pods with the spark-role label.
func isSparkPod(pod *apiv1.Pod) bool {
	sparkRole, ok := pod.Labels[sparkRoleLabel]
	return ok && (sparkRole == sparkDriverRole || sparkRole == sparkExecutorRole)
}

// isInitializerPresent returns if the list of pending Initializer of the given pod contains an instance of this Initializer.
func isInitializerPresent(pod *apiv1.Pod) bool {
	if pod.Initializers == nil {
		return false
	}

	for _, pending := range pod.Initializers.Pending {
		if pending.Name == initializerName {
			return true
		}
	}
	return false
}

func handleConfigMaps(pod *apiv1.Pod, container *apiv1.Container) {
	sparkConfigMapName, ok := pod.Annotations[config.SparkConfigMapAnnotation]
	if ok {
		volumeName := config.AddSparkConfigMapVolumeToPod(sparkConfigMapName, pod)
		config.MountSparkConfigMapToContainer(volumeName, config.DefaultSparkConfDir, container)
	}
	hadoopConfigMapName, ok := pod.Annotations[config.HadoopConfigMapAnnotation]
	if ok {
		volumeName := config.AddHadoopConfigMapVolumeToPod(hadoopConfigMapName, pod)
		config.MountHadoopConfigMapToContainer(volumeName, config.DefaultHadoopConfDir, container)
	}
}

func handleSecrets(pod *apiv1.Pod, container *apiv1.Container) {
	gcpServiceAccountSecretName, ok := pod.Annotations[config.GCPServiceAccountSecretAnnotation]
	if ok {
		secret.AddSecretVolumeToPod(secret.ServiceAccountSecretVolumeName, gcpServiceAccountSecretName, pod)
		secret.MountServiceAccountSecretToContainer(secret.ServiceAccountSecretVolumeName, container)
	}
}

// remoteInitializer removes the initializer from the list of pending initializers of the given Pod.
func remoteInitializer(pod *apiv1.Pod) {
	if pod.Initializers == nil {
		return
	}

	var updated []metav1.Initializer
	for _, pending := range pod.Initializers.Pending {
		if pending.Name != initializerName {
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

	glog.Infof("Removed initializer on PersistentVolume %s", pod.Name)
	return
}
