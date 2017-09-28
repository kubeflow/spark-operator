package initializer

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/golang/glog"

	"github.com/liyinan926/spark-operator/pkg/config"
	"github.com/liyinan926/spark-operator/pkg/secret"

	"k8s.io/api/admissionregistration/v1alpha1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

const (
	// InitializerName is the name that will appear in the list of pending initializers in Pod spec.
	initializerName = "pod-initializer.spark-operator.k8s.io"
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
	syncHandler func(key string) error
}

// NewController creates a new instance of Controller.
func NewController(kubeClient clientset.Interface) *Controller {
	controller := &Controller{
		kubeClient: kubeClient,
		queue:      workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "spark-initializer"),
	}
	controller.syncHandler = controller.syncSparkPod

	restClient := kubeClient.CoreV1().RESTClient()
	watchlist := cache.NewListWatchFromClient(restClient, "pods", apiv1.NamespaceAll, fields.Everything())
	// Wrap the returned watchlist to workaround the inability to include
	// the `IncludeUninitialized` list option when setting up watch clients.
	includeUninitializedWatchlist := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			options.IncludeUninitialized = true
			return watchlist.List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.IncludeUninitialized = true
			return watchlist.Watch(options)
		},
	}

	_, controller.sparkPodController = cache.NewInformer(
		includeUninitializedWatchlist,
		&apiv1.Pod{},
		30*time.Second,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    controller.onPodAdded,
			DeleteFunc: controller.onPodDeleted,
		},
	)

	return controller
}

// Run runs the initializer controller.
func (ic *Controller) Run(threadiness int, stopCh <-chan struct{}, errCh chan<- error) {
	defer utilruntime.HandleCrash()
	defer ic.queue.ShutDown()

	glog.Info("Starting the Spark Pod initializer controller")
	defer glog.Info("Shutting down the Spark Pod initializer controller")

	glog.Infof("Adding the InitializerConfiguration %s...", initializerConfigName)
	err := ic.addInitializationConfig()
	if err != nil {
		errCh <- fmt.Errorf("Failed to add InitializationConfiguration %s: %v", initializerConfigName, err)
		return
	}

	glog.Info("Starting the Pod controller...")
	go ic.sparkPodController.Run(stopCh)

	glog.Info("Starting the workers of the Spark Pod initializer controller...")
	// Start up worker threads based on threadiness.
	for i := 0; i < threadiness; i++ {
		// runWorker will loop until "something bad" happens. Until will then rekick
		// the worker after one second.
		go wait.Until(ic.runWorker, time.Second, stopCh)
	}

	<-stopCh

	glog.Infof("Deleting the InitializerConfiguration %s...", initializerConfigName)
	err = ic.deleteInitializationConfig()
	if err != nil {
		errCh <- fmt.Errorf("Failed to delete InitializationConfiguration %s: %v", initializerConfigName, err)
		return
	}

	errCh <- nil
}

func (ic *Controller) addInitializationConfig() error {
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

	icClient := ic.kubeClient.AdmissionregistrationV1alpha1().InitializerConfigurations()
	existingConfig, err := icClient.Get(initializerConfigName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			// InitializerConfig wasn't found.
			_, err = icClient.Create(&sparkPodInitializerConfig)
			if err != nil {
				return fmt.Errorf("Failed to create InitializerConfiguration: %v", err)
			}
			return nil
		}
		// API error.
		return fmt.Errorf("Failed to get InitializerConfiguration: %v", err)
	}

	// InitializerConfig was found, check we are in the list.
	found := false
	for _, initializer := range existingConfig.Initializers {
		if initializer.Name == initializerName {
			found = true
			break
		}
	}

	if found {
		glog.Warning("InitializerConfiguration %s with Initializer %s already exists", initializerConfigName, initializerName)
		return nil
	}

	glog.Warning("Found InitializerConfiguration %s without Initializer %s", initializerConfigName, initializerName)
	existingConfig.Initializers = append(existingConfig.Initializers, sparkPodInitializer)
	glog.Infof("Updating InitializerConfiguration %s", initializerConfigName)
	_, err = icClient.Update(existingConfig)
	if err != nil {
		return fmt.Errorf("Failed to update InitializerConfiguration: %v", err)
	}
	return nil
}

func (ic *Controller) deleteInitializationConfig() error {
	err := ic.kubeClient.AdmissionregistrationV1alpha1().InitializerConfigurations().Delete(initializerConfigName, &metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("Failed to delete InitializerConfiguration: %v", err)
	}
	return nil
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

	err := ic.syncHandler(key.(string))
	if err == nil {
		// Successfully processed the key or the key was not found so tell the queue to stop tracking
		// history for your key. This will reset things like failure counts for per-item rate limiting.
		ic.queue.Forget(key)
		return true
	}

	// There was a failure so be sure to report it. This method allows for pluggable error handling
	// which can be used for things like cluster-monitoring
	utilruntime.HandleError(fmt.Errorf("Sync pod %q failed with %v", key, err))
	// Since we failed, we should requeue the item to work on later.  This method will add a backoff
	// to avoid hotlooping on particular items (they're probably still not going to work right away)
	// and overall controller protection (everything I've done is broken, this controller needs to
	// calm down or it can starve other useful work) cases.
	ic.queue.AddRateLimited(key)

	return true
}

// syncSparkPod does the actual processing of the given Spark Pod.
func (ic *Controller) syncSparkPod(key string) error {
	namespace, name, err := getNamespaceName(key)
	pod, err := ic.kubeClient.CoreV1().Pods(namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	glog.Infof("Processing Spark %s pod %s", pod.Labels[sparkRoleLabel], pod.Name)

	// Make a copy.
	copy, err := runtime.NewScheme().DeepCopy(pod)
	if err != nil {
		return err
	}
	modifiedPod := copy.(*apiv1.Pod)
	if len(modifiedPod.Spec.Containers) <= 0 {
		return fmt.Errorf("No container found in Pod %s", modifiedPod.Name)
	}
	// We assume that the first container is the Spark container.
	appContainer := &modifiedPod.Spec.Containers[0]

	// Perform the initialization tasks.
	addOwnerReference(modifiedPod)
	handleConfigMaps(modifiedPod, appContainer)
	handleSecrets(modifiedPod, appContainer)
	// Remove this initializer from the list of pending intializers and update the Pod.
	remoteInitializer(modifiedPod)

	return patchPod(pod, modifiedPod, ic.kubeClient)
}

// onPodAdded is the callback function called when an event for a new Pod is informed.
func (ic *Controller) onPodAdded(obj interface{}) {
	pod, ok := obj.(*apiv1.Pod)
	if !ok {
		glog.Errorf("Received non-pod object: %v", obj)
		return
	}

	// The presence of the Initializer in the pending list of Initializers in the pod
	// is a sign that the pod is uninitialized.
	if isInitializerPresent(pod) {
		if isSparkPod(pod) {
			ic.queue.AddRateLimited(getQueueKey(pod))
		} else {
			// For non-Spark pods we don't put them into the queue.
			handleNonSparkPod(pod, ic.kubeClient)
		}
	}
}

// onPodDeleted is the callback function called when an event for a deleted Pod is informed.
func (ic *Controller) onPodDeleted(obj interface{}) {
	pod, ok := obj.(*apiv1.Pod)
	if !ok {
		glog.Errorf("Received non-pod object: %v", obj)
	}

	if isSparkPod(pod) {
		glog.Infof("Spark %s pod %s was deleted, dequeuing it...", pod.Labels[sparkRoleLabel], pod.Name)
		key := getQueueKey(pod)
		ic.queue.Forget(key)
		ic.queue.Done(key)
	}
}

func getQueueKey(pod *apiv1.Pod) string {
	return fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
}

func getNamespaceName(key string) (string, string, error) {
	parts := strings.Split(key, "/")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("Malformed queue key %s", key)
	}

	return parts[0], parts[1], nil
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

// isSparkPod tells if a Pod is a Spark Pod.
func isSparkPod(pod *apiv1.Pod) bool {
	sparkRole, ok := pod.Labels[sparkRoleLabel]
	return ok && (sparkRole == sparkDriverRole || sparkRole == sparkExecutorRole)
}

func handleNonSparkPod(pod *apiv1.Pod, clientset clientset.Interface) error {
	// Make a copy.
	copy, err := runtime.NewScheme().DeepCopy(pod)
	if err != nil {
		return err
	}
	podCopy := copy.(*apiv1.Pod)

	// Remove this initializer from the list of pending intializers and update the Pod.
	remoteInitializer(podCopy)

	return updatePod(podCopy, clientset)
}

func handleConfigMaps(pod *apiv1.Pod, container *apiv1.Container) {
	sparkConfigMapName, ok := pod.Annotations[config.SparkConfigMapAnnotation]
	if ok {
		glog.Infof("Mounting Spark ConfigMap %s to pod %s", sparkConfigMapName, pod.Name)
		volumeName := config.AddSparkConfigMapVolumeToPod(sparkConfigMapName, pod)
		config.MountSparkConfigMapToContainer(volumeName, config.DefaultSparkConfDir, container)
	}
	hadoopConfigMapName, ok := pod.Annotations[config.HadoopConfigMapAnnotation]
	if ok {
		glog.Infof("Mounting Hadoop ConfigMap %s to pod %s", hadoopConfigMapName, pod.Name)
		volumeName := config.AddHadoopConfigMapVolumeToPod(hadoopConfigMapName, pod)
		config.MountHadoopConfigMapToContainer(volumeName, config.DefaultHadoopConfDir, container)
	}
}

func handleSecrets(pod *apiv1.Pod, container *apiv1.Container) {
	secretName, mountPath, found := secret.FindGCPServiceAccountSecret(pod.Annotations)
	if found {
		glog.Infof("Mounting GCP service account secret %s to pod %s", secretName, pod.Name)
		secret.AddSecretVolumeToPod(secret.ServiceAccountSecretVolumeName, secretName, pod)
		secret.MountServiceAccountSecretToContainer(mountPath, container)
	}

	secrets := secret.FindGeneralSecrets(pod.Annotations)
	for secretName, mountPath := range secrets {
		glog.Infof("Mounting secret %s to pod %s", secretName, pod.Name)
		secretVolumeName := secretName + "-volume"
		secret.AddSecretVolumeToPod(secretVolumeName, secretName, pod)
		secret.MountSecretToContainer(secretVolumeName, mountPath, container)
	}
}

func addOwnerReference(pod *apiv1.Pod) {
	ownerReferenceStr, ok := pod.Annotations[config.OwnerReferenceAnnotation]
	if ok {
		ownerReference := &metav1.OwnerReference{}
		err := ownerReference.Unmarshal([]byte(ownerReferenceStr))
		if err != nil {
			glog.Errorf("Failed to add OwnerReference to Pod %s: %v", pod.Name, err)
		}
		pod.ObjectMeta.OwnerReferences = append(pod.ObjectMeta.OwnerReferences, *ownerReference)
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

	glog.Infof("Removed initializer on pod %s", pod.Name)
	return
}

func updatePod(newPod *apiv1.Pod, clientset clientset.Interface) error {
	glog.Infof("Updating pod %s", newPod.Name)
	_, err := clientset.CoreV1().Pods(newPod.Namespace).Update(newPod)
	if err != nil {
		return err
	}
	return nil
}

func patchPod(originalPod, modifiedPod *apiv1.Pod, clientset clientset.Interface) error {
	originalData, err := json.Marshal(originalPod)
	if err != nil {
		return err
	}
	modifiedData, err := json.Marshal(modifiedPod)
	if err != nil {
		return err
	}

	patch, err := strategicpatch.CreateTwoWayMergePatch(originalData, modifiedData, apiv1.Pod{})
	if err != nil {
		return err
	}
	_, err = clientset.CoreV1().Pods(originalPod.Namespace).Patch(originalPod.Name, types.StrategicMergePatchType, patch)
	if err != nil {
		return err
	}

	return nil
}
