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

package initializer

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/golang/glog"

	"k8s.io/api/admissionregistration/v1alpha1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"k8s.io/spark-on-k8s-operator/pkg/config"
	"k8s.io/spark-on-k8s-operator/pkg/util"
)

const (
	// InitializerName is the name that will appear in the list of pending initializer in Pod spec.
	sparkPodInitializerName = "spark-pod-initializer.sparkoperator.k8s.io"
	// InitializerConfigName is the name of the InitializerConfig object.
	sparkPodInitializerConfigName = "spark-pod-initializer-config"
	// SparkRoleLabel is an label we use to distinguish Spark pods for other Pods.
	sparkRoleLabel = "spark-role"
	// SparkDriverRole is the value of the spark-role label assigned to Spark driver Pods.
	sparkDriverRole = "driver"
	// SparkExecutorRole is the value of the spark-role label assigned to Spark executor Pods.
	sparkExecutorRole = "executor"
)

// SparkPodInitializer watches uninitialized Spark driver and executor pods and modifies pod specs
// based on certain annotations on the pods. For example, it is responsible for mounting
// user-specified secrets and ConfigMaps into the driver and executor pods.
type SparkPodInitializer struct {
	// Client to the Kubernetes API.
	kubeClient clientset.Interface
	// podInformer is a shared informer for Pods (including uninitialized ones).
	podInformer cache.Controller
	// podStore is the store of cached Pods.
	podStore cache.Store
	// A queue of uninitialized Pods that need to be processed by this initializer.
	queue workqueue.RateLimitingInterface
	// To allow injection of syncHandler for testing.
	syncHandler func(key string) error
}

// New creates a new instance of Initializer.
func New(kubeClient clientset.Interface) *SparkPodInitializer {
	initializer := &SparkPodInitializer{
		kubeClient: kubeClient,
		queue: workqueue.NewNamedRateLimitingQueue(
			workqueue.DefaultControllerRateLimiter(),
			"spark-pod-initializer"),
	}
	initializer.syncHandler = initializer.syncSparkPod

	podInterface := kubeClient.CoreV1().Pods(apiv1.NamespaceAll)
	includeUninitializedWatchlist := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			options.IncludeUninitialized = true
			return podInterface.List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			options.IncludeUninitialized = true
			return podInterface.Watch(options)
		},
	}

	initializer.podStore, initializer.podInformer = cache.NewInformer(
		includeUninitializedWatchlist,
		&apiv1.Pod{},
		// resyncPeriod. Every resyncPeriod, all resources in the cache will re-trigger events.
		// Set to 0 to disable the resync.
		0*time.Second,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    initializer.onPodAdded,
			DeleteFunc: initializer.onPodDeleted,
		},
	)

	return initializer
}

// Start starts the initializer.
func (ic *SparkPodInitializer) Start(workers int, stopCh <-chan struct{}) error {
	glog.Info("Starting the Spark Pod initializer")

	if err := ic.addInitializationConfig(); err != nil {
		return err
	}

	glog.Info("Starting the Pod informer of the Spark Pod initializer")
	go ic.podInformer.Run(stopCh)

	// Wait for all involved caches to be synced, before processing items from the queue is started
	if !cache.WaitForCacheSync(stopCh, ic.podInformer.HasSynced) {
		return fmt.Errorf("timed out waiting for cache to sync")
	}

	glog.Info("Starting the workers of the Spark Pod initializer")
	// Start up worker threads.
	for i := 0; i < workers; i++ {
		// runWorker will loop until "something bad" happens. Until will then rekick
		// the worker after one second.
		go wait.Until(ic.runWorker, time.Second, stopCh)
	}

	return nil
}

// Stop stops the initializer.
func (ic *SparkPodInitializer) Stop() {
	glog.Info("Stopping the Spark Pod initializer")
	ic.queue.ShutDown()
	if err := ic.deleteInitializationConfig(); err != nil {
		glog.Errorf("failed to delete the InitializerConfiguration %s", sparkPodInitializerConfigName)
	}
}

func (ic *SparkPodInitializer) addInitializationConfig() error {
	sparkPodInitializer := v1alpha1.Initializer{
		Name: sparkPodInitializerName,
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
			Name: sparkPodInitializerConfigName,
		},
		Initializers: []v1alpha1.Initializer{sparkPodInitializer},
	}

	glog.Infof("Adding the InitializerConfiguration %s", sparkPodInitializerConfigName)
	icClient := ic.kubeClient.AdmissionregistrationV1alpha1().InitializerConfigurations()
	existingConfig, err := icClient.Get(sparkPodInitializerConfigName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			// InitializerConfig wasn't found.
			_, err = icClient.Create(&sparkPodInitializerConfig)
			if err != nil {
				return fmt.Errorf("failed to create InitializerConfiguration %s: %v", sparkPodInitializerConfigName, err)
			}
			return nil
		}
		// API error.
		return fmt.Errorf("failed to get InitializerConfiguration %s: %v", sparkPodInitializerConfigName, err)
	}

	// InitializerConfig was found, check we are in the list.
	found := false
	for _, existingInitializer := range existingConfig.Initializers {
		if existingInitializer.Name == sparkPodInitializerName {
			found = true
			break
		}
	}

	if found {
		glog.Warningf(
			"InitializerConfiguration %s with Initializer %s already exists",
			sparkPodInitializerConfigName,
			sparkPodInitializerName)
		return nil
	}

	glog.Warningf(
		"Found InitializerConfiguration %s without Initializer %s",
		sparkPodInitializerConfigName,
		sparkPodInitializerName)
	existingConfig.Initializers = append(existingConfig.Initializers, sparkPodInitializer)
	glog.Infof("Updating InitializerConfiguration %s", sparkPodInitializerConfigName)
	_, err = icClient.Update(existingConfig)
	if err != nil {
		return fmt.Errorf("failed to update InitializerConfiguration %s: %v", sparkPodInitializerConfigName, err)
	}

	return nil
}

func (ic *SparkPodInitializer) deleteInitializationConfig() error {
	glog.Infof("Deleting the InitializerConfiguration %s", sparkPodInitializerConfigName)
	var zero int64 = 0
	err := ic.kubeClient.AdmissionregistrationV1alpha1().InitializerConfigurations().Delete(
		sparkPodInitializerConfigName,
		&metav1.DeleteOptions{GracePeriodSeconds: &zero})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete InitializerConfiguration: %v", err)
	}

	return nil
}

// runWorker runs a single controller worker.
func (ic *SparkPodInitializer) runWorker() {
	defer utilruntime.HandleCrash()
	for ic.processNextItem() {
	}
}

// onPodAdded is the callback function called when an event for a new Pod is informed.
func (ic *SparkPodInitializer) onPodAdded(obj interface{}) {
	ic.enqueue(obj)
}

// onPodDeleted is the callback function called when an event for a deleted Pod is informed.
func (ic *SparkPodInitializer) onPodDeleted(obj interface{}) {
	ic.dequeue(obj)
}

// processNextItem processes the next item in the queue.
func (ic *SparkPodInitializer) processNextItem() bool {
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
	utilruntime.HandleError(fmt.Errorf("failed to sync pod %q: %v", key, err))
	// Since we failed, we should requeue the item to work on later.  This method will add a backoff
	// to avoid hotlooping on particular items (they're probably still not going to work right away)
	// and overall controller protection (everything I've done is broken, this controller needs to
	// calm down or it can starve other useful work) cases.
	ic.queue.AddRateLimited(key)

	return true
}

func (ic *SparkPodInitializer) syncSparkPod(key string) error {
	item, exists, err := ic.podStore.GetByKey(key)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	pod := item.(*apiv1.Pod)
	if !isInitializerPresent(pod) {
		return nil
	}
	if isSparkPod(pod) {
		_, err = ic.initializeSparkPod(pod)
	} else {
		err = ic.handleNonSparkPod(pod)
	}

	return err
}

// initializeSparkPod does the actual initialization of the given Spark Pod.
func (ic *SparkPodInitializer) initializeSparkPod(pod *apiv1.Pod) (*apiv1.Pod, error) {
	glog.Infof("Processing Spark %s pod %s", pod.Labels[sparkRoleLabel], pod.Name)

	// Make a copy.
	podCopy := pod.DeepCopy()

	if len(podCopy.Spec.Containers) <= 0 {
		return nil, fmt.Errorf("no container found in Pod %s", podCopy.Name)
	}
	// We assume that the first container is the Spark container.
	appContainer := &podCopy.Spec.Containers[0]

	// Perform the initialization tasks.
	addOwnerReference(podCopy)
	handleConfigMaps(podCopy, appContainer)
	handleVolumes(podCopy, appContainer)
	// Remove this initializer from the list of pending initializer and update the Pod.
	removeSelf(podCopy)

	return patchPod(pod, podCopy, ic.kubeClient)
}

func (ic *SparkPodInitializer) handleNonSparkPod(pod *apiv1.Pod) error {
	// Make a copy.
	podCopy := pod.DeepCopy()
	// Remove the name of itself from the list of pending initializer and update the Pod.
	removeSelf(podCopy)
	return updatePod(podCopy, ic.kubeClient)
}

func (ic *SparkPodInitializer) enqueue(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		glog.Errorf("failed to get key for %v: %v", obj, err)
		return
	}

	ic.queue.AddRateLimited(key)
}

func (ic *SparkPodInitializer) dequeue(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		glog.Errorf("failed to get key for %v: %v", obj, err)
		return
	}

	ic.queue.Forget(key)
	ic.queue.Done(key)
}

// isInitializerPresent returns if the list of pending Initializer of the given pod
// contains an instance of this Initializer.
func isInitializerPresent(pod *apiv1.Pod) bool {
	if pod.Initializers == nil {
		return false
	}

	for _, pending := range pod.Initializers.Pending {
		if pending.Name == sparkPodInitializerName {
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

func addOwnerReference(pod *apiv1.Pod) error {
	ownerReferenceStr, ok := pod.Annotations[config.OwnerReferenceAnnotation]
	if ok {
		ownerReference, err := util.UnmarshalOwnerReference(ownerReferenceStr)
		if err != nil {
			return err
		}
		pod.ObjectMeta.OwnerReferences = append(pod.ObjectMeta.OwnerReferences, *ownerReference)
	}
	return nil
}

func handleConfigMaps(pod *apiv1.Pod, container *apiv1.Container) {
	sparkConfigMapName, ok := pod.Annotations[config.SparkConfigMapAnnotation]
	if ok {
		glog.Infof("Mounting Spark ConfigMap %s to pod %s", sparkConfigMapName, pod.Name)
		config.AddSparkConfigMapVolumeToPod(sparkConfigMapName, pod)
		config.MountSparkConfigMapToContainer(container)
	}

	hadoopConfigMapName, ok := pod.Annotations[config.HadoopConfigMapAnnotation]
	if ok {
		glog.Infof("Mounting Hadoop ConfigMap %s to pod %s", hadoopConfigMapName, pod.Name)
		config.AddHadoopConfigMapVolumeToPod(hadoopConfigMapName, pod)
		config.MountHadoopConfigMapToContainer(container)
	}

	configMaps := config.FindGeneralConfigMaps(pod.Annotations)
	for name, mountPath := range configMaps {
		glog.Infof("Mounting ConfigMap %s to pod %s", name, pod.Name)
		volumeName := name + "-volume"
		config.AddConfigMapVolumeToPod(volumeName, name, pod)
		config.MountConfigMapToContainer(volumeName, mountPath, container)
	}
}

func handleVolumes(pod *apiv1.Pod, container *apiv1.Container) error {
	volumes, err := config.FindVolumes(pod.Annotations)
	if err != nil {
		return err
	}
	volumeMounts, err := config.FindVolumeMounts(pod.Annotations)
	if err != nil {
		return err
	}
	for name := range volumeMounts {
		if volume, ok := volumes[name]; ok {
			config.AddVolumeToPod(volume, pod)
			config.MountVolumeToContainer(volumeMounts[name], container)
		}
	}
	return nil
}

// removeSelf removes the initializer from the list of pending initializers of the given Pod.
func removeSelf(pod *apiv1.Pod) {
	if pod.Initializers == nil {
		return
	}

	var updated []metav1.Initializer
	for _, pending := range pod.Initializers.Pending {
		if pending.Name != sparkPodInitializerName {
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
}

func updatePod(newPod *apiv1.Pod, clientset clientset.Interface) error {
	glog.Infof("Updating pod %s", newPod.Name)
	_, err := clientset.CoreV1().Pods(newPod.Namespace).Update(newPod)
	if err != nil {
		return err
	}
	return nil
}

func patchPod(originalPod, modifiedPod *apiv1.Pod, clientset clientset.Interface) (*apiv1.Pod, error) {
	originalData, err := json.Marshal(originalPod)
	if err != nil {
		return nil, err
	}
	modifiedData, err := json.Marshal(modifiedPod)
	if err != nil {
		return nil, err
	}

	patch, err := strategicpatch.CreateTwoWayMergePatch(originalData, modifiedData, apiv1.Pod{})
	if err != nil {
		return nil, err
	}

	return clientset.CoreV1().Pods(originalPod.Namespace).Patch(
		originalPod.Name,
		types.StrategicMergePatchType,
		patch)
}
