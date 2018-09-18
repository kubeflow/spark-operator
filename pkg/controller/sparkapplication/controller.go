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
	"os/exec"
	"reflect"
	"time"

	"github.com/golang/glog"

	"os"
	"path/filepath"

	"strings"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/kubernetes/typed/extensions/v1beta1"
	"k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	crdclientset "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
	crdscheme "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned/scheme"
	crdinformers "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/informers/externalversions"
	crdlisters "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/listers/sparkoperator.k8s.io/v1alpha1"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/util"
)

const (
	sparkDriverRole           = "driver"
	sparkExecutorRole         = "executor"
	sparkExecutorIDLabel      = "spark-exec-id"
	podAlreadyExistsErrorCode = "code=409"
)

var (
	keyFunc     = cache.DeletionHandlingMetaNamespaceKeyFunc
	execCommand = exec.Command
)

// Controller manages instances of SparkApplication.
type Controller struct {
	crdClient         crdclientset.Interface
	kubeClient        clientset.Interface
	extensionsClient  v1beta1.ExtensionsV1beta1Interface
	queue             workqueue.RateLimitingInterface
	cacheSynced       cache.InformerSynced
	recorder          record.EventRecorder
	metrics           *sparkAppMetrics
	applicationLister crdlisters.SparkApplicationLister
	podLister         v1.PodLister
	ingressUrlFormat  string
}

// NewController creates a new Controller.
func NewController(
	crdClient crdclientset.Interface,
	kubeClient clientset.Interface,
	extensionsClient v1beta1.ExtensionsV1beta1Interface,
	crdInformerFactory crdinformers.SharedInformerFactory,
	podInformerFactory informers.SharedInformerFactory,
	metricsConfig *util.MetricConfig,
	namespace string,
	ingressUrlFormat string) *Controller {
	crdscheme.AddToScheme(scheme.Scheme)

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.V(2).Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{
		Interface: kubeClient.CoreV1().Events(namespace),
	})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, apiv1.EventSource{Component: "spark-operator"})

	return newSparkApplicationController(crdClient, kubeClient, extensionsClient, crdInformerFactory, podInformerFactory, recorder, metricsConfig, ingressUrlFormat)
}

func newSparkApplicationController(
	crdClient crdclientset.Interface,
	kubeClient clientset.Interface,
	extensionsClient v1beta1.ExtensionsV1beta1Interface,
	crdInformerFactory crdinformers.SharedInformerFactory,
	podInformerFactory informers.SharedInformerFactory,
	eventRecorder record.EventRecorder,
	metricsConfig *util.MetricConfig,
	ingressUrlFormat string) *Controller {
	queue := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(),
		"spark-application-controller")

	controller := &Controller{
		crdClient:        crdClient,
		kubeClient:       kubeClient,
		extensionsClient: extensionsClient,
		recorder:         eventRecorder,
		queue:            queue,
		ingressUrlFormat: ingressUrlFormat,
	}

	if metricsConfig != nil {
		controller.metrics = newSparkAppMetrics(metricsConfig.MetricsPrefix, metricsConfig.MetricsLabels)
		controller.metrics.registerMetrics()
	}

	crdInformer := crdInformerFactory.Sparkoperator().V1alpha1().SparkApplications()
	crdInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.onAdd,
		UpdateFunc: controller.onUpdate,
		DeleteFunc: controller.onDelete,
	})
	controller.applicationLister = crdInformer.Lister()

	podsInformer := podInformerFactory.Core().V1().Pods()
	sparkPodEventHandler := newSparkPodEventHandler(controller.queue.AddRateLimited)
	podsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sparkPodEventHandler.onPodAdded,
		UpdateFunc: sparkPodEventHandler.onPodUpdated,
		DeleteFunc: sparkPodEventHandler.onPodDeleted,
	})
	controller.podLister = podsInformer.Lister()

	controller.cacheSynced = func() bool {
		return crdInformer.Informer().HasSynced() && podsInformer.Informer().HasSynced()
	}
	return controller
}

// Start starts the Controller by registering a watcher for SparkApplication objects.
func (c *Controller) Start(workers int, stopCh <-chan struct{}) error {
	glog.Info("Starting the workers of the SparkApplication controller")
	for i := 0; i < workers; i++ {
		// runWorker will loop until "something bad" happens. Until will then rekick
		// the worker after one second.
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	// Wait for all involved caches to be synced, before processing items from the queue is started.
	if !cache.WaitForCacheSync(stopCh, c.cacheSynced) {
		return fmt.Errorf("timed out waiting for cache to sync")
	}
	return nil
}

// Stop stops the controller.
func (c *Controller) Stop() {
	glog.Info("Stopping the SparkApplication controller")
	c.queue.ShutDown()
}

// Callback function called when a new SparkApplication object gets created.
func (c *Controller) onAdd(obj interface{}) {
	app := obj.(*v1alpha1.SparkApplication)

	glog.Infof("SparkApplication %s was added, enqueueing it for submission", app.Name)
	c.enqueue(app)
	c.recorder.Eventf(
		app,
		apiv1.EventTypeNormal,
		"SparkApplicationAdded",
		"SparkApplication %s was added, enqueued it for submission",
		app.Name)
}

func (c *Controller) onUpdate(oldObj, newObj interface{}) {
	oldApp := oldObj.(*v1alpha1.SparkApplication)
	newApp := newObj.(*v1alpha1.SparkApplication)

	// The spec has changed. This is currently not supported as we can potentially miss this update
	// and end up in an inconsistent state.
	if !reflect.DeepEqual(oldApp.Spec, newApp.Spec) {
		glog.Warningf("Spark Application update is not supported. Please delete and re-create the SparkApplication %s for the new specification to have effect", oldApp.GetName())
		c.recorder.Eventf(
			newApp,
			apiv1.EventTypeWarning,
			"SparkApplicationUpdateFailed",
			"Spark Application update is not supported. Please delete and re-create the SparkApplication %s for the new Specification to have effect.",
			newApp.Name)
		return
	}

	if !newApp.GetObjectMeta().GetDeletionTimestamp().IsZero() {
		// CRD deletion requested, lets delete driver and UI.
		if err := c.deleteDriver(newApp, true); err != nil {
			glog.Errorf("failed to delete the driver pod and UI service for deleted SparkApplication %s: %v",
				newApp.Name, err)
			return
		}
		// Successfully deleted driver. Remove it from the Finalizer List.
		for k, elem := range newApp.Finalizers {
			if elem == sparkDriverRole {
				newApp.Finalizers = append(newApp.Finalizers[:k], newApp.Finalizers[k+1:]...)
				break
			}
		}
		if err := c.updateApp(newApp); err != nil {
			glog.Errorf("Failed to update App %s. Error:%v", newApp.GetName(), err)
		}
		return
	}

	glog.V(2).Infof("Spark Application %s enqueued for Processing.", newApp.GetName())
	c.enqueue(newApp)
}

func (c *Controller) onDelete(obj interface{}) {

	c.dequeue(obj)

	var app *v1alpha1.SparkApplication
	switch obj.(type) {
	case *v1alpha1.SparkApplication:
		app = obj.(*v1alpha1.SparkApplication)
	case cache.DeletedFinalStateUnknown:
		deletedObj := obj.(cache.DeletedFinalStateUnknown).Obj
		app = deletedObj.(*v1alpha1.SparkApplication)
	}

	if app != nil {
		c.recorder.Eventf(
			app,
			apiv1.EventTypeNormal,
			"SparkApplicationDeleted",
			"SparkApplication %s was deleted",
			app.Name)
	}
}

// runWorker runs a single controller worker.
func (c *Controller) runWorker() {
	defer utilruntime.HandleCrash()
	for c.processNextItem() {
	}
}

func (c *Controller) processNextItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	glog.V(2).Infof("Starting processing key: %v", key)
	defer glog.V(2).Infof("Ending processing key: %v", key)
	err := c.syncSparkApplication(key.(string))
	if err == nil {
		// Successfully processed the key or the key was not found so tell the queue to stop tracking
		// history for your key. This will reset things like failure counts for per-item rate limiting.
		c.queue.Forget(key)
		return true
	}

	// There was a failure so be sure to report it. This method allows for pluggable error handling
	// which can be used for things like cluster-monitoring
	utilruntime.HandleError(fmt.Errorf("failed to sync SparkApplication %q: %v", key, err))
	return true
}

// Helper Struct to encapsulate current State of the driver pod.
type driverState struct {
	podName            string         // Name of the driver pod.
	sparkApplicationID string         // sparkApplicationID.
	nodeName           string         // Name of the node the driver pod runs on.
	podPhase           apiv1.PodPhase // Driver pod phase.
	completionTime     metav1.Time    // Time the driver completes.
}

func (c *Controller) getUpdatedAppStatus(app *v1alpha1.SparkApplication) *v1alpha1.SparkApplication {

	// Fetch all the pods for the App.
	selector, err := labels.NewRequirement(config.SparkAppNameLabel, selection.Equals, []string{app.Name})
	pods, err := c.podLister.List(labels.NewSelector().Add(*selector))

	if err != nil {
		glog.Errorf("Error while fetching pods for %v in namespace: %v. Error: %v", app.Name, app.Namespace, err)
		return nil
	}
	executorStateMap := map[string]v1alpha1.ExecutorState{}
	var currentDriverState *driverState

	for _, pod := range pods {
		if isDriverPod(pod) {
			currentDriverState = &driverState{
				podName:            pod.Name,
				nodeName:           pod.Spec.NodeName,
				podPhase:           pod.Status.Phase,
				sparkApplicationID: getSparkApplicationID(pod),
			}
			if pod.Status.Phase == apiv1.PodSucceeded || pod.Status.Phase == apiv1.PodFailed {
				currentDriverState.completionTime = metav1.Now()
			}
			c.recordDriverEvent(app, pod.Status.Phase, pod.Name)
		}
		if isExecutorPod(pod) {
			c.recordExecutorEvent(app, podPhaseToExecutorState(pod.Status.Phase), pod.Name)
			executorStateMap[pod.Name] = podPhaseToExecutorState(pod.Status.Phase)
		}
	}

	if currentDriverState != nil {
		if newAppState, err := driverPodPhaseToApplicationState(currentDriverState.podPhase); err == nil {
			// Valid Driver State: Update CRD.
			if currentDriverState.podName != "" {
				app.Status.DriverInfo.PodName = currentDriverState.podName
			}
			app.Status.SparkApplicationID = currentDriverState.sparkApplicationID
			if currentDriverState.nodeName != "" {
				if nodeIP := c.getNodeExternalIP(currentDriverState.nodeName); nodeIP != "" {
					app.Status.DriverInfo.WebUIAddress = fmt.Sprintf("%s:%d", nodeIP,
						app.Status.DriverInfo.WebUIPort)
				}
			}
			app.Status.AppState.State = newAppState
			if app.Status.CompletionTime.IsZero() && !currentDriverState.completionTime.IsZero() {
				app.Status.CompletionTime = currentDriverState.completionTime
			}

			if isAppTerminated(newAppState) {
				c.recorder.Eventf(
					app,
					apiv1.EventTypeNormal,
					"SparkApplicationTerminated",
					"SparkApplication %s terminated with state: %v",
					currentDriverState.podName,
					newAppState)
			}
		} else {
			glog.Warningf("Invalid Driver State: " + err.Error())
		}
	} else {
		glog.Warningf("Driver not found  for app %s:%s", app.GetNamespace(), app.GetName())
	}

	// Apply Executor Updates
	if app.Status.ExecutorState == nil {
		app.Status.ExecutorState = make(map[string]v1alpha1.ExecutorState)
	}

	for name, execStatus := range executorStateMap {
		app.Status.ExecutorState[name] = execStatus
	}

	// Handle missing/deleted executors.
	for name, oldStatus := range app.Status.ExecutorState {
		_, exists := executorStateMap[name]
		if !isExecutorTerminated(oldStatus) && !exists {

			glog.Infof("Executor Pod not found. Assuming pod was deleted %s", name)
			app.Status.ExecutorState[name] = v1alpha1.ExecutorFailedState

		}
	}

	return app
}

func (c *Controller) syncSparkApplication(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return fmt.Errorf("failed to get the namespace and name from key %s: %v", key, err)
	}
	app, err := c.getSparkApplication(namespace, name)
	if err != nil {
		return err
	}

	appToUpdate := app.DeepCopy()
	var updatedApp *v1alpha1.SparkApplication

	switch app.Status.AppState.State {
	case v1alpha1.NewState:
		appToUpdate.Status.SubmissionAttempts = 0
		glog.Infof("Creating Submission for SparkApp: %s", key)
		updatedApp = c.submitSparkApplication(appToUpdate)
	case v1alpha1.CompletedState:
		if app.Spec.RestartPolicy.Type == v1alpha1.Always {
			// Reset SubmissionAttempts Count since this is a new overall retry.
			appToUpdate.Status.SubmissionAttempts = 0
			glog.Infof("Creating Submission for SparkApp: %s", key)
			updatedApp = c.submitSparkApplication(appToUpdate)
		}
	case v1alpha1.FailedState:
		shouldRetry := false
		if app.Spec.RestartPolicy.Type == v1alpha1.Always {
			shouldRetry = true
		} else if app.Spec.RestartPolicy.Type == v1alpha1.OnFailure {
			// We Retry if we haven't hit the retry limit.
			if app.Spec.RestartPolicy.OnFailureRetries != nil && app.Status.Attempts < (*app.Spec.RestartPolicy.OnFailureRetries)+1 {
				shouldRetry = true
			}
		}
		if shouldRetry && hasRetryIntervalPassed(app.Spec.RestartPolicy.OnFailureRetryInterval, app.Status.Attempts, app.Status.CompletionTime) {
			{
				// Reset SubmissionAttempts Count since this is a new overall retry.
				appToUpdate.Status.SubmissionAttempts = 0
				glog.Infof("Creating Submission for SparkApp: %s", key)
				updatedApp = c.submitSparkApplication(appToUpdate)
			}
		}
	case v1alpha1.FailedSubmissionState:
		shouldRetry := false
		if app.Spec.RestartPolicy.Type == v1alpha1.Always {
			shouldRetry = true
		} else if app.Spec.RestartPolicy.Type == v1alpha1.OnFailure {
			// We Retry if we haven't hit the retry limit.
			if app.Spec.RestartPolicy.OnSubmissionFailureRetries != nil && app.Status.SubmissionAttempts < (*app.Spec.RestartPolicy.OnSubmissionFailureRetries)+1 {
				shouldRetry = true
			}
		}
		if shouldRetry && hasRetryIntervalPassed(app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval, app.Status.SubmissionAttempts, app.Status.SubmissionTime) {
			glog.Infof("Creating Submission for SparkApp: %s", key)
			updatedApp = c.submitSparkApplication(appToUpdate)
		}
	case v1alpha1.SubmittedState, v1alpha1.RunningState:
		//App already submitted, get driver and executor pods and update Status.
		updatedApp = c.getUpdatedAppStatus(appToUpdate)
	}

	// Update CRD if not nil.
	if updatedApp != nil {
		glog.V(2).Infof("Trying to update App %s, from: [%v] to :[%v]", app.Name, app.Status, updatedApp.Status)
		if c.updateAppAndExportMetrics(app, updatedApp) != nil {
			glog.Errorf("Failed to update App: %s. Error: %v", app.GetName(), err)
			return err
		}
	}
	return nil
}

// Helper func to determine if we have waited enough to retry the SparkApplication.
func hasRetryIntervalPassed(retryInterval *int64, attemptsDone int32, lastEventTime metav1.Time) bool {

	if retryInterval == nil || lastEventTime.IsZero() || attemptsDone <= 0 {
		glog.Errorf("Invalid Parameters passed: retryInterval %v, attemptsDone: %v, lastEventTime: %v", retryInterval, attemptsDone, lastEventTime)
		return false
	}

	// Retry if we have waited at-least equal to attempts*RetryInterval since we do a linear back-off.
	interval := time.Duration(*retryInterval) * time.Second * time.Duration(attemptsDone)
	currentTime := time.Now()
	if currentTime.After(lastEventTime.Add(interval)) {
		return true
	}
	return false
}

// submitSparkApplication creates a new submission for the given SparkApplication and submits it using spark-submit.
func (c *Controller) submitSparkApplication(app *v1alpha1.SparkApplication) *v1alpha1.SparkApplication {

	submissionCmdArgs, err := buildSubmissionCommandArgs(app)
	if err != nil {
		app.Status = v1alpha1.SparkApplicationStatus{
			AppState: v1alpha1.ApplicationState{
				State:        v1alpha1.FailedSubmissionState,
				ErrorMessage: err.Error(),
			},
			SubmissionAttempts: app.Status.SubmissionAttempts + 1,
			SubmissionTime:     metav1.Now(),
		}
		return app
	}

	// Try submitting App.
	submission := newSubmission(submissionCmdArgs, app)
	sparkHome, present := os.LookupEnv(sparkHomeEnvVar)
	if !present {
		glog.Error("SPARK_HOME is not specified")
	}
	var command = filepath.Join(sparkHome, "/bin/spark-submit")

	cmd := execCommand(command, submission.args...)
	glog.Infof("spark-submit arguments: %v", cmd.Args)

	if _, err := cmd.Output(); err != nil {
		var errorMsg string
		if exitErr, ok := err.(*exec.ExitError); ok {
			errorMsg = string(exitErr.Stderr)
		}
		// Already Exists. Do nothing.
		if strings.Contains(errorMsg, podAlreadyExistsErrorCode) {
			glog.Warningf("Trying to resubmit an already submitted SparkApplication %s in namespace %s. Error: %s", submission.name, submission.namespace, errorMsg)
			return nil
		}
		glog.Errorf("failed to run spark-submit for SparkApplication %s in namespace: %s. Error: %s", submission.name,
			submission.namespace, errorMsg)
		app.Status = v1alpha1.SparkApplicationStatus{
			AppState: v1alpha1.ApplicationState{
				State:        v1alpha1.FailedSubmissionState,
				ErrorMessage: errorMsg,
			},
			SubmissionAttempts: app.Status.SubmissionAttempts + 1,
			SubmissionTime:     metav1.Now(),
		}
		c.recorder.Eventf(
			app,
			apiv1.EventTypeWarning,
			"SparkApplicationSubmissionFailed",
			"failed to create a submission for SparkApplication %s: %s",
			app.Name,
			errorMsg)
	} else {
		glog.Infof("spark-submit completed for SparkApplication %s in namespace %s", submission.name, submission.namespace)
		app.Status = v1alpha1.SparkApplicationStatus{
			AppState: v1alpha1.ApplicationState{
				State: v1alpha1.SubmittedState,
			},
			SubmissionAttempts: app.Status.SubmissionAttempts + 1,
			Attempts:           app.Status.Attempts + 1,
			SubmissionTime:     metav1.Now(),
		}
		// Add driver as a finalizer to prevent SparkApplication deletion till driver is deleted.
		if app.ObjectMeta.Finalizers == nil {
			app.ObjectMeta.Finalizers = []string{sparkDriverRole}
		} else {
			app.ObjectMeta.Finalizers = append(app.ObjectMeta.Finalizers, sparkDriverRole)
		}

		if app.Spec.Monitoring != nil && app.Spec.Monitoring.Prometheus != nil {
			// configPrometheusMonitoring may update app.Spec.
			configPrometheusMonitoring(app, c.kubeClient)
		}

		// Create Spark UI Service.
		service, err := createSparkUIService(app, c.kubeClient)
		if err != nil {
			glog.Errorf("Failed to create UI service for SparkApplication %s: %v", app.Name, err)
		} else {
			app.Status.DriverInfo.WebUIServiceName = service.serviceName
			app.Status.DriverInfo.WebUIPort = service.nodePort

			// Create UI Ingress if ingress-format is set.
			if c.ingressUrlFormat != "" {
				ingress, err := createSparkUIIngress(app, *service, c.ingressUrlFormat, c.extensionsClient)
				if err != nil {
					glog.Errorf("Failed to create UI Ingress for SparkApplication %s: %v", app.Name, err)
				} else {
					app.Status.DriverInfo.WebUIIngressAddress = ingress.ingressUrl
					app.Status.DriverInfo.WebUIIngressName = ingress.ingressName
				}
			}
		}
	}
	return app
}

func (c *Controller) updateAppAndExportMetrics(oldApp, newApp *v1alpha1.SparkApplication) error {

	// Skip update if nothing changed.
	if reflect.DeepEqual(oldApp, newApp) {
		return nil
	}

	app, err := c.crdClient.SparkoperatorV1alpha1().SparkApplications(newApp.Namespace).Update(newApp)
	// Export metrics if the update was successful.
	if err == nil && c.metrics != nil {
		c.metrics.exportMetrics(oldApp, app)
	}
	return err
}

func (c *Controller) updateApp(toUpdate *v1alpha1.SparkApplication) error {
	_, err := c.crdClient.SparkoperatorV1alpha1().SparkApplications(toUpdate.Namespace).Update(toUpdate)
	return err
}

func (c *Controller) getSparkApplication(namespace string, name string) (*v1alpha1.SparkApplication, error) {
	return c.applicationLister.SparkApplications(namespace).Get(name)
}

func (c *Controller) deleteDriver(app *v1alpha1.SparkApplication, waitForDriverDeletion bool) error {
	if app.Status.DriverInfo.PodName != "" {
		err := c.kubeClient.CoreV1().Pods(app.Namespace).Delete(app.Status.DriverInfo.PodName,
			metav1.NewDeleteOptions(0))
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	if waitForDriverDeletion {
		wait.Poll(500*time.Millisecond, 60*time.Second, func() (bool, error) {
			_, err := c.kubeClient.CoreV1().Pods(app.Namespace).Get(app.Status.DriverInfo.PodName, metav1.GetOptions{})
			if err != nil {
				if errors.IsNotFound(err) {
					return true, nil
				}
				return false, err
			}
			return false, nil
		})
	}
	return nil
}

func (c *Controller) enqueue(obj interface{}) {
	key, err := keyFunc(obj)
	if err != nil {
		glog.Errorf("failed to get key for %v: %v", obj, err)
		return
	}

	c.queue.AddRateLimited(key)
}

func (c *Controller) dequeue(obj interface{}) {
	key, err := keyFunc(obj)
	if err != nil {
		glog.Errorf("failed to get key for %v: %v", obj, err)
		return
	}

	c.queue.Forget(key)
	c.queue.Done(key)
}

func (c *Controller) getNodeExternalIP(nodeName string) string {
	node, err := c.kubeClient.CoreV1().Nodes().Get(nodeName, metav1.GetOptions{})
	if err != nil {
		glog.Errorf("failed to get node %s", nodeName)
		return ""
	}

	for _, address := range node.Status.Addresses {
		if address.Type == apiv1.NodeExternalIP {
			return address.Address
		}
	}
	return ""
}

func (c *Controller) recordDriverEvent(
	app *v1alpha1.SparkApplication, phase apiv1.PodPhase, name string) {

	switch phase {
	case apiv1.PodSucceeded:
		c.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkDriverCompleted", "Driver %s completed", name)
	case apiv1.PodPending:
		c.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkDriverPending", "Driver %s is pending", name)
	case apiv1.PodRunning:
		c.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkDriverRunning", "Driver %s is running", name)
	case apiv1.PodFailed:
		c.recorder.Eventf(app, apiv1.EventTypeWarning, "SparkDriverFailed", "Driver %s failed", name)
	case apiv1.PodUnknown:
		c.recorder.Eventf(app, apiv1.EventTypeWarning, "SparkDriverUnknownState", "Driver %s in unknown state", name)
	}
}

func (c *Controller) recordExecutorEvent(
	app *v1alpha1.SparkApplication, state v1alpha1.ExecutorState, name string) {

	switch state {
	case v1alpha1.ExecutorCompletedState:
		c.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkExecutorCompleted", "Executor %s completed", name)
	case v1alpha1.ExecutorPendingState:
		c.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkExecutorPending", "Executor %s is pending", name)
	case v1alpha1.ExecutorRunningState:
		c.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkExecutorRunning", "Executor %s is running", name)
	case v1alpha1.ExecutorFailedState:
		c.recorder.Eventf(app, apiv1.EventTypeWarning, "SparkExecutorFailed", "Executor %s failed", name)
	case v1alpha1.ExecutorUnknownState:
		c.recorder.Eventf(app, apiv1.EventTypeWarning, "SparkExecutorUnknownState", "Executor %s in unknown state", name)
	}
}
