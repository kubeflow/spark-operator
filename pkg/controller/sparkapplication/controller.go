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
	"strconv"
	"time"

	"github.com/golang/glog"

	"os"
	"path/filepath"

	apiv1 "k8s.io/api/core/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	crdclientset "k8s.io/spark-on-k8s-operator/pkg/client/clientset/versioned"
	crdscheme "k8s.io/spark-on-k8s-operator/pkg/client/clientset/versioned/scheme"
	crdinformers "k8s.io/spark-on-k8s-operator/pkg/client/informers/externalversions"
	"k8s.io/spark-on-k8s-operator/pkg/util"
)

const (
	sparkRoleLabel       = "spark-role"
	sparkDriverRole      = "driver"
	sparkExecutorRole    = "executor"
	sparkExecutorIDLabel = "spark-exec-id"
	maximumUpdateRetries = 3
)

var (
	keyFunc     = cache.DeletionHandlingMetaNamespaceKeyFunc
	execCommand = exec.Command
)

// Controller manages instances of SparkApplication.
type Controller struct {
	crdClient        crdclientset.Interface
	kubeClient       clientset.Interface
	extensionsClient apiextensionsclient.Interface
	queue            workqueue.RateLimitingInterface
	cacheSynced      cache.InformerSynced
	recorder         record.EventRecorder
	sparkPodMonitor  *sparkPodMonitor
	metrics          *sparkAppMetrics
}

// NewController creates a new Controller.
func NewController(
	crdClient crdclientset.Interface,
	kubeClient clientset.Interface,
	extensionsClient apiextensionsclient.Interface,
	informerFactory crdinformers.SharedInformerFactory,
	metricsConfig *util.MetricConfig,
	namespace string) *Controller {
	crdscheme.AddToScheme(scheme.Scheme)

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.V(2).Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{
		Interface: kubeClient.CoreV1().Events(namespace),
	})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, apiv1.EventSource{Component: "spark-operator"})

	return newSparkApplicationController(crdClient, kubeClient, extensionsClient, informerFactory, recorder, metricsConfig, namespace)
}

func newSparkApplicationController(
	crdClient crdclientset.Interface,
	kubeClient clientset.Interface,
	extensionsClient apiextensionsclient.Interface,
	informerFactory crdinformers.SharedInformerFactory,
	eventRecorder record.EventRecorder,
	metricsConfig *util.MetricConfig,
	namespace string) *Controller {
	queue := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(),
		"spark-application-controller")
	sparkPodMonitor := newSparkPodMonitor(kubeClient, namespace, queue)

	controller := &Controller{
		crdClient:        crdClient,
		kubeClient:       kubeClient,
		extensionsClient: extensionsClient,
		recorder:         eventRecorder,
		queue:            queue,
		sparkPodMonitor:  sparkPodMonitor,
	}

	if metricsConfig != nil {
		controller.metrics = newSparkAppMetrics(metricsConfig.MetricsPrefix, metricsConfig.MetricsLabels)
		controller.metrics.registerMetrics()
	}

	informer := informerFactory.Sparkoperator().V1alpha1().SparkApplications()
	informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.onAdd,
		UpdateFunc: controller.onUpdate,
		DeleteFunc: controller.onDelete,
	})
	controller.cacheSynced = informer.Informer().HasSynced
	return controller
}

// Start starts the Controller by registering a watcher for SparkApplication objects.
func (c *Controller) Start(workers int, stopCh <-chan struct{}) error {
	glog.Info("Starting the SparkApplication controller")

	if !cache.WaitForCacheSync(stopCh, c.cacheSynced) {
		return fmt.Errorf("timed out waiting for cache to sync")
	}

	glog.Info("Starting the workers of the SparkApplication controller")
	for i := 0; i < workers; i++ {
		// runWorker will loop until "something bad" happens. Until will then rekick
		// the worker after one second.
		go wait.Until(c.runWorker, time.Second, stopCh)
	}
	go c.sparkPodMonitor.run(stopCh)
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
	if shouldSubmit(app) {
		glog.Infof("SparkApplication %s was added, enqueueing it for submission", app.Name)
		c.enqueue(app)
		c.recorder.Eventf(
			app,
			apiv1.EventTypeNormal,
			"SparkApplicationAdded",
			"SparkApplication %s was added, enqueued it for submission",
			app.Name)
	}
}

func (c *Controller) onUpdate(oldObj, newObj interface{}) {
	oldApp := oldObj.(*v1alpha1.SparkApplication)
	newApp := newObj.(*v1alpha1.SparkApplication)

	// The resource version has not changed, nothing to do.
	if oldApp.ResourceVersion == newApp.ResourceVersion {
		return
	}

	if reflect.DeepEqual(oldApp.Spec, newApp.Spec) {
		// The spec has not changed but the application is subject to restart if the application state has changed.
		// If the application state remains the same, it doesn't make sense to even check the restart eligibility.
		if oldApp.Status.AppState.State != newApp.Status.AppState.State && shouldRestart(newApp) {
			c.handleRestart(newApp)
		}
		return
	}

	if oldApp.Status.DriverInfo.PodName != "" {
		// Clear the application ID if the driver pod of the old application is to be deleted. This is important as
		// otherwise deleting the driver pod of the old application if it's still running will result in a driver state
		// update by the sparkPodMonitor with the driver pod phase set to PodFailed. This may lead to a restart of the
		// application if it'c subject to a restart because of the failure state update of the driver pod. Clearing the
		// application ID causes the state update regarding the old driver pod to be ignored in
		// processSingleDriverStateUpdate because of mismatched application IDs and as a consequence no restart to be
		// triggered. This prevents the application to be submitted twice: one from the restart and one from below.
		c.updateSparkApplicationStatusWithRetries(newApp, func(status *v1alpha1.SparkApplicationStatus) {
			status.AppID = ""
		})
	}

	// Delete the driver pod and UI service of the old application. Note that deleting the driver pod kills the
	// application if it is still running. Skip submitting the new application if cleanup for the old application
	// failed to avoid potentially running both the old and new applications at the same time.
	if err := c.deleteDriverAndUIService(oldApp, true); err != nil {
		glog.Errorf("failed to delete the old driver pod and UI service for SparkApplication %s: %v",
			oldApp.Name, err)
		return
	}

	glog.Infof("SparkApplication %s was updated, enqueueing it for submission", newApp.Name)
	c.enqueue(newApp)
	c.recorder.Eventf(
		newApp,
		apiv1.EventTypeNormal,
		"SparkApplicationUpdated",
		"SparkApplication %s was updated and enqueued for submission",
		newApp.Name)
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

		if err := c.deleteDriverAndUIService(app, false); err != nil {
			glog.Errorf("failed to delete the driver pod and UI service for deleted SparkApplication %s: %v",
				app.Name, err)
		}
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
	// Since we failed, we should requeue the item to work on later.  This method will add a backoff
	// to avoid hot-looping on particular items (they're probably still not going to work right away)
	// and overall controller protection (everything I've done is broken, this controller needs to
	// calm down or it can starve other useful work) cases.
	c.queue.AddRateLimited(key)

	return true
}

// driverStateUpdate encapsulates state update of the driver.
type driverStateUpdate struct {
	podName        string         // Name of the driver pod.
	nodeName       string         // Name of the node the driver pod runs on.
	podPhase       apiv1.PodPhase // Driver pod phase.
	completionTime metav1.Time    // Time the driver completes.
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

	switch app.Status.AppState.State {
	case "":
		fallthrough
	case v1alpha1.NewState:
		glog.V(2).Infof("Creating Submission for SparkApp: %s", key)
		err = c.submitSparkApplication(app, 0)
		if err != nil {
			c.recorder.Eventf(
				app,
				apiv1.EventTypeWarning,
				"SparkApplicationSubmissionCreationFailed",
				"failed to create a submission for SparkApplication %s: %s",
				app.Name,
				err.Error())
			return err
		}
	case v1alpha1.FailedSubmissionState:
		retryAttempt := app.Status.SubmissionRetries + 1
		glog.V(2).Infof("Retrying Spark App: %s. RetryAttempt: %d", key, retryAttempt)
		err = c.submitSparkApplication(app, retryAttempt)
		if err != nil {
			c.recorder.Eventf(
				app,
				apiv1.EventTypeWarning,
				"SparkApplicationSubmissionCreationFailed",
				"failed to create a submission for SparkApplication %s: %s",
				app.Name,
				err.Error())
			return err
		}
	default:
		//App already submitted, get driver and executor pods and update Status
		selector := fmt.Sprintf("sparkoperator.k8s.io/app-name=%v", name)
		listOptions := metav1.ListOptions{LabelSelector: selector}

		pods, err := c.kubeClient.CoreV1().Pods(namespace).List(listOptions)

		if err != nil {
			glog.Errorf("Error while fetching pods for %v in namespace: %v. Error: %v", name, namespace, err)
			return err
		}

		executorStateUpdates := map[string]v1alpha1.ExecutorState{}
		var driverStateUpdates *driverStateUpdate

		for _, pod := range pods.Items {

			// The controller may receive status updates of executors belonging to the previous run of the application and
			// should ignore such updates. For example, if a user makes some change to a SparkApplication and does 'kube apply'
			// to update the SparkApplication object on the API server while the application is running, the controller may
			// still receive status updates of executors of the old application run even after the new run starts.
			if getAppID(&pod) != app.Status.AppID {
				continue
			}

			if isDriverPod(&pod) {
				driverStateUpdates = &driverStateUpdate{
					podName:  pod.Name,
					nodeName: pod.Spec.NodeName,
					podPhase: pod.Status.Phase,
				}
				if pod.Status.Phase == apiv1.PodSucceeded || pod.Status.Phase == apiv1.PodFailed {
					driverStateUpdates.completionTime = metav1.Now()
				}

				c.recordDriverEvent(app, pod.Status.Phase, pod.Name)
				newAppState := driverPodPhaseToApplicationState(pod.Status.Phase)
				if isAppTerminated(newAppState) {
					c.recorder.Eventf(
						app,
						apiv1.EventTypeNormal,
						"SparkApplicationTerminated",
						"SparkApplication %s terminated with state: %v",
						pod.Name,
						newAppState)
				}
			}

			if isExecutorPod(&pod) {
				c.recordExecutorEvent(app, podPhaseToExecutorState(pod.Status.Phase), pod.Name)
				executorStateUpdates[pod.Name] = podPhaseToExecutorState(pod.Status.Phase)
			}
		}

		if driverStateUpdates == nil {
			glog.Warningf("Driver Pod not found. Assuming pod was deleted and app:%s failed", app.Name)
			driverStateUpdates = &driverStateUpdate{
				podPhase: apiv1.PodFailed,
			}
		}

		c.updateSparkApplicationStatusWithRetries(app, func(status *v1alpha1.SparkApplicationStatus) {

			glog.V(2).Infof("Trying to apply updates: Driver: %+v, Executor: %+v", driverStateUpdates, executorStateUpdates)

			newAppStatus := driverPodPhaseToApplicationState(driverStateUpdates.podPhase)
			if isValidDriverStateTransition(status.AppState.State, newAppStatus) {
				// Valid Driver Transition: Update state
				if driverStateUpdates.podName != "" {
					status.DriverInfo.PodName = driverStateUpdates.podName
				}

				if driverStateUpdates.nodeName != "" {
					if nodeIP := c.getNodeExternalIP(driverStateUpdates.nodeName); nodeIP != "" {
						status.DriverInfo.WebUIAddress = fmt.Sprintf("%s:%d", nodeIP,
							status.DriverInfo.WebUIPort)
					}
				}
				status.AppState.State = newAppStatus
				if status.CompletionTime.IsZero() && !driverStateUpdates.completionTime.IsZero() {
					status.CompletionTime = metav1.Now()
				}
			} else {
				glog.Warningf("Invalid App/Driver State Transition From: %v. To: %v found", status.AppState.State, newAppStatus)
			}

			// Apply Executor Updates
			if status.ExecutorState == nil {
				status.ExecutorState = make(map[string]v1alpha1.ExecutorState)
			}

			for name, execStatus := range executorStateUpdates {
				existingState, ok := status.ExecutorState[name]
				if ok && !isValidExecutorStateTransition(existingState, execStatus) {
					glog.Warningf("Invalid Executor State Transition From: %v. To: %v found", existingState, execStatus)
				} else {
					status.ExecutorState[name] = execStatus
				}
			}

			// Missing/deleted executors
			for name, oldStatus := range status.ExecutorState {
				_, exists := executorStateUpdates[name]
				if !isExecutorTerminated(oldStatus) && !exists {

					glog.Infof("Executor Pod not found. Assuming pod was deleted %s", name)
					status.ExecutorState[name] = v1alpha1.ExecutorFailedState

				}
			}
		})

	}
	return nil
}

// submitSparkApplication creates a new submission for the given SparkApplication and submits it using spark-submit.
func (c *Controller) submitSparkApplication(app *v1alpha1.SparkApplication, retryAttempt int32) error {

	appStatus := v1alpha1.SparkApplicationStatus{
		AppID: buildAppID(app),
		AppState: v1alpha1.ApplicationState{
			State: v1alpha1.NewState,
		},
	}
	// Let's make a copy and invoke buildSubmissionCommandArgs as it can potentially modify the App CRD
	appCopy := app.DeepCopy()
	appCopy.Status = appStatus
	submissionCmdArgs, err := buildSubmissionCommandArgs(appCopy)
	if err != nil {
		return fmt.Errorf(
			"failed to build the submission command for SparkApplication %s: %v",
			app.Name,
			err)
	}
	submission := newSubmission(submissionCmdArgs, appCopy)

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
		glog.Errorf("failed to run spark-submit for SparkApplication %s in namespace %s: %s", submission.name,
			submission.namespace, errorMsg)

		appStatus.AppState.State = v1alpha1.FailedSubmissionState
		appStatus.AppState.ErrorMessage = errorMsg
		appStatus.SubmissionRetries = retryAttempt
		appStatus.SubmissionTime = metav1.Now()
		c.recorder.Eventf(
			app,
			apiv1.EventTypeWarning,
			"SparkApplicationSubmissionFailed",
			"SparkApplication %s failed submission: %s",
			app.Name,
			appStatus.AppState.ErrorMessage)
	} else {
		glog.Infof("spark-submit completed for SparkApplication %s in namespace %s", submission.name, submission.namespace)
		appStatus.AppState.State = v1alpha1.SubmittedState
		appStatus.SubmissionTime = metav1.Now()
		appStatus.SubmissionRetries = retryAttempt

		// Create UI service on submit-success
		name, port, err := createSparkUIService(app, appStatus.AppID, c.kubeClient)
		if err != nil {
			glog.Errorf("failed to create a UI service for SparkApplication %s: %v", app.Name, err)
		} else {
			appStatus.DriverInfo.WebUIServiceName = name
			appStatus.DriverInfo.WebUIPort = port
		}
	}

	// Update CRD with status.
	c.updateSparkApplicationStatusWithRetries(app, func(status *v1alpha1.SparkApplicationStatus) {
		*status = v1alpha1.SparkApplicationStatus{}
		appStatus.DeepCopyInto(status)
	})

	if appStatus.AppState.State == v1alpha1.FailedSubmissionState {
		c.handleResubmission(app.GetNamespace(), app.GetName())
	}
	return nil
}

func (c *Controller) updateSparkApplicationStatusWithRetries(
	original *v1alpha1.SparkApplication,
	updateFunc func(*v1alpha1.SparkApplicationStatus)) *v1alpha1.SparkApplication {
	var lastUpdateErr error
	for i := 0; i < maximumUpdateRetries; i++ {
		toUpdate := original.DeepCopy()

		glog.V(2).Infof("Trying to update SparkApplication %s", original.Name)
		// Apply update
		updateFunc(&toUpdate.Status)

		// Let's keep the old App status if this is not a valid transition.
		if original.Status.AppState.State != toUpdate.Status.AppState.State &&
			!isValidDriverStateTransition(original.Status.AppState.State, toUpdate.Status.AppState.State) {
			glog.Warningf("Invalid Driver State Transition. From:[%v] To:[%v]", original.Status.AppState.State, toUpdate.Status.AppState.State)
			toUpdate.Status.AppState = original.Status.AppState
		}

		updated, err := c.tryUpdateStatus(original, toUpdate)
		if err == nil {
			if c.metrics != nil && updated != nil {
				// Original is the last state returned by API Server before update.
				c.metrics.exportMetrics(original, updated)
			}
			return updated
		}
		lastUpdateErr = err
		glog.Errorf("[Attempt: %d] failed to update SparkApplication [%s]. Error: [%v]", i, toUpdate.Name, err)

		// Failed update to the API server.
		// Get the latest version from the API server first and re-apply the update.
		name := toUpdate.Name
		original, err = c.crdClient.SparkoperatorV1alpha1().SparkApplications(toUpdate.Namespace).Get(name,
			metav1.GetOptions{})
		if err != nil {
			glog.Errorf("failed to get SparkApplication %s: %v", name, err)
			return nil
		}
	}

	if lastUpdateErr != nil {
		glog.Errorf("failed to update SparkApplication %s: %v", original.Name, lastUpdateErr)
	}

	return nil
}

func (c *Controller) tryUpdateStatus(
	original *v1alpha1.SparkApplication,
	toUpdate *v1alpha1.SparkApplication) (*v1alpha1.SparkApplication, error) {
	if reflect.DeepEqual(original.Status, toUpdate.Status) {
		return nil, nil
	}

	return c.crdClient.SparkoperatorV1alpha1().SparkApplications(toUpdate.Namespace).Update(toUpdate)
}

func (c *Controller) getSparkApplication(namespace string, name string) (*v1alpha1.SparkApplication, error) {
	return c.crdClient.SparkoperatorV1alpha1().SparkApplications(namespace).Get(name,
		metav1.GetOptions{})
}

// handleRestart handles application restart if the application has terminated and is subject to restart according to
// the restart policy.
func (c *Controller) handleRestart(app *v1alpha1.SparkApplication) {
	glog.Infof("SparkApplication %s failed or terminated, restarting it with RestartPolicy %s",
		app.Name, app.Spec.RestartPolicy)

	// Delete the old driver pod and UI service if necessary. Note that in case an error occurred here, we simply
	// log the error and continue enqueueing the application for resubmission because the driver has already
	// terminated so failure to cleanup the old driver pod and UI service is not a blocker. Also note that because
	// deleting a already terminated driver pod won't trigger a driver state update by the sparkPodMonitor so won't
	// cause repetitive restart handling.
	if err := c.deleteDriverAndUIService(app, false); err != nil {
		glog.Errorf("failed to delete the old driver pod and UI service for SparkApplication %s: %v",
			app.Name, err)
	}

	//Enqueue the object for re-submission.
	c.enqueue(app)

	c.recorder.Eventf(
		app,
		apiv1.EventTypeNormal,
		"SparkApplicationRestart",
		"SparkApplication %s was enqueued for restart",
		app.Name)
}

func (c *Controller) handleResubmission(namespace string, appName string) {
	app, err := c.getSparkApplication(namespace, appName)
	if err != nil {
		glog.Errorf("Failed to fetch SparkApplication %s in namespace %s", appName, namespace)
	}

	if !shouldRetrySubmission(app) {
		glog.Infof("Submission Retries exhauted for app %s. Not retrying.", app.Name)
		return
	}

	glog.Infof("Retrying submission of SparkApplication %s", app.Name)
	if err := c.deleteDriverAndUIService(app, false); err != nil {
		glog.Errorf("failed to delete the old driver pod and UI service for SparkApplication %s: %v",
			app.Name, err)
	}

	if app.Spec.SubmissionRetryInterval != nil {
		interval := time.Duration(*app.Spec.SubmissionRetryInterval) * time.Second
		// Attempt = SubmissionRetries +1
		c.enqueueAfter(app, time.Duration(app.Status.SubmissionRetries+1)*interval)
	} else {
		c.enqueue(app)
	}

	c.recorder.Eventf(
		app,
		apiv1.EventTypeNormal,
		"SparkApplicationSubmissionRetry",
		"SparkApplication %s was enqueued for submission retry",
		app.Name)
}

func (c *Controller) deleteDriverAndUIService(app *v1alpha1.SparkApplication, waitForDriverDeletion bool) error {
	if app.Status.DriverInfo.PodName != "" {
		err := c.kubeClient.CoreV1().Pods(app.Namespace).Delete(app.Status.DriverInfo.PodName,
			metav1.NewDeleteOptions(0))
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
	}
	if app.Status.DriverInfo.WebUIServiceName != "" {
		err := c.kubeClient.CoreV1().Services(app.Namespace).Delete(app.Status.DriverInfo.WebUIServiceName,
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

func (c *Controller) enqueueAfter(obj interface{}, after time.Duration) {
	key, err := keyFunc(obj)
	if err != nil {
		glog.Errorf("failed to get key for %v: %v", obj, err)
		return
	}

	c.queue.AddAfter(key, after)
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
	if phase == apiv1.PodSucceeded {
		c.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkDriverCompleted", "Driver %s completed", name)
	} else if phase == apiv1.PodFailed {
		c.recorder.Eventf(app, apiv1.EventTypeWarning, "SparkDriverFailed", "Driver %s failed", name)
	}
}

func (c *Controller) recordExecutorEvent(
	app *v1alpha1.SparkApplication, state v1alpha1.ExecutorState, name string) {
	if state == v1alpha1.ExecutorCompletedState {
		c.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkExecutorCompleted", "Executor %s completed", name)
	} else if state == v1alpha1.ExecutorFailedState {
		c.recorder.Eventf(app, apiv1.EventTypeWarning, "SparkExecutorFailed", "Executor %s failed", name)
	}
}

// shouldSubmit determines if a given application informed by onAdd should be submitted or not.
func shouldSubmit(app *v1alpha1.SparkApplication) bool {
	// The application should be submitted if one of the following conditions is satisfied:
	// 1. The application has not been submitted to run yet.
	// 2. The previous submission of the application failed and it is subject to re-submission based on the
	//    observed application state and submission failure tracking information.
	// 3. The application terminated and is subject to restart based on the observed application state and
	//    restart policy.
	return app.Status.AppState.State == "" ||
		app.Status.AppState.State == v1alpha1.NewState ||
		(app.Status.AppState.State == v1alpha1.FailedSubmissionState && shouldRetrySubmission(app)) ||
		shouldRestart(app)
}

// shouldRestart determines if a given application has terminated and is subject to restart according to the restart
// policy in the specification.
func shouldRestart(app *v1alpha1.SparkApplication) bool {
	return (app.Status.AppState.State == v1alpha1.FailedState && app.Spec.RestartPolicy == v1alpha1.OnFailure) ||
		(isAppTerminated(app.Status.AppState.State) && app.Spec.RestartPolicy == v1alpha1.Always)
}

// shouldRetrySubmission determines if a given application is subject to a submission retry.
func shouldRetrySubmission(app *v1alpha1.SparkApplication) bool {
	return app.Spec.MaxSubmissionRetries != nil &&
		app.Status.SubmissionRetries < *app.Spec.MaxSubmissionRetries
}

func getApplicationKey(namespace, name string) (string, error) {
	return cache.MetaNamespaceKeyFunc(&metav1.ObjectMeta{
		Namespace: namespace,
		Name:      name,
	})
}

// buildAppID builds an application ID in the form of <application name>-<32-bit hash>.
func buildAppID(app *v1alpha1.SparkApplication) string {
	hasher := util.NewHash32()
	hasher.Write([]byte(app.Name))
	hasher.Write([]byte(app.Namespace))
	hasher.Write([]byte(app.UID))
	hasher.Write([]byte(strconv.FormatInt(time.Now().UnixNano(), 10)))
	return fmt.Sprintf("%s-%d", app.Name, hasher.Sum32())
}

func isAppTerminated(appState v1alpha1.ApplicationStateType) bool {
	return appState == v1alpha1.CompletedState || appState == v1alpha1.FailedState
}

func isExecutorTerminated(executorState v1alpha1.ExecutorState) bool {
	return executorState == v1alpha1.ExecutorCompletedState || executorState == v1alpha1.ExecutorFailedState
}

func driverPodPhaseToApplicationState(podPhase apiv1.PodPhase) v1alpha1.ApplicationStateType {
	switch podPhase {
	case apiv1.PodPending:
		return v1alpha1.SubmittedState
	case apiv1.PodRunning:
		return v1alpha1.RunningState
	case apiv1.PodSucceeded:
		return v1alpha1.CompletedState
	case apiv1.PodFailed:
		return v1alpha1.FailedState
	default:
		return v1alpha1.UnknownState
	}
}
