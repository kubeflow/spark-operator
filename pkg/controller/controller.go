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

package controller

import (
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"time"

	"github.com/golang/glog"

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
	crdinformers "k8s.io/spark-on-k8s-operator/pkg/client/informers/externalversions"
	"k8s.io/spark-on-k8s-operator/pkg/crd"
	"k8s.io/spark-on-k8s-operator/pkg/util"
)

const (
	sparkRoleLabel       = "spark-role"
	sparkDriverRole      = "driver"
	sparkExecutorRole    = "executor"
	sparkExecutorIDLabel = "spark-exec-id"
	maximumUpdateRetries = 3
)

// SparkApplicationController manages instances of SparkApplication.
type SparkApplicationController struct {
	crdClient             crdclientset.Interface
	kubeClient            clientset.Interface
	extensionsClient      apiextensionsclient.Interface
	queue                 workqueue.RateLimitingInterface
	informer              cache.SharedIndexInformer
	store                 cache.Store
	recorder              record.EventRecorder
	runner                *sparkSubmitRunner
	sparkPodMonitor       *sparkPodMonitor
	appStateReportingChan <-chan *appStateUpdate
	podStateReportingChan <-chan interface{}
}

// New creates a new SparkApplicationController.
func New(
	crdClient crdclientset.Interface,
	kubeClient clientset.Interface,
	extensionsClient apiextensionsclient.Interface,
	submissionRunnerWorkers int) *SparkApplicationController {
	v1alpha1.AddToScheme(scheme.Scheme)
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.V(2).Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{
		Interface: kubeClient.CoreV1().Events(apiv1.NamespaceAll),
	})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, apiv1.EventSource{Component: "spark-operator"})

	return newSparkApplicationController(crdClient, kubeClient, extensionsClient, recorder, submissionRunnerWorkers)
}

func newSparkApplicationController(
	crdClient crdclientset.Interface,
	kubeClient clientset.Interface,
	extensionsClient apiextensionsclient.Interface,
	eventRecorder record.EventRecorder,
	submissionRunnerWorkers int) *SparkApplicationController {
	queue := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(),
		"spark-application-controller")

	appStateReportingChan := make(chan *appStateUpdate, submissionRunnerWorkers)
	podStateReportingChan := make(chan interface{})

	runner := newSparkSubmitRunner(submissionRunnerWorkers, appStateReportingChan)
	sparkPodMonitor := newSparkPodMonitor(kubeClient, podStateReportingChan)

	controller := &SparkApplicationController{
		crdClient:             crdClient,
		kubeClient:            kubeClient,
		extensionsClient:      extensionsClient,
		recorder:              eventRecorder,
		queue:                 queue,
		runner:                runner,
		sparkPodMonitor:       sparkPodMonitor,
		appStateReportingChan: appStateReportingChan,
		podStateReportingChan: podStateReportingChan,
	}

	informerFactory := crdinformers.NewSharedInformerFactory(
		crdClient,
		// resyncPeriod. Every resyncPeriod, all resources in the cache will re-trigger events.
		// Set to 0 to disable the resync.
		0*time.Second)
	controller.informer = informerFactory.Sparkoperator().V1alpha1().SparkApplications().Informer()
	controller.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.onAdd,
		DeleteFunc: controller.onDelete,
	})
	controller.store = controller.informer.GetStore()

	return controller
}

// Start starts the SparkApplicationController by registering a watcher for SparkApplication objects.
func (s *SparkApplicationController) Start(workers int, stopCh <-chan struct{}) error {
	glog.Info("Starting the SparkApplication controller")

	glog.Infof("Creating CustomResourceDefinition %s", crd.FullName)
	err := crd.CreateCRD(s.extensionsClient)
	if err != nil {
		return fmt.Errorf("failed to create CustomResourceDefinition %s: %v", crd.FullName, err)
	}

	glog.Info("Starting the SparkApplication informer")
	go s.informer.Run(stopCh)

	if !cache.WaitForCacheSync(stopCh, s.informer.HasSynced) {
		return fmt.Errorf("timed out waiting for cache to sync")
	}

	glog.Info("Starting the workers of the SparkApplication controller")
	for i := 0; i < workers; i++ {
		// runWorker will loop until "something bad" happens. Until will then rekick
		// the worker after one second.
		go wait.Until(s.runWorker, time.Second, stopCh)
	}

	go s.runner.run(stopCh)
	go s.sparkPodMonitor.run(stopCh)

	go s.processAppStateUpdates()
	go s.processPodStateUpdates()

	return nil
}

func (s *SparkApplicationController) Stop() {
	glog.Info("Stopping the SparkApplication controller")
	s.queue.ShutDown()
	glog.Infof("Deleting CustomResourceDefinition %s", crd.FullName)
	if err := crd.DeleteCRD(s.extensionsClient); err != nil {
		glog.Errorf("failed to delete CustomResourceDefinition %s: %v", crd.FullName, err)
	}
}

// Callback function called when a new SparkApplication object gets created.
func (s *SparkApplicationController) onAdd(obj interface{}) {
	app := obj.(*v1alpha1.SparkApplication)

	s.recorder.Eventf(
		app,
		apiv1.EventTypeNormal,
		"SparkApplicationSubmission",
		"Submitting SparkApplication: %s",
		app.Name)

	key := getApplicationKey(app.Namespace, app.Name)
	s.queue.AddRateLimited(key)
}

func (s *SparkApplicationController) onDelete(obj interface{}) {
	app := obj.(*v1alpha1.SparkApplication)

	s.recorder.Eventf(
		app,
		apiv1.EventTypeNormal,
		"SparkApplicationDeletion",
		"Deleting SparkApplication: %s",
		app.Name)

	key := getApplicationKey(app.Namespace, app.Name)
	s.queue.Forget(key)
	s.queue.Done(key)
}

// runWorker runs a single controller worker.
func (s *SparkApplicationController) runWorker() {
	defer utilruntime.HandleCrash()
	for s.processNextItem() {
	}
}

func (s *SparkApplicationController) processNextItem() bool {
	key, quit := s.queue.Get()
	if quit {
		return false
	}
	defer s.queue.Done(key)

	err := s.syncSparkApplication(key.(string))
	if err == nil {
		// Successfully processed the key or the key was not found so tell the queue to stop tracking
		// history for your key. This will reset things like failure counts for per-item rate limiting.
		s.queue.Forget(key)
		return true
	}

	// There was a failure so be sure to report it. This method allows for pluggable error handling
	// which can be used for things like cluster-monitoring
	utilruntime.HandleError(fmt.Errorf("failed to sync SparkApplication %q: %v", key, err))
	// Since we failed, we should requeue the item to work on later.  This method will add a backoff
	// to avoid hot-looping on particular items (they're probably still not going to work right away)
	// and overall controller protection (everything I've done is broken, this controller needs to
	// calm down or it can starve other useful work) cases.
	s.queue.AddRateLimited(key)

	return true
}

func (s *SparkApplicationController) syncSparkApplication(key string) error {
	app, err := s.getSparkApplicationFromStore(key)
	if err != nil {
		return err
	}
	s.submitApp(app)
	return nil
}

func (s *SparkApplicationController) submitApp(app *v1alpha1.SparkApplication) {
	appStatus := v1alpha1.SparkApplicationStatus{
		AppID: buildAppID(app),
		AppState: v1alpha1.ApplicationState{
			State: v1alpha1.NewState,
		},
	}
	name, port, err := createSparkUIService(app, appStatus.AppID, s.kubeClient)
	if err != nil {
		glog.Errorf("failed to create a UI service for SparkApplication %s: %v", app.Name, err)
	} else {
		appStatus.DriverInfo.WebUIServiceName = name
		appStatus.DriverInfo.WebUIPort = port
	}

	updatedApp := s.updateSparkApplicationStatusWithRetries(app, func(status *v1alpha1.SparkApplicationStatus) {
		*status = appStatus
	})
	if updatedApp == nil {
		return
	}

	submissionCmdArgs, err := buildSubmissionCommandArgs(updatedApp)
	if err != nil {
		glog.Errorf(
			"failed to build the submission command for SparkApplication %s: %v",
			updatedApp.Name,
			err)
	}

	s.runner.submit(newSubmission(submissionCmdArgs, updatedApp))
}

func (s *SparkApplicationController) processPodStateUpdates() {
	for update := range s.podStateReportingChan {
		switch update.(type) {
		case *driverStateUpdate:
			updatedApp := s.processSingleDriverStateUpdate(update.(*driverStateUpdate))
			if updatedApp != nil && isAppTerminated(updatedApp.Status.AppState.State) {
				s.handleRestart(updatedApp)
			}
			continue
		case *executorStateUpdate:
			s.processSingleExecutorStateUpdate(update.(*executorStateUpdate))
		}
	}
}

func (s *SparkApplicationController) processSingleDriverStateUpdate(
	update *driverStateUpdate) *v1alpha1.SparkApplication {
	glog.V(2).Infof(
		"Received driver state update for SparkApplication %s in namespace %s with phase %s",
		update.appName, update.appNamespace, update.podPhase)

	key := getApplicationKey(update.appNamespace, update.appName)
	app, err := s.getSparkApplicationFromStore(key)
	if err != nil {
		// Update may be the result of pod deletion due to deletion of the owning SparkApplication object.
		// Ignore the error if the owning SparkApplication object does not exist.
		if !errors.IsNotFound(err) {
			glog.Errorf("failed to get SparkApplication %s in namespace %s from the store: %v", update.appName,
				update.appNamespace, err)
		}
		return nil
	}

	// The application state is solely based on the driver pod phase except when submission fails and
	// no driver pod is launched.
	appState := driverPodPhaseToApplicationState(update.podPhase)
	if isAppTerminated(appState) {
		s.recorder.Eventf(
			app,
			apiv1.EventTypeNormal,
			"SparkApplicationTermination",
			"SparkApplication %s terminated with state: %v",
			update.appName,
			appState)
	}

	updated := s.updateSparkApplicationStatusWithRetries(app, func(status *v1alpha1.SparkApplicationStatus) {
		status.DriverInfo.PodName = update.podName
		if update.nodeName != "" {
			if nodeIP := s.getNodeExternalIP(update.nodeName); nodeIP != "" {
				status.DriverInfo.WebUIAddress = fmt.Sprintf("%s:%d", nodeIP,
					status.DriverInfo.WebUIPort)
			}
		}

		status.AppState.State = appState
		if !update.completionTime.IsZero() {
			status.CompletionTime = update.completionTime
		}
	})

	return updated
}

func (s *SparkApplicationController) processAppStateUpdates() {
	for update := range s.appStateReportingChan {
		s.processSingleAppStateUpdate(update)
	}
}

func (s *SparkApplicationController) processSingleAppStateUpdate(update *appStateUpdate) {
	key := getApplicationKey(update.namespace, update.name)
	app, err := s.getSparkApplicationFromStore(key)
	if err != nil {
		glog.Errorf("failed to get SparkApplication %s in namespace %s from the store: %v", update.name,
			update.namespace, err)
		return
	}

	submissionRetries := app.Status.SubmissionRetries
	if update.state == v1alpha1.FailedSubmissionState {
		s.recorder.Eventf(
			app,
			apiv1.EventTypeNormal,
			"SparkApplicationSubmissionFailure",
			"SparkApplication %s failed submission",
			update.name)

		if submissionRetries < app.Spec.MaxSubmissionRetries {
			glog.Infof("Retrying submission of SparkApplication %s", update.name)
			key := getApplicationKey(update.namespace, update.name)
			s.queue.AddRateLimited(key)
			submissionRetries++
			s.recorder.Eventf(
				app,
				apiv1.EventTypeNormal,
				"SparkApplicationSubmissionRetry",
				"Retried submission of SparkApplication %s",
				update.name)
		} else {
			glog.Errorf("maximum number of submission retries of SparkApplication %s has been reached, not "+
				"attempting more retries", update.name)
		}
	}

	s.updateSparkApplicationStatusWithRetries(app, func(status *v1alpha1.SparkApplicationStatus) {
		status.AppState.State = update.state
		status.AppState.ErrorMessage = update.errorMessage
		status.SubmissionRetries = submissionRetries
		if !update.submissionTime.IsZero() {
			status.SubmissionTime = update.submissionTime
		}
	})
}

func (s *SparkApplicationController) processSingleExecutorStateUpdate(update *executorStateUpdate) {
	glog.V(2).Infof(
		"Received state update of executor %s for SparkApplication %s in namespace %s with state %s",
		update.executorID, update.appName, update.appNamespace, update.state)

	key := getApplicationKey(update.appNamespace, update.appName)
	app, err := s.getSparkApplicationFromStore(key)
	if err != nil {
		// Update may be the result of pod deletion due to deletion of the owning SparkApplication object.
		// Ignore the error if the owning SparkApplication object does not exist.
		if !errors.IsNotFound(err) {
			glog.Errorf("failed to get SparkApplication %s in namespace %s from the store: %v", update.appName,
				update.appNamespace, err)
		}
		return
	}

	s.updateSparkApplicationStatusWithRetries(app, func(status *v1alpha1.SparkApplicationStatus) {
		if status.ExecutorState == nil {
			status.ExecutorState = make(map[string]v1alpha1.ExecutorState)
		}
		if update.state != v1alpha1.ExecutorPendingState {
			status.ExecutorState[update.podName] = update.state
		}
	})
}

func (s *SparkApplicationController) updateSparkApplicationStatusWithRetries(
	original *v1alpha1.SparkApplication,
	updateFunc func(*v1alpha1.SparkApplicationStatus)) *v1alpha1.SparkApplication {
	toUpdate := original.DeepCopy()

	var lastUpdateErr error
	for i := 0; i < maximumUpdateRetries; i++ {
		updated, err := s.tryUpdateStatus(original, toUpdate, updateFunc)
		if err == nil {
			return updated
		}
		lastUpdateErr = err

		// Failed update to the API server.
		// Get the latest version from the API server first and re-apply the update.
		name := toUpdate.Name
		toUpdate, err = s.crdClient.SparkoperatorV1alpha1().SparkApplications(toUpdate.Namespace).Get(name,
			metav1.GetOptions{})
		if err != nil {
			glog.Errorf("failed to get SparkApplication %s: %v", name, err)
			return nil
		}
	}

	if lastUpdateErr != nil {
		glog.Errorf("failed to update SparkApplication %s: %v", toUpdate.Name, lastUpdateErr)
	}

	return nil
}

func (s *SparkApplicationController) tryUpdateStatus(
	original *v1alpha1.SparkApplication,
	toUpdate *v1alpha1.SparkApplication,
	updateFunc func(*v1alpha1.SparkApplicationStatus)) (*v1alpha1.SparkApplication, error) {
	updateFunc(&toUpdate.Status)
	if reflect.DeepEqual(original.Status, toUpdate.Status) {
		return nil, nil
	}

	return s.crdClient.SparkoperatorV1alpha1().SparkApplications(toUpdate.Namespace).Update(toUpdate)
}

func (s *SparkApplicationController) getSparkApplicationFromStore(key string) (*v1alpha1.SparkApplication, error) {
	item, exists, err := s.store.GetByKey(key)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, &errors.StatusError{
			ErrStatus: metav1.Status{
				Status: metav1.StatusFailure,
				Code:   http.StatusNotFound,
				Reason: metav1.StatusReasonNotFound,
			},
		}
	}

	return item.(*v1alpha1.SparkApplication), nil
}

func getApplicationKey(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

func (s *SparkApplicationController) getNodeExternalIP(nodeName string) string {
	node, err := s.kubeClient.CoreV1().Nodes().Get(nodeName, metav1.GetOptions{})
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

func (s *SparkApplicationController) handleRestart(app *v1alpha1.SparkApplication) {
	if (app.Status.AppState.State == v1alpha1.FailedState && app.Spec.RestartPolicy == v1alpha1.OnFailure) ||
		app.Spec.RestartPolicy == v1alpha1.Always {
		glog.Infof("SparkApplication %s failed or terminated, restarting it with RestartPolicy %s",
			app.Name, app.Spec.RestartPolicy)
		s.recorder.Eventf(
			app,
			apiv1.EventTypeNormal,
			"SparkApplicationRestart",
			"Re-starting SparkApplication: %s",
			app.Name)

		// Cleanup old driver pod and UI service if necessary.
		if app.Status.DriverInfo.PodName != "" {
			err := s.kubeClient.CoreV1().Pods(app.Namespace).Delete(app.Status.DriverInfo.PodName,
				&metav1.DeleteOptions{})
			if err != nil {
				glog.Errorf("failed to delete old driver pod %s of SparkApplication %s: %v",
					app.Status.DriverInfo.PodName, app.Name, err)
			}
		}
		if app.Status.DriverInfo.WebUIServiceName != "" {
			err := s.kubeClient.CoreV1().Services(app.Namespace).Delete(app.Status.DriverInfo.WebUIServiceName,
				&metav1.DeleteOptions{})
			if err != nil {
				glog.Errorf("failed to delete old web UI service %s of SparkApplication %s: %v",
					app.Status.DriverInfo.WebUIServiceName, app.Name, err)
			}
		}

		// Add the application key to the queue for re-submission.
		key := getApplicationKey(app.Namespace, app.Name)
		s.queue.AddRateLimited(key)
	}
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
