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
	"reflect"
	"sync"
	"time"

	"github.com/golang/glog"
	"k8s.io/spark-on-k8s-operator/pkg/apis/v1alpha1"
	"k8s.io/spark-on-k8s-operator/pkg/crd"
	"k8s.io/spark-on-k8s-operator/pkg/util"

	apiv1 "k8s.io/api/core/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const (
	sparkUIServiceNameAnnotationKey = "ui-service-name"
	sparkRoleLabel                  = "spark-role"
	sparkDriverRole                 = "driver"
	sparkExecutorRole               = "executor"
	sparkExecutorIDLabel            = "spark-exec-id"
	maximumUpdateRetries            = 3
)

// SparkApplicationController manages instances of SparkApplication.
type SparkApplicationController struct {
	crdClient                  crd.ClientInterface
	kubeClient                 clientset.Interface
	extensionsClient           apiextensionsclient.Interface
	runner                     *sparkSubmitRunner
	sparkPodMonitor            *sparkPodMonitor
	appStateReportingChan      <-chan appStateUpdate
	driverStateReportingChan   <-chan driverStateUpdate
	executorStateReportingChan <-chan executorStateUpdate
	runningApps                map[string]*v1alpha1.SparkApplication // Guarded by mutex.
	mutex                      sync.Mutex                            // Guard SparkApplication updates to the API server and runningApps.
}

// New creates a new SparkApplicationController.
func New(
	crdClient crd.ClientInterface,
	kubeClient clientset.Interface,
	extensionsClient apiextensionsclient.Interface,
	submissionRunnerWorkers int) *SparkApplicationController {
	appStateReportingChan := make(chan appStateUpdate, submissionRunnerWorkers)
	driverStateReportingChan := make(chan driverStateUpdate)
	executorStateReportingChan := make(chan executorStateUpdate)
	runner := newSparkSubmitRunner(submissionRunnerWorkers, appStateReportingChan)
	sparkPodMonitor := newSparkPodMonitor(kubeClient, driverStateReportingChan, executorStateReportingChan)

	return &SparkApplicationController{
		crdClient:                  crdClient,
		kubeClient:                 kubeClient,
		extensionsClient:           extensionsClient,
		runner:                     runner,
		sparkPodMonitor:            sparkPodMonitor,
		appStateReportingChan:      appStateReportingChan,
		driverStateReportingChan:   driverStateReportingChan,
		executorStateReportingChan: executorStateReportingChan,
		runningApps:                make(map[string]*v1alpha1.SparkApplication),
	}
}

// Run starts the SparkApplicationController by registering a watcher for SparkApplication objects.
func (s *SparkApplicationController) Run(stopCh <-chan struct{}, errCh chan<- error) {
	glog.Info("Starting the SparkApplication controller")
	defer glog.Info("Stopping the SparkApplication controller")

	glog.Infof("Creating the CustomResourceDefinition %s", crd.FullName)
	err := crd.CreateCRD(s.extensionsClient)
	if err != nil {
		errCh <- fmt.Errorf("failed to create the CustomResourceDefinition %s: %v", crd.FullName, err)
		return
	}

	glog.Info("Starting the SparkApplication watcher")
	_, err = s.watchSparkApplications(stopCh)
	if err != nil {
		errCh <- fmt.Errorf("failed to register watch for SparkApplication resource: %v", err)
		return
	}

	go s.runner.run(stopCh)
	go s.sparkPodMonitor.run(stopCh)

	go s.processAppStateUpdates()
	go s.processDriverStateUpdates()
	go s.processExecutorStateUpdates()

	<-stopCh
}

func (s *SparkApplicationController) watchSparkApplications(stopCh <-chan struct{}) (cache.Controller, error) {
	source := cache.NewListWatchFromClient(
		s.crdClient.RESTClient(),
		crd.Plural,
		apiv1.NamespaceAll,
		fields.Everything())

	_, cacheController := cache.NewInformer(
		source,
		&v1alpha1.SparkApplication{},
		// resyncPeriod. Every resyncPeriod, all resources in the cache will retrigger events.
		// Set to 0 to disable the resync.
		0*time.Second,
		// SparkApplication resource event handlers.
		cache.ResourceEventHandlerFuncs{
			AddFunc:    s.onAdd,
			UpdateFunc: s.onUpdate,
			DeleteFunc: s.onDelete,
		})

	go cacheController.Run(stopCh)
	return cacheController, nil
}

// Callback function called when a new SparkApplication object gets created.
func (s *SparkApplicationController) onAdd(obj interface{}) {
	app := obj.(*v1alpha1.SparkApplication)
	// NEVER modify objects from the store. It's a read-only, local cache.
	// You can use scheme.Copy() to make a deep copy of original object and modify this copy
	// Or create a copy manually for better performance.
	s.submitApp(app.DeepCopy())
}

func (s *SparkApplicationController) onUpdate(oldObj, newObj interface{}) {
	oldApp := oldObj.(*v1alpha1.SparkApplication)
	s.mutex.Lock()
	delete(s.runningApps, oldApp.Status.AppID)
	s.mutex.Unlock()

	// Kill the old application instance by deleting its driver pod.
	err := s.kubeClient.CoreV1().Pods(oldApp.Namespace).Delete(oldApp.Status.DriverInfo.PodName, &metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		glog.Errorf("failed to delete the driver pod %s of the old instance of application %s",
			oldApp.Status.DriverInfo.PodName, oldApp.Name)
		return
	}

	// Then submit an instance of the new application to run.
	newApp := newObj.(*v1alpha1.SparkApplication)
	s.submitApp(newApp.DeepCopy())
}

func (s *SparkApplicationController) onDelete(obj interface{}) {
	app := obj.(*v1alpha1.SparkApplication)
	s.mutex.Lock()
	delete(s.runningApps, app.Status.AppID)
	s.mutex.Unlock()

	if serviceName, ok := app.Annotations[sparkUIServiceNameAnnotationKey]; ok {
		glog.Infof("Deleting the UI service %s for SparkApplication %s", serviceName, app.Name)
		err := s.kubeClient.CoreV1().Services(app.Namespace).Delete(serviceName, &metav1.DeleteOptions{})
		if err != nil {
			glog.Errorf(
				"failed to delete the UI service %s for SparkApplication %s: %v",
				serviceName,
				app.Name,
				err)
		}
	}

	driverServiceName := app.Status.DriverInfo.PodName + "-svc"
	glog.Infof(
		"Deleting the headless service %s for the driver of SparkApplication %s",
		driverServiceName,
		app.Name)
	s.kubeClient.CoreV1().Services(app.Namespace).Delete(driverServiceName, &metav1.DeleteOptions{})
	glog.Infof(
		"Deleting the driver pod %s of SparkApplication %s",
		app.Status.DriverInfo.PodName,
		app.Name)
	s.kubeClient.CoreV1().Pods(app.Namespace).Delete(app.Status.DriverInfo.PodName, &metav1.DeleteOptions{})
}

func (s *SparkApplicationController) submitApp(app *v1alpha1.SparkApplication) {
	app.Status.AppID = buildAppID(app)
	app.Status.AppState.State = v1alpha1.NewState
	app.Annotations = make(map[string]string)

	serviceName, err := createSparkUIService(app, s.kubeClient)
	if err != nil {
		glog.Errorf("failed to create a UI service for SparkApplication %s: %v", app.Name, err)
	}
	app.Annotations[sparkUIServiceNameAnnotationKey] = serviceName

	s.mutex.Lock()
	defer s.mutex.Unlock()

	updatedApp, err := s.crdClient.Update(app)
	if err != nil {
		glog.Errorf("failed to update SparkApplication %s: %v", app.Name, err)
		return
	}

	s.runningApps[updatedApp.Status.AppID] = updatedApp

	submissionCmdArgs, err := buildSubmissionCommandArgs(updatedApp)
	if err != nil {
		glog.Errorf(
			"failed to build the submission command for SparkApplication %s: %v",
			updatedApp.Name,
			err)
	}
	if !updatedApp.Spec.SubmissionByUser {
		s.runner.submit(newSubmission(submissionCmdArgs, updatedApp))
	}
}

func (s *SparkApplicationController) processDriverStateUpdates() {
	for update := range s.driverStateReportingChan {
		s.processSingleDriverStateUpdate(update)
	}
}

func (s *SparkApplicationController) processSingleDriverStateUpdate(update driverStateUpdate) {
	glog.V(2).Infof(
		"Received driver state update for %s with phase %s",
		update.appID,
		update.podPhase)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if app, ok := s.runningApps[update.appID]; ok {
		updated := s.updateSparkApplicationWithRetries(app, app.DeepCopy(), func(toUpdate *v1alpha1.SparkApplication) {
			toUpdate.Status.DriverInfo.PodName = update.podName
			if update.nodeName != "" {
				nodeIP := s.getNodeExternalIP(update.nodeName)
				if nodeIP != "" {
					toUpdate.Status.DriverInfo.WebUIAddress = fmt.Sprintf(
						"%s:%d", nodeIP, toUpdate.Status.DriverInfo.WebUIPort)
				}
			}

			appState := driverPodPhaseToApplicationState(update.podPhase)
			// Update the application based on the driver pod phase if the driver has terminated.
			if isAppTerminated(appState) {
				toUpdate.Status.AppState.State = appState
			}
		})

		if updated != nil {
			s.runningApps[updated.Status.AppID] = updated
		}
	}
}

func (s *SparkApplicationController) processAppStateUpdates() {
	for update := range s.appStateReportingChan {
		s.processSingleAppStateUpdate(update)
	}
}

func (s *SparkApplicationController) processSingleAppStateUpdate(update appStateUpdate) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if app, ok := s.runningApps[update.appID]; ok {
		updated := s.updateSparkApplicationWithRetries(app, app.DeepCopy(), func(toUpdate *v1alpha1.SparkApplication) {
			// The application state may have already been set based on the driver pod state,
			// so it's set here only if otherwise.
			if !isAppTerminated(toUpdate.Status.AppState.State) {
				toUpdate.Status.AppState.State = update.state
				toUpdate.Status.AppState.ErrorMessage = update.errorMessage
			}
			if !update.submissionTime.IsZero() {
				toUpdate.Status.SubmissionTime = update.submissionTime
			}
			if !update.completionTime.IsZero() {
				toUpdate.Status.CompletionTime = update.completionTime
			}
		})

		if updated != nil {
			s.runningApps[updated.Status.AppID] = updated
		}
	}
}

func (s *SparkApplicationController) processExecutorStateUpdates() {
	for update := range s.executorStateReportingChan {
		s.processSingleExecutorStateUpdate(update)
	}
}

func (s *SparkApplicationController) processSingleExecutorStateUpdate(update executorStateUpdate) {
	glog.V(2).Infof(
		"Received state update of executor %s for %s with state %s",
		update.executorID,
		update.appID,
		update.state)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if app, ok := s.runningApps[update.appID]; ok {
		updated := s.updateSparkApplicationWithRetries(app, app.DeepCopy(), func(toUpdate *v1alpha1.SparkApplication) {
			if toUpdate.Status.ExecutorState == nil {
				toUpdate.Status.ExecutorState = make(map[string]v1alpha1.ExecutorState)
			}
			if update.state != v1alpha1.ExecutorPendingState {
				toUpdate.Status.ExecutorState[update.podName] = update.state
			}
		})

		if updated != nil {
			s.runningApps[updated.Status.AppID] = updated
		}
	}
}

func (s *SparkApplicationController) updateSparkApplicationWithRetries(
	original *v1alpha1.SparkApplication,
	toUpdate *v1alpha1.SparkApplication,
	updateFunc func(*v1alpha1.SparkApplication)) *v1alpha1.SparkApplication {
	for i := 0; i < maximumUpdateRetries; i++ {
		updateFunc(toUpdate)
		if reflect.DeepEqual(original.Status, toUpdate.Status) {
			return nil
		}

		updated, err := s.crdClient.Update(toUpdate)
		if err == nil {
			return updated
		}

		// Failed update to the API server.
		// Get the latest version from the API server first and re-apply the update.
		name := toUpdate.Name
		toUpdate, err = s.crdClient.Get(toUpdate.Name, toUpdate.Namespace)
		if err != nil {
			glog.Errorf("failed to get SparkApplication %s: %v", name, err)
			return nil
		}
	}

	return nil
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

// buildAppID builds an application ID in the form of <application name>-<32-bit hash>.
func buildAppID(app *v1alpha1.SparkApplication) string {
	hasher := util.NewHash32()
	hasher.Write([]byte(app.Name))
	hasher.Write([]byte(app.Namespace))
	hasher.Write([]byte(app.UID))
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
		return ""
	}
}
