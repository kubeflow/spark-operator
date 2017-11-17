package controller

import (
	"fmt"
	"sync"

	"github.com/golang/glog"
	"github.com/liyinan926/spark-operator/pkg/apis/v1alpha1"
	"github.com/liyinan926/spark-operator/pkg/crd"
	"github.com/liyinan926/spark-operator/pkg/util"

	apiv1 "k8s.io/api/core/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const (
	// SparkUIServiceNameAnnotationKey is the annotation key for recording the UI service name.
	SparkUIServiceNameAnnotationKey = "ui-service-name"
	sparkRoleLabel                  = "spark-role"
	sparkDriverRole                 = "driver"
	sparkExecutorRole               = "executor"
	sparkExecutorIDLabel            = "spark-exec-id"
)

// SparkApplicationController manages instances of SparkApplication.
type SparkApplicationController struct {
	crdClient                  *crd.Client
	kubeClient                 clientset.Interface
	extensionsClient           apiextensionsclient.Interface
	runner                     *SparkSubmitRunner
	sparkPodMonitor            *SparkPodMonitor
	appStateReportingChan      <-chan appStateUpdate
	driverStateReportingChan   <-chan driverStateUpdate
	executorStateReportingChan <-chan executorStateUpdate
	runningApps                map[string]*v1alpha1.SparkApplication
	mutex                      sync.Mutex
}

// New creates a new SparkApplicationController.
func New(
	crdClient *crd.Client,
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

	glog.Infof("Creating the CustomResourceDefinition %s", crd.CRDFullName)
	err := crd.CreateCRD(s.extensionsClient)
	if err != nil {
		errCh <- fmt.Errorf("failed to create the CustomResourceDefinition %s: %v", crd.CRDFullName, err)
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
		s.crdClient.Client,
		crd.CRDPlural,
		apiv1.NamespaceAll,
		fields.Everything())

	_, controller := cache.NewInformer(
		source,
		&v1alpha1.SparkApplication{},
		// resyncPeriod. Every resyncPeriod, all resources in the cache will retrigger events.
		// Set to 0 to disable the resync.
		0,
		// SparkApplication resource event handlers.
		cache.ResourceEventHandlerFuncs{
			AddFunc:    s.onAdd,
			UpdateFunc: s.onUpdate,
			DeleteFunc: s.onDelete,
		})

	go controller.Run(stopCh)
	return controller, nil
}

// Callback function called when a new SparkApplication object gets created.
func (s *SparkApplicationController) onAdd(obj interface{}) {
	app := obj.(*v1alpha1.SparkApplication)
	// NEVER modify objects from the store. It's a read-only, local cache.
	// You can use scheme.Copy() to make a deep copy of original object and modify this copy
	// Or create a copy manually for better performance.
	appCopy := app.DeepCopy()
	appCopy.Status.AppID = buildAppID(appCopy)
	appCopy.Status.AppState.State = v1alpha1.NewState
	appCopy.Annotations = make(map[string]string)

	serviceName, err := createSparkUIService(appCopy, s.kubeClient)
	if err != nil {
		glog.Errorf("failed to create a UI service for SparkApplication %s: %v", appCopy.Name, err)
	}
	appCopy.Annotations[SparkUIServiceNameAnnotationKey] = serviceName

	s.mutex.Lock()
	defer s.mutex.Unlock()

	updatedApp, err := s.crdClient.Update(appCopy)
	if err != nil {
		glog.Errorf("failed to update SparkApplication %s: %v", appCopy.Name, err)
		return
	}

	s.runningApps[updatedApp.Status.AppID] = updatedApp

	submissionCmdArgs, err := buildSubmissionCommandArgs(updatedApp)
	if err != nil {
		glog.Errorf("failed to build the submission command for SparkApplication %s: %v", updatedApp.Name, err)
	}
	if !updatedApp.Spec.SubmissionByUser {
		s.runner.submit(newSubmission(submissionCmdArgs, updatedApp))
	}
}

func (s *SparkApplicationController) onUpdate(oldObj, newObj interface{}) {
	newApp := newObj.(*v1alpha1.SparkApplication)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.runningApps[newApp.Status.AppID] = newApp.DeepCopy()
}

func (s *SparkApplicationController) onDelete(obj interface{}) {
	app := obj.(*v1alpha1.SparkApplication)
	s.mutex.Lock()
	delete(s.runningApps, app.Status.AppID)
	s.mutex.Unlock()

	if serviceName, ok := app.Annotations[SparkUIServiceNameAnnotationKey]; ok {
		glog.Infof("Deleting the UI service %s for SparkApplication %s", serviceName, app.Name)
		err := s.kubeClient.CoreV1().Services(app.Namespace).Delete(serviceName, &metav1.DeleteOptions{})
		if err != nil {
			glog.Errorf("failed to delete the UI service %s for SparkApplication %s: %v", serviceName, app.Name, err)
		}
	}

	driverServiceName := app.Status.DriverInfo.PodName + "-svc"
	glog.Infof("Deleting the headless service %s for the driver of SparkApplication %s", driverServiceName, app.Name)
	s.kubeClient.CoreV1().Services(app.Namespace).Delete(driverServiceName, &metav1.DeleteOptions{})
	glog.Infof("Deleting the driver pod %s of SparkApplication %s", app.Status.DriverInfo.PodName, app.Name)
	s.kubeClient.CoreV1().Pods(app.Namespace).Delete(app.Status.DriverInfo.PodName, &metav1.DeleteOptions{})
}

func (s *SparkApplicationController) processDriverStateUpdates() {
	for update := range s.driverStateReportingChan {
		s.processSingleDriverStateUpdate(update)
	}
}

func (s *SparkApplicationController) processSingleDriverStateUpdate(update driverStateUpdate) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if app, ok := s.runningApps[update.appID]; ok {
		app.Status.DriverInfo.PodName = update.podName
		if update.nodeName != "" {
			nodeIP := s.getNodeExternalIP(update.nodeName)
			if nodeIP != "" {
				app.Status.DriverInfo.WebUIAddress = fmt.Sprintf("%s:%d", nodeIP, app.Status.DriverInfo.WebUIPort)
			}
		}

		appState := driverPodPhaseToApplicationState(update.podPhase)
		// Update the application based on the driver pod phase if the driver has terminated.
		if isAppTerminated(appState) {
			app.Status.AppState.State = appState
		}

		updated, err := s.crdClient.Update(app)
		if err != nil {
			glog.Errorf("failed to update SparkApplication %s: %v", app.Name, err)
		} else {
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
		// The application state may have already been set based on the driver pod state, so it's set here only if otherwise.
		if !isAppTerminated(app.Status.AppState.State) {
			app.Status.AppState.State = update.state
			app.Status.AppState.ErrorMessage = update.errorMessage
			updated, err := s.crdClient.Update(app)
			if err != nil {
				glog.Errorf("failed to update SparkApplication %s: %v", app.Name, err)
			} else {
				s.runningApps[updated.Status.AppID] = updated
			}
		}
	}
}

func (s *SparkApplicationController) processExecutorStateUpdates() {
	for update := range s.executorStateReportingChan {
		s.processSingleExecutorStateUpdate(update)
	}
}

func (s *SparkApplicationController) processSingleExecutorStateUpdate(update executorStateUpdate) {
	glog.V(2).Infof("Received new state %s for executor %s running in %s", update.state, update.executorID, update.podName)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if app, ok := s.runningApps[update.appID]; ok {
		if app.Status.ExecutorState == nil {
			app.Status.ExecutorState = make(map[string]v1alpha1.ExecutorState)
		}
		if update.state == v1alpha1.ExecutorCompletedState || update.state == v1alpha1.ExecutorFailedState {
			app.Status.ExecutorState[update.podName] = update.state
			updated, err := s.crdClient.Update(app)
			if err != nil {
				glog.Errorf("failed to update SparkApplication %s: %v", app.Name, err)
			} else {
				s.runningApps[updated.Status.AppID] = updated
			}
		}
	}
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
