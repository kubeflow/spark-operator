package controller

import (
	"fmt"

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
	// SubmissionCommandAnnotationKey is the annotation key for recording the submission command.
	SubmissionCommandAnnotationKey = "submission-command"
)

// SparkApplicationController manages instances of SparkApplication.
type SparkApplicationController struct {
	crdClient             *crd.Client
	kubeClient            clientset.Interface
	extensionsClient      apiextensionsclient.Interface
	appStateReportingChan chan *v1alpha1.SparkApplication
	runner                *SparkSubmitRunner
}

// NewSparkApplicationController creates a new SparkApplicationController.
func NewSparkApplicationController(
	crdClient *crd.Client,
	kubeClient clientset.Interface,
	extensionsClient apiextensionsclient.Interface) *SparkApplicationController {
	appStateReportingChan := make(chan *v1alpha1.SparkApplication, 3)
	return &SparkApplicationController{
		crdClient:             crdClient,
		kubeClient:            kubeClient,
		extensionsClient:      extensionsClient,
		appStateReportingChan: appStateReportingChan,
		runner:                newRunner(3, appStateReportingChan),
	}
}

// Run starts the SparkApplicationController by registering a watcher for SparkApplication objects.
func (s *SparkApplicationController) Run(stopCh <-chan struct{}, errCh chan<- error) {
	glog.Info("Starting the SparkApplication controller")
	defer glog.Info("Shutting down the SparkApplication controller")

	glog.Infof("Creating the CustomResourceDefinition %s...", crd.CRDFullName)
	err := crd.CreateCRD(s.extensionsClient)
	if err != nil {
		errCh <- fmt.Errorf("Failed to create the CustomResourceDefinition for SparkApplication: %v", err)
		return
	}

	glog.Info("Starting the SparkApplication watcher...")
	_, err = s.watchSparkApplications(stopCh)
	if err != nil {
		errCh <- fmt.Errorf("Failed to register watch for SparkApplication resource: %v", err)
		return
	}

	go s.runner.start(stopCh)
	go func() {
		for app := range s.appStateReportingChan {
			_, err = s.crdClient.Update(app)
			if err != nil {
				glog.Errorf("Failed to update SparkApplication %s: %v", app.Name, err)
			}
		}
	}()

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
	glog.Infof("[CONTROLLER] OnAdd %s\n", app.ObjectMeta.SelfLink)

	// NEVER modify objects from the store. It's a read-only, local cache.
	// You can use scheme.Copy() to make a deep copy of original object and modify this copy
	// Or create a copy manually for better performance.
	copyObj, err := s.crdClient.Scheme.DeepCopy(app)
	if err != nil {
		glog.Errorf("failed to create a deep copy of example object: %v\n", err)
		return
	}
	appCopy := copyObj.(*v1alpha1.SparkApplication)
	appCopy.Status.AppID = buildAppID(appCopy)
	appCopy.Status.State = v1alpha1.NewState
	appCopy.Status.RequestedExecutors = appCopy.Spec.Executor.Instances
	appCopy.Annotations = make(map[string]string)

	serviceName, err := createSparkUIService(appCopy, s.kubeClient)
	if err != nil {
		glog.Errorf("Failed to create a UI service for SparkApplication %s: %v", appCopy.Name, err)
	}
	appCopy.Annotations[SparkUIServiceNameAnnotationKey] = serviceName

	updatedApp, err := s.crdClient.Update(appCopy)
	if err != nil {
		glog.Errorf("Failed to update SparkApplication %s: %v", appCopy.Name, err)
	}

	submissionCmdArgs, err := buildSubmissionCommandArgs(updatedApp)
	if err != nil {
		glog.Errorf("Failed to build the submission command for SparkApplication %s: %v", updatedApp.Name, err)
	}
	if !updatedApp.Spec.SubmissionByUser {
		s.runner.submit(newSubmission(submissionCmdArgs, updatedApp))
	}
}

func (s *SparkApplicationController) onUpdate(oldObj, newObj interface{}) {
	newApp := newObj.(*v1alpha1.SparkApplication)
	glog.Infof("[CONTROLLER] OnUpdate %s\n", newApp.ObjectMeta.SelfLink)
}

func (s *SparkApplicationController) onDelete(obj interface{}) {
	app := obj.(*v1alpha1.SparkApplication)
	glog.Infof("[CONTROLLER] OnDelete %s\n", app.ObjectMeta.SelfLink)
	if serviceName, ok := app.Annotations[SparkUIServiceNameAnnotationKey]; ok {
		glog.Infof("Deleting the UI service %s for SparkApplication %s", serviceName, app.Name)
		err := s.kubeClient.CoreV1().Services(app.Namespace).Delete(serviceName, &metav1.DeleteOptions{})
		if err != nil {
			glog.Errorf("Failed to delete the UI service %s for SparkApplication %s: %v", serviceName, app.Name, err)
		}
	}
}

// buildAppID builds an application ID in the form of <application name>-<32-bit hash>.
func buildAppID(app *v1alpha1.SparkApplication) string {
	hasher := util.NewHash32()
	hasher.Write([]byte(app.Name))
	hasher.Write([]byte(app.Namespace))
	hasher.Write([]byte(app.UID))
	return fmt.Sprintf("%s-%d", app.Name, hasher.Sum32())
}
