package controller

import (
	"context"

	"github.com/golang/glog"
	"github.com/liyinan926/spark-operator/pkg/apis/v1alpha1"
	"github.com/liyinan926/spark-operator/pkg/config"
	"github.com/liyinan926/spark-operator/pkg/crd"
	"github.com/liyinan926/spark-operator/pkg/submission"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// SparkApplicationController manages instances of SparkApplication.
type SparkApplicationController struct {
	crdClient        *crd.Client
	kubeClient       clientset.Interface
	submissionClient *submission.SparkSubmissionClient
}

// NewSparkApplicationController creates a new SparkApplicationController.
func NewSparkApplicationController(crdClient *crd.Client, kubeClient clientset.Interface) *SparkApplicationController {
	return &SparkApplicationController{
		crdClient:  crdClient,
		kubeClient: kubeClient,
		submissionClient: &submission.SparkSubmissionClient{
			KubeClient: kubeClient,
		},
	}
}

// Run starts the SparkApplicationController by registering a watcher for SparkApplication objects.
func (s *SparkApplicationController) Run(ctx context.Context) error {
	_, err := s.watchSparkApplications(ctx)
	if err != nil {
		glog.Errorf("Failed to register watch for SparkApplication resource: %v\n", err)
		return err
	}

	<-ctx.Done()
	return ctx.Err()
}

func (s *SparkApplicationController) watchSparkApplications(ctx context.Context) (cache.Controller, error) {
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

	go controller.Run(ctx.Done())
	return controller, nil
}

// Callback function called when a new SparkApplication object gets created.
func (s *SparkApplicationController) onAdd(obj interface{}) {
	app := obj.(*v1alpha1.SparkApplication)
	glog.Infof("[CONTROLLER] OnAdd %s\n", app.ObjectMeta.SelfLink)

	// NEVER modify objects from the store. It's a read-only, local cache.
	// You can use scheme.Copy() to make a deep copy of original object and modify this copy
	// Or create a copy manually for better performance.
	copyObj, err := s.crdClient.Scheme.Copy(app)
	if err != nil {
		glog.Errorf("failed to create a deep copy of example object: %v\n", err)
		return
	}
	appCopy := copyObj.(*v1alpha1.SparkApplication)
	appCopy.Status = v1alpha1.SparkApplicationStatus{}

	if appCopy.Spec.SparkConfDir != nil {
		err = config.CreateSparkConfigMap(*appCopy.Spec.SparkConfDir, appCopy.Namespace, appCopy, s.kubeClient)
		if err != nil {
			glog.Errorf("failed to create a ConfigMap for Spark configuration at %s: %v\n", *appCopy.Spec.SparkConfDir, err)
			return
		}
	}
	if appCopy.Spec.HadoopConfigDir != nil {
		err = config.CreateSparkConfigMap(*appCopy.Spec.HadoopConfigDir, appCopy.Namespace, appCopy, s.kubeClient)
		if err != nil {
			glog.Errorf("failed to create a ConfigMap for Hadoop configuration at %s: %v\n", *appCopy.Spec.HadoopConfigDir, err)
			return
		}
	}

	s.crdClient.Update(appCopy, appCopy.Namespace)
}

func (s *SparkApplicationController) onUpdate(oldObj, newObj interface{}) {
	oldApp := oldObj.(*v1alpha1.SparkApplication)
	newApp := newObj.(*v1alpha1.SparkApplication)
	glog.Infof("[CONTROLLER] OnUpdate %s to %s\n", oldApp.ObjectMeta.SelfLink, newApp.ObjectMeta.SelfLink)
}

func (s *SparkApplicationController) onDelete(obj interface{}) {
	app := obj.(*v1alpha1.SparkApplication)
	glog.Infof("[CONTROLLER] OnDelete %s\n", app.ObjectMeta.SelfLink)
}
