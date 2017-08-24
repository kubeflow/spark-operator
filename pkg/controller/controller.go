package controller

import (
	"context"
	"strconv"

	"github.com/golang/glog"
	"github.com/liyinan926/spark-operator/pkg/apis/v1alpha1"
	"github.com/liyinan926/spark-operator/pkg/crd"
	"github.com/liyinan926/spark-operator/pkg/submission"
	"github.com/liyinan926/spark-operator/pkg/util"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const (
	sparkUIPortConfigurationKey = "spark.ui.port"
	defaultSparkWebUIPort       = "4040"
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
	copyObj, err := s.crdClient.Scheme.DeepCopy(app)
	if err != nil {
		glog.Errorf("failed to create a deep copy of example object: %v\n", err)
		return
	}
	appCopy := copyObj.(*v1alpha1.SparkApplication)
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

func (s *SparkApplicationController) createServiceForSparkUI(app *v1alpha1.SparkApplication) error {
	port := getUITargetPort(app)
	service := apiv1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      buildUIServiceName(app),
			Namespace: app.Namespace,
			Labels:    map[string]string{},
		},
		Spec: apiv1.ServiceSpec{
			Ports: []apiv1.ServicePort{
				apiv1.ServicePort{
					Port:       strconv.Atoi(port),
					TargetPort: port,
				},
			},
			Selector: map[string]string{},
		},
	}

	service, err := s.kubeClient.CoreV1().Services(app.Namespace).Create(service)
	if err != nil {
		return err
	}
	app.Status.WebUIServiceName = service.Name
}

func buildUIServiceName(app *v1alpha1.SparkApplication) string {
	hasher := util.NewHash32()
	hasher.Write(app.Name)
	hasher.Write(app.Namespace)
	hasher.Write(app.UID)
	return fmt.Sprintf("%s-%d", app.Name, hasher.Sum32())
}

// getWebUITargetPort attempts to get the Spark web UI port from configuration property spark.ui.port
// in Spec.SparkConf if it is present, otherwise the default port is returned.
// Note that we don't attempt to get the port from Spec.SparkConfigMap.
func getUITargetPort(app *v1alpha1.SparkApplication) string {
	port, ok := app.Spec.SparkConf[sparkUIPortConfigurationKey]
	if ok {
		return port
	}
	return defaultSparkWebUIPort
}
