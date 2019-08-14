package volcano

import (
	"fmt"
	"github.com/golang/glog"

	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	"volcano.sh/volcano/pkg/apis/scheduling/v1alpha2"
	volcanoclient "volcano.sh/volcano/pkg/client/clientset/versioned"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta1"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/controller/sparkapplication/batchscheduler/interface"
)

const (
	PodGroupName = "podgroups.scheduling.incubator.k8s.io"
)

type VolcanoBatchScheduler struct {
	extensionClient apiextensionsclient.Interface
	volcanoClient   volcanoclient.Interface
}

func GetPluginName() string {
	return "volcano"
}

func (v *VolcanoBatchScheduler) Name() string {
	return GetPluginName()
}

func (v *VolcanoBatchScheduler) ShouldSchedule(app *v1beta1.SparkApplication) bool {

	checkScheduler := func(scheduler *string) bool {
		return scheduler != nil && *scheduler == v.Name()
	}

	if app.Spec.Mode == v1beta1.ClientMode {
		return checkScheduler(app.Spec.Executor.SchedulerName)
	}
	if app.Spec.Mode == v1beta1.ClusterMode {
		return checkScheduler(app.Spec.Executor.SchedulerName) && checkScheduler(app.Spec.Driver.SchedulerName)
	}

	glog.Warningf("Unsupported Spark application mode %s, abandon schedule via volcano.", app.Spec.Mode)
	return false
}

func (v *VolcanoBatchScheduler) OnSubmitSparkApplication(app *v1beta1.SparkApplication) (*v1beta1.SparkApplication, error) {
	if app.Spec.Executor.Annotations == nil {
		app.Spec.Executor.Annotations = make(map[string]string)
	}

	if app.Spec.Driver.Annotations == nil {
		app.Spec.Driver.Annotations = make(map[string]string)
	}

	if app.Spec.Mode == v1beta1.ClientMode {
		return v.syncPodGroupForClientAPP(app)
	} else if app.Spec.Mode == v1beta1.ClusterMode {
		return v.syncPodGroupForClusterAPP(app)
	}
	return app, nil
}

func (v *VolcanoBatchScheduler) syncPodGroupForClientAPP(app *v1beta1.SparkApplication) (*v1beta1.SparkApplication, error) {
	//We only care about the executor pods in client mode
	if _, ok := app.Spec.Executor.Annotations[v1alpha2.GroupNameAnnotationKey]; !ok {
		if err := v.syncPodGroup(app, *app.Spec.Executor.Instances); err == nil {
			app.Spec.Executor.Annotations[v1alpha2.GroupNameAnnotationKey] = v.getAppPodGroupName(app)
		}
	}
	return app, nil
}

func (v *VolcanoBatchScheduler) syncPodGroupForClusterAPP(app *v1beta1.SparkApplication) (*v1beta1.SparkApplication, error) {
	//We need both mark Driver and Executor when submitting
	//NOTE: Although we only have one pod for Spark Driver, we still manage it into PodGroup,since it can
	//utilize other advanced features in volcano, for instance, namespace resource fairness.
	if _, ok := app.Spec.Driver.Annotations[v1alpha2.GroupNameAnnotationKey]; !ok {
		if err := v.syncPodGroup(app, 1); err != nil {
			app.Spec.Executor.Annotations[v1alpha2.GroupNameAnnotationKey] = v.getAppPodGroupName(app)
			app.Spec.Driver.Annotations[v1alpha2.GroupNameAnnotationKey] = v.getAppPodGroupName(app)
		}
	}
	return app, nil
}

func (v *VolcanoBatchScheduler) OnSparkDriverPodScheduled(app *v1beta1.SparkApplication) (*v1beta1.SparkApplication, error) {
	// Update PodGroup MinMember if required
	if app.Spec.Mode == v1beta1.ClusterMode {
		return app, v.syncPodGroup(app, 1+*(app.Spec.Executor.Instances))
	}
	return app, nil
}

func (v *VolcanoBatchScheduler) getAppPodGroupName(app *v1beta1.SparkApplication) string {
	return fmt.Sprintf("sparkapplication-%s-podgroup", app.Name)
}

func (v *VolcanoBatchScheduler) syncPodGroup(app *v1beta1.SparkApplication, size int32) error {
	var err error
	podGroupName := v.getAppPodGroupName(app)
	if pg, err := v.volcanoClient.SchedulingV1alpha2().PodGroups(app.Namespace).Get(podGroupName, v1.GetOptions{}); err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		podGroup := v1alpha2.PodGroup{
			ObjectMeta: v1.ObjectMeta{
				Namespace: app.Namespace,
				Name:      podGroupName,
				OwnerReferences: []v1.OwnerReference{
					*v1.NewControllerRef(app, v1beta1.SchemeGroupVersion.WithKind("SparkApplication")),
				},
			},
			Spec: v1alpha2.PodGroupSpec{
				MinMember: size,
			},
		}

		_, err = v.volcanoClient.SchedulingV1alpha2().PodGroups(app.Namespace).Create(&podGroup)
	} else {
		if pg.Spec.MinMember != size {
			pg.Spec.MinMember = size
			_, err = v.volcanoClient.SchedulingV1alpha2().PodGroups(app.Namespace).Update(pg)
		}
	}
	if err != nil {
		glog.Errorf(
			"Unable to sync PodGroup with error: %s. Abandon schedule pods via volcano.", err)
	}
	return err
}

func New(config *rest.Config) schedulerinterface.BatchScheduler {

	vkClient, err := volcanoclient.NewForConfig(config)
	if err != nil {
		glog.Errorf("Unable to initialize volcano client with error %v", err)
		return nil
	}
	extClient, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		glog.Errorf("Unable to initialize k8s extension client with error %v", err)
		return nil
	}

	if _, err := extClient.ApiextensionsV1beta1().CustomResourceDefinitions().Get(
		PodGroupName, v1.GetOptions{}); err != nil {
		glog.Errorf(
			"PodGroup CRD is required to exists in current cluster error: %s.", err)
		return nil
	}
	return &VolcanoBatchScheduler{
		extensionClient: extClient,
		volcanoClient:   vkClient,
	}
}
