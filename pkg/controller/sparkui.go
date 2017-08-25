package controller

import (
	"fmt"

	"github.com/golang/glog"
	"github.com/liyinan926/spark-operator/pkg/apis/v1alpha1"
	"github.com/liyinan926/spark-operator/pkg/util"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
)

const (
	sparkUIPortConfigurationKey = "spark.ui.port"
	defaultSparkWebUIPort       = "4040"
)

func createSparkUIService(app *v1alpha1.SparkApplication, kubeClient clientset.Interface) error {
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

	glog.Infof("Creating a service %s for the Spark UI for application %s", service.Name, app.Name)
	service, err := s.kubeClient.CoreV1().Services(app.Namespace).Create(service)
	if err != nil {
		return err
	}
	app.Status.WebUIServiceName = service.Name
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

// buildUIServiceName builds a unique name of the service for the Spark UI.
func buildUIServiceName(app *v1alpha1.SparkApplication) string {
	hasher := util.NewHash32()
	hasher.Write(app.Name)
	hasher.Write(app.Namespace)
	hasher.Write(app.UID)
	return fmt.Sprintf("%s-%d", app.Name, hasher.Sum32())
}
