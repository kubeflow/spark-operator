package controller

import (
	"fmt"
	"strconv"

	"github.com/golang/glog"
	"github.com/liyinan926/spark-operator/pkg/apis/v1alpha1"
	"github.com/liyinan926/spark-operator/pkg/config"
	"github.com/liyinan926/spark-operator/pkg/util"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientset "k8s.io/client-go/kubernetes"
)

const (
	sparkUIPortConfigurationKey = "spark.ui.port"
	defaultSparkWebUIPort       = "4040"
	sparkRoleLabel              = "spark-role"
	sparkDriverRole             = "driver"
)

func createSparkUIService(app *v1alpha1.SparkApplication, kubeClient clientset.Interface) error {
	portStr := getUITargetPort(app)
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return err
	}
	service := &apiv1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      buildUIServiceName(app),
			Namespace: app.Namespace,
			Labels:    map[string]string{},
		},
		Spec: apiv1.ServiceSpec{
			Ports: []apiv1.ServicePort{
				apiv1.ServicePort{
					Port:       int32(port),
					TargetPort: intstr.FromInt(port),
				},
			},
			Selector: map[string]string{
				config.SparkAppIDLabel: app.Status.AppID,
				sparkRoleLabel:         sparkDriverRole,
			},
			Type: apiv1.ServiceTypeNodePort,
		},
	}

	glog.Infof("Creating a service %s for the Spark UI for application %s", service.Name, app.Name)
	service, err = kubeClient.CoreV1().Services(app.Namespace).Create(service)
	if err != nil {
		return err
	}
	app.Status.UIServiceInfo = v1alpha1.UIServiceInfo{
		Name: service.Name,
		Port: int32(port),
	}

	return nil
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
	hasher.Write([]byte(app.Name))
	hasher.Write([]byte(app.Namespace))
	hasher.Write([]byte(app.UID))
	return fmt.Sprintf("%s-ui-%d", app.Name, hasher.Sum32())
}
