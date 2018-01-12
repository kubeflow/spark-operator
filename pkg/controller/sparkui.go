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
	"strconv"

	"github.com/golang/glog"

	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	"k8s.io/spark-on-k8s-operator/pkg/config"
	"k8s.io/spark-on-k8s-operator/pkg/util"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
)

const (
	sparkUIPortConfigurationKey = "spark.ui.port"
	defaultSparkWebUIPort       = "4040"
)

func createSparkUIService(app *v1alpha1.SparkApplication, kubeClient clientset.Interface) {
	portStr := getUITargetPort(app)
	port, err := strconv.Atoi(portStr)
	if err != nil {
		glog.Errorf("invalid Spark UI port: %s", portStr)
		return
	}

	service := &apiv1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      buildUIServiceName(app),
			Namespace: app.Namespace,
			Labels: map[string]string{
				config.SparkAppIDLabel: app.Status.AppID,
			},
			OwnerReferences: []metav1.OwnerReference{getOwnerReference(app)},
		},
		Spec: apiv1.ServiceSpec{
			Ports: []apiv1.ServicePort{
				{
					Name: "spark-driver-ui-port",
					Port: int32(port),
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
		glog.Errorf("failed to create a UI service for SparkApplication %s: %v", app.Name, err)
		return
	}

	app.Status.DriverInfo = v1alpha1.DriverInfo{
		WebUIServiceName: service.Name,
		WebUIPort:        service.Spec.Ports[0].NodePort,
	}
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
	hasher.Write([]byte(app.Status.AppID))
	return fmt.Sprintf("%s-ui-%d", app.Name, hasher.Sum32())
}
