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

package sparkapplication

import (
	"fmt"
	"net/url"
	"strconv"

	clientset "k8s.io/client-go/kubernetes"

	"github.com/kubeflow/spark-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/util"
)

const (
	sparkUIPortConfigurationKey        = "spark.ui.port"
	defaultSparkWebUIPort       int32  = 4040
	defaultSparkWebUIPortName   string = "spark-driver-ui-port"
)

func createSparkUIIngress(app *v1beta2.SparkApplication, service SparkService, ingressURL *url.URL, ingressClassName string, kubeClient clientset.Interface) (*SparkIngress, error) {
	ingressName := getDefaultUIIngressName(app)
	if util.IngressCapabilities.Has("networking.k8s.io/v1") {
		return createDriverIngress_v1(app, service, ingressName, ingressURL, ingressClassName, kubeClient)
	} else {
		return createDriverIngress_legacy(app, service, ingressName, ingressURL, kubeClient)
	}
}

func createSparkUIService(
	app *v1beta2.SparkApplication,
	kubeClient clientset.Interface) (*SparkService, error) {
	portName := getUIServicePortName(app)
	port, err := getUIServicePort(app)
	if err != nil {
		return nil, fmt.Errorf("invalid Spark UI servicePort: %d", port)
	}
	tPort, err := getUITargetPort(app)
	if err != nil {
		return nil, fmt.Errorf("invalid Spark UI targetPort: %d", tPort)
	}
	serviceName := getDefaultUIServiceName(app)
	serviceType := getUIServiceType(app)
	serviceAnnotations := getServiceAnnotations(app)
	serviceLabels := getServiceLabels(app)
	return createDriverIngressService(app, portName, port, tPort, serviceName, serviceType, serviceAnnotations, serviceLabels, kubeClient)
}

// getWebUITargetPort attempts to get the Spark web UI port from configuration property spark.ui.port
// in Spec.SparkConf if it is present, otherwise the default port is returned.
// Note that we don't attempt to get the port from Spec.SparkConfigMap.
func getUITargetPort(app *v1beta2.SparkApplication) (int32, error) {
	portStr, ok := app.Spec.SparkConf[sparkUIPortConfigurationKey]
	if ok {
		port, err := strconv.Atoi(portStr)
		return int32(port), err
	}
	return defaultSparkWebUIPort, nil
}

func getUIServicePort(app *v1beta2.SparkApplication) (int32, error) {
	if app.Spec.SparkUIOptions == nil {
		return getUITargetPort(app)
	}
	port := app.Spec.SparkUIOptions.ServicePort
	if port != nil {
		return *port, nil
	}
	return defaultSparkWebUIPort, nil
}

func getUIServicePortName(app *v1beta2.SparkApplication) string {
	if app.Spec.SparkUIOptions == nil {
		return defaultSparkWebUIPortName
	}
	portName := app.Spec.SparkUIOptions.ServicePortName
	if portName != nil {
		return *portName
	}
	return defaultSparkWebUIPortName
}
