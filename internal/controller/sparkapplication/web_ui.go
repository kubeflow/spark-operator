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

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
)

func (r *Reconciler) createWebUIService(app *v1beta2.SparkApplication) (*SparkService, error) {
	portName := getWebUIServicePortName(app)
	port, err := getWebUIServicePort(app)
	if err != nil {
		return nil, fmt.Errorf("invalid Spark UI servicePort: %d", port)
	}

	targetPort, err := getWebUITargetPort(app)
	if err != nil {
		return nil, fmt.Errorf("invalid Spark UI targetPort: %d", targetPort)
	}

	serviceName := util.GetDefaultUIServiceName(app)
	serviceType := util.GetWebUIServiceType(app)
	serviceLabels := util.GetWebUIServiceLabels(app)
	serviceAnnotations := util.GetWebUIServiceAnnotations(app)

	return r.createDriverIngressService(app, portName, port, targetPort, serviceName, serviceType, serviceAnnotations, serviceLabels)
}

func (r *Reconciler) createWebUIIngress(app *v1beta2.SparkApplication, service SparkService, ingressURL *url.URL, ingressClassName string) (*SparkIngress, error) {
	ingressName := util.GetDefaultUIIngressName(app)
	if util.IngressCapabilities.Has("networking.k8s.io/v1") {
		return r.createDriverIngressV1(app, service, ingressName, ingressURL, ingressClassName)
	}
	return r.createDriverIngressLegacy(app, service, ingressName, ingressURL)
}

func getWebUIServicePortName(app *v1beta2.SparkApplication) string {
	if app.Spec.SparkUIOptions == nil {
		return common.DefaultSparkWebUIPortName
	}
	portName := app.Spec.SparkUIOptions.ServicePortName
	if portName != nil {
		return *portName
	}
	return common.DefaultSparkWebUIPortName
}

func getWebUIServicePort(app *v1beta2.SparkApplication) (int32, error) {
	if app.Spec.SparkUIOptions == nil {
		return getWebUITargetPort(app)
	}
	port := app.Spec.SparkUIOptions.ServicePort
	if port != nil {
		return *port, nil
	}
	return common.DefaultSparkWebUIPort, nil
}

// getWebUITargetPort attempts to get the Spark web UI port from configuration property spark.ui.port
// in Spec.SparkConf if it is present, otherwise the default port is returned.
// Note that we don't attempt to get the port from Spec.SparkConfigMap.
func getWebUITargetPort(app *v1beta2.SparkApplication) (int32, error) {
	portStr, ok := app.Spec.SparkConf[common.SparkUIPortKey]
	if !ok {
		return common.DefaultSparkWebUIPort, nil
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return common.DefaultSparkWebUIPort, nil
	}
	return int32(port), nil
}
