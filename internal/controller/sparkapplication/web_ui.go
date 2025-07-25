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
	"context"
	"fmt"
	"net/url"
	"strconv"

	networkingv1 "k8s.io/api/networking/v1"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

func (r *Reconciler) configWebUI(_ context.Context, app *v1beta2.SparkApplication) error {
	if !r.options.EnableUIService || r.options.IngressURLFormat == "" {
		return nil
	}

	ingressURL, err := getDriverIngressURL(r.options.IngressURLFormat, app)
	if err != nil {
		return fmt.Errorf("failed to get ingress url: %v", err)
	}
	if ingressURL.Path == "" {
		return nil
	}

	if app.Spec.SparkConf == nil {
		app.Spec.SparkConf = make(map[string]string)
	}
	util.SetIfNotExists(app.Spec.SparkConf, common.SparkUIProxyBase, ingressURL.Path)
	util.SetIfNotExists(app.Spec.SparkConf, common.SparkUIProxyRedirectURI, "/")
	return nil
}

func (r *Reconciler) createWebUIService(ctx context.Context, app *v1beta2.SparkApplication) (*SparkService, error) {
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

	return r.createDriverIngressService(ctx, app, portName, port, targetPort, serviceName, serviceType, serviceAnnotations, serviceLabels)
}

func (r *Reconciler) createWebUIIngress(ctx context.Context, app *v1beta2.SparkApplication, service SparkService, ingressURL *url.URL, ingressClassName string, defaultIngressTLS []networkingv1.IngressTLS, defaultIngressAnnotations map[string]string) (*SparkIngress, error) {
	ingressName := util.GetDefaultUIIngressName(app)
	if util.IngressCapabilities.Has("networking.k8s.io/v1") {
		return r.createDriverIngressV1(ctx, app, service, ingressName, ingressURL, ingressClassName, defaultIngressTLS, defaultIngressAnnotations)
	}
	return r.createDriverIngressLegacy(ctx, app, service, ingressName, ingressURL)
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
