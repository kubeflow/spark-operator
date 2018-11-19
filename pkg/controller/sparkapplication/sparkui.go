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
	"regexp"
	"strconv"

	"github.com/golang/glog"

	apiv1 "k8s.io/api/core/v1"
	extensions "k8s.io/api/extensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
)

const (
	sparkUIPortConfigurationKey = "spark.ui.port"
	defaultSparkWebUIPort       = "4040"
)

var ingressUrlRegex = regexp.MustCompile("{{\\s*[$]appName\\s*}}")

func getSparkUIIngressURL(ingressUrlFormat string, appName string) string {
	return ingressUrlRegex.ReplaceAllString(ingressUrlFormat, appName)
}

// Struct to encapsulate service
type SparkService struct {
	serviceName string
	servicePort int32
	nodePort    int32
}

// Struct to encapsulate SparkIngress
type SparkIngress struct {
	ingressName string
	ingressUrl  string
}

func createSparkUIIngress(app *v1alpha1.SparkApplication, service SparkService, ingressUrlFormat string, kubeClient clientset.Interface) (*SparkIngress, error) {
	ingressUrl := getSparkUIIngressURL(ingressUrlFormat, app.GetName())
	ingress := extensions.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getDefaultUIIngressName(app),
			Namespace: app.Namespace,
			Labels: map[string]string{
				config.SparkAppNameLabel: app.Name,
			},
			OwnerReferences: []metav1.OwnerReference{*getOwnerReference(app)},
		},
		Spec: extensions.IngressSpec{
			Rules: []extensions.IngressRule{{
				Host: ingressUrl,
				IngressRuleValue: extensions.IngressRuleValue{
					HTTP: &extensions.HTTPIngressRuleValue{
						Paths: []extensions.HTTPIngressPath{{
							Backend: extensions.IngressBackend{
								ServiceName: service.serviceName,
								ServicePort: intstr.IntOrString{
									Type:   intstr.Int,
									IntVal: service.servicePort,
								},
							},
						}},
					},
				},
			}},
		},
	}
	glog.Infof("Creating an Ingress %s for the Spark UI for application %s", ingress.Name, app.Name)
	_, err := kubeClient.ExtensionsV1beta1().Ingresses(ingress.Namespace).Create(&ingress)

	if err != nil {
		return nil, err
	}
	return &SparkIngress{
		ingressName: ingress.Name,
		ingressUrl:  ingressUrl,
	}, nil
}

func createSparkUIService(
	app *v1alpha1.SparkApplication,
	kubeClient clientset.Interface) (*SparkService, error) {
	portStr := getUITargetPort(app)
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("invalid Spark UI port: %s", portStr)
	}

	service := &apiv1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getDefaultUIServiceName(app),
			Namespace: app.Namespace,
			Labels: map[string]string{
				config.SparkAppNameLabel: app.Name,
			},
			OwnerReferences: []metav1.OwnerReference{*getOwnerReference(app)},
		},
		Spec: apiv1.ServiceSpec{
			Ports: []apiv1.ServicePort{
				{
					Name: "spark-driver-ui-port",
					Port: int32(port),
				},
			},
			Selector: map[string]string{
				config.SparkAppNameLabel: app.Name,
				config.SparkRoleLabel:    sparkDriverRole,
			},
			Type: apiv1.ServiceTypeNodePort,
		},
	}

	glog.Infof("Creating a service %s for the Spark UI for application %s", service.Name, app.Name)
	service, err = kubeClient.CoreV1().Services(app.Namespace).Create(service)
	if err != nil {
		return nil, err
	}

	return &SparkService{
		serviceName: service.Name,
		servicePort: int32(port),
		nodePort:    service.Spec.Ports[0].NodePort,
	}, nil
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
