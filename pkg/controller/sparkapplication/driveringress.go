/*
Copyright 2024 spark-operator contributors

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
	"github.com/golang/glog"
	"net/url"
	"regexp"

	apiv1 "k8s.io/api/core/v1"
	extensions "k8s.io/api/extensions/v1beta1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/kubeflow/spark-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/config"
	"github.com/kubeflow/spark-operator/pkg/util"
)

// SparkService encapsulates information about the driver UI service.
type SparkService struct {
	serviceName        string
	serviceType        apiv1.ServiceType
	servicePort        int32
	servicePortName    string
	targetPort         intstr.IntOrString
	serviceIP          string
	serviceAnnotations map[string]string
	serviceLabels      map[string]string
}

// SparkIngress encapsulates information about the driver UI ingress.
type SparkIngress struct {
	ingressName      string
	ingressURL       *url.URL
	ingressClassName string
	annotations      map[string]string
	ingressTLS       []networkingv1.IngressTLS
}

var ingressAppNameURLRegex = regexp.MustCompile("{{\\s*[$]appName\\s*}}")
var ingressAppNamespaceURLRegex = regexp.MustCompile("{{\\s*[$]appNamespace\\s*}}")

func getDriverIngressURL(ingressURLFormat string, appName string, appNamespace string) (*url.URL, error) {
	ingressURL := ingressAppNamespaceURLRegex.ReplaceAllString(ingressAppNameURLRegex.ReplaceAllString(ingressURLFormat, appName), appNamespace)
	parsedURL, err := url.Parse(ingressURL)
	if err != nil {
		return nil, err
	}
	if parsedURL.Scheme == "" {
		//url does not contain any scheme, adding http:// so url.Parse can function correctly
		parsedURL, err = url.Parse("http://" + ingressURL)
		if err != nil {
			return nil, err
		}
	}
	return parsedURL, nil
}

func createDriverIngress(app *v1beta2.SparkApplication, driverIngressConfiguration *v1beta2.DriverIngressConfiguration, service SparkService, ingressURL *url.URL, ingressClassName string, kubeClient clientset.Interface) (*SparkIngress, error) {
	if driverIngressConfiguration.ServicePort == nil {
		return nil, fmt.Errorf("cannot create Driver Ingress for application %s/%s due to empty ServicePort on driverIngressConfiguration", app.Namespace, app.Name)
	}
	ingressName := fmt.Sprintf("%s-ing-%d", app.Name, *driverIngressConfiguration.ServicePort)
	if util.IngressCapabilities.Has("networking.k8s.io/v1") {
		return createDriverIngress_v1(app, service, ingressName, ingressURL, ingressClassName, kubeClient)
	} else {
		return createDriverIngress_legacy(app, service, ingressName, ingressURL, kubeClient)
	}
}

func createDriverIngress_v1(app *v1beta2.SparkApplication, service SparkService, ingressName string, ingressURL *url.URL, ingressClassName string, kubeClient clientset.Interface) (*SparkIngress, error) {
	ingressResourceAnnotations := getIngressResourceAnnotations(app)
	ingressTlsHosts := getIngressTlsHosts(app)

	ingressURLPath := ingressURL.Path
	// If we're serving on a subpath, we need to ensure we create capture groups
	if ingressURLPath != "" && ingressURLPath != "/" {
		ingressURLPath = ingressURLPath + "(/|$)(.*)"
	}

	implementationSpecific := networkingv1.PathTypeImplementationSpecific

	ingress := networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:            ingressName,
			Namespace:       app.Namespace,
			Labels:          getResourceLabels(app),
			OwnerReferences: []metav1.OwnerReference{*getOwnerReference(app)},
		},
		Spec: networkingv1.IngressSpec{
			Rules: []networkingv1.IngressRule{{
				Host: ingressURL.Host,
				IngressRuleValue: networkingv1.IngressRuleValue{
					HTTP: &networkingv1.HTTPIngressRuleValue{
						Paths: []networkingv1.HTTPIngressPath{{
							Backend: networkingv1.IngressBackend{
								Service: &networkingv1.IngressServiceBackend{
									Name: service.serviceName,
									Port: networkingv1.ServiceBackendPort{
										Number: service.servicePort,
									},
								},
							},
							Path:     ingressURLPath,
							PathType: &implementationSpecific,
						}},
					},
				},
			}},
		},
	}

	if len(ingressResourceAnnotations) != 0 {
		ingress.ObjectMeta.Annotations = ingressResourceAnnotations
	}

	// If we're serving on a subpath, we need to ensure we use the capture groups
	if ingressURL.Path != "" && ingressURL.Path != "/" {
		if ingress.ObjectMeta.Annotations == nil {
			ingress.ObjectMeta.Annotations = make(map[string]string)
		}
		ingress.ObjectMeta.Annotations["nginx.ingress.kubernetes.io/rewrite-target"] = "/$2"
	}
	if len(ingressTlsHosts) != 0 {
		ingress.Spec.TLS = ingressTlsHosts
	}
	if len(ingressClassName) != 0 {
		ingress.Spec.IngressClassName = &ingressClassName
	}

	glog.Infof("Creating an Ingress %s for the Spark UI for application %s", ingress.Name, app.Name)
	_, err := kubeClient.NetworkingV1().Ingresses(ingress.Namespace).Create(context.TODO(), &ingress, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	return &SparkIngress{
		ingressName:      ingress.Name,
		ingressURL:       ingressURL,
		ingressClassName: ingressClassName,
		annotations:      ingress.Annotations,
		ingressTLS:       ingressTlsHosts,
	}, nil
}

func createDriverIngress_legacy(app *v1beta2.SparkApplication, service SparkService, ingressName string, ingressURL *url.URL, kubeClient clientset.Interface) (*SparkIngress, error) {
	ingressResourceAnnotations := getIngressResourceAnnotations(app)
	// var ingressTlsHosts networkingv1.IngressTLS[]
	// That we convert later for extensionsv1beta1, but return as is in SparkIngress
	ingressTlsHosts := getIngressTlsHosts(app)

	ingressURLPath := ingressURL.Path
	// If we're serving on a subpath, we need to ensure we create capture groups
	if ingressURLPath != "" && ingressURLPath != "/" {
		ingressURLPath = ingressURLPath + "(/|$)(.*)"
	}

	ingress := extensions.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:            ingressName,
			Namespace:       app.Namespace,
			Labels:          getResourceLabels(app),
			OwnerReferences: []metav1.OwnerReference{*getOwnerReference(app)},
		},
		Spec: extensions.IngressSpec{
			Rules: []extensions.IngressRule{{
				Host: ingressURL.Host,
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
							Path: ingressURLPath,
						}},
					},
				},
			}},
		},
	}

	if len(ingressResourceAnnotations) != 0 {
		ingress.ObjectMeta.Annotations = ingressResourceAnnotations
	}

	// If we're serving on a subpath, we need to ensure we use the capture groups
	if ingressURL.Path != "" && ingressURL.Path != "/" {
		if ingress.ObjectMeta.Annotations == nil {
			ingress.ObjectMeta.Annotations = make(map[string]string)
		}
		ingress.ObjectMeta.Annotations["nginx.ingress.kubernetes.io/rewrite-target"] = "/$2"
	}
	if len(ingressTlsHosts) != 0 {
		ingress.Spec.TLS = convertIngressTlsHostsToLegacy(ingressTlsHosts)
	}
	glog.Infof("Creating an extensions/v1beta1 Ingress %s for application %s", ingress.Name, app.Name)
	_, err := kubeClient.ExtensionsV1beta1().Ingresses(ingress.Namespace).Create(context.TODO(), &ingress, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	return &SparkIngress{
		ingressName: ingress.Name,
		ingressURL:  ingressURL,
		annotations: ingress.Annotations,
		ingressTLS:  ingressTlsHosts,
	}, nil
}

func convertIngressTlsHostsToLegacy(ingressTlsHosts []networkingv1.IngressTLS) []extensions.IngressTLS {
	var ingressTlsHosts_legacy []extensions.IngressTLS
	for _, ingressTlsHost := range ingressTlsHosts {
		ingressTlsHosts_legacy = append(ingressTlsHosts_legacy, extensions.IngressTLS{
			Hosts:      ingressTlsHost.Hosts,
			SecretName: ingressTlsHost.SecretName,
		})
	}
	return ingressTlsHosts_legacy
}

func createDriverIngressService(
	app *v1beta2.SparkApplication,
	portName string,
	port int32,
	targetPort int32,
	serviceName string,
	serviceType apiv1.ServiceType,
	serviceAnnotations map[string]string,
	serviceLabels map[string]string,
	kubeClient clientset.Interface) (*SparkService, error) {
	service := &apiv1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            serviceName,
			Namespace:       app.Namespace,
			Labels:          getResourceLabels(app),
			OwnerReferences: []metav1.OwnerReference{*getOwnerReference(app)},
		},
		Spec: apiv1.ServiceSpec{
			Ports: []apiv1.ServicePort{
				{
					Name: portName,
					Port: port,
					TargetPort: intstr.IntOrString{
						Type:   intstr.Int,
						IntVal: targetPort,
					},
				},
			},
			Selector: map[string]string{
				config.SparkAppNameLabel: app.Name,
				config.SparkRoleLabel:    config.SparkDriverRole,
			},
			Type: serviceType,
		},
	}

	if len(serviceAnnotations) != 0 {
		service.ObjectMeta.Annotations = serviceAnnotations
	}

	if len(serviceLabels) != 0 {
		glog.Infof("Creating a service labels %s for the Driver Ingress: %v", service.Name, &serviceLabels)
		service.ObjectMeta.Labels = serviceLabels
	}

	glog.Infof("Creating a service %s for the Driver Ingress for application %s", service.Name, app.Name)
	service, err := kubeClient.CoreV1().Services(app.Namespace).Create(context.TODO(), service, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}

	return &SparkService{
		serviceName:        service.Name,
		serviceType:        service.Spec.Type,
		servicePort:        service.Spec.Ports[0].Port,
		servicePortName:    service.Spec.Ports[0].Name,
		targetPort:         service.Spec.Ports[0].TargetPort,
		serviceIP:          service.Spec.ClusterIP,
		serviceAnnotations: serviceAnnotations,
		serviceLabels:      serviceLabels,
	}, nil
}

func getDriverIngressServicePort(driverIngressConfiguration *v1beta2.DriverIngressConfiguration) (int32, error) {
	port := driverIngressConfiguration.ServicePort
	if port == nil {
		return 0, fmt.Errorf("servie port is nil on driver ingress configuration")
	}
	return *port, nil
}

func getDriverIngressServicePortName(driverIngressConfiguration *v1beta2.DriverIngressConfiguration) string {
	portName := driverIngressConfiguration.ServicePortName
	if portName != nil {
		return *portName
	}
	port := 0
	if driverIngressConfiguration.ServicePort != nil {
		port = int(*driverIngressConfiguration.ServicePort)
	}
	return fmt.Sprintf("driver-ing-%d", port)
}

func getDriverIngressServiceName(app *v1beta2.SparkApplication, port int32) string {
	return fmt.Sprintf("%s-driver-%d", app.Name, port)
}

func getDriverIngressServiceType(driverIngressConfiguration *v1beta2.DriverIngressConfiguration) apiv1.ServiceType {
	if driverIngressConfiguration.ServiceType != nil {
		return *driverIngressConfiguration.ServiceType
	}
	return apiv1.ServiceTypeClusterIP
}

func getDriverIngressServiceAnnotations(driverIngressConfiguration *v1beta2.DriverIngressConfiguration) map[string]string {
	serviceAnnotations := map[string]string{}
	if driverIngressConfiguration.ServiceAnnotations != nil {
		for key, value := range driverIngressConfiguration.ServiceAnnotations {
			serviceAnnotations[key] = value
		}
	}
	return serviceAnnotations
}

func getDriverIngressServiceLabels(driverIngressConfiguration *v1beta2.DriverIngressConfiguration) map[string]string {
	serviceLabels := map[string]string{}
	if driverIngressConfiguration.ServiceLabels != nil {
		for key, value := range driverIngressConfiguration.ServiceLabels {
			serviceLabels[key] = value
		}
	}
	return serviceLabels
}

func createDriverIngressServiceFromConfiguration(
	app *v1beta2.SparkApplication,
	driverIngressConfiguration *v1beta2.DriverIngressConfiguration,
	kubeClient clientset.Interface) (*SparkService, error) {
	portName := getDriverIngressServicePortName(driverIngressConfiguration)
	port, err := getDriverIngressServicePort(driverIngressConfiguration)
	if err != nil {
		return nil, err
	}
	serviceName := getDriverIngressServiceName(app, port)
	serviceType := getDriverIngressServiceType(driverIngressConfiguration)
	serviceAnnotations := getDriverIngressServiceAnnotations(driverIngressConfiguration)
	serviceLabels := getDriverIngressServiceLabels(driverIngressConfiguration)
	return createDriverIngressService(app, portName, port, port, serviceName, serviceType, serviceAnnotations, serviceLabels, kubeClient)
}
