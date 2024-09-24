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
	"net/url"
	"regexp"

	corev1 "k8s.io/api/core/v1"
	extensionsv1beta1 "k8s.io/api/extensions/v1beta1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
)

// SparkService encapsulates information about the driver UI service.
type SparkService struct {
	serviceName        string
	serviceType        corev1.ServiceType
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

var ingressAppNameURLRegex = regexp.MustCompile(`{{\s*[$]appName\s*}}`)
var ingressAppNamespaceURLRegex = regexp.MustCompile(`{{\s*[$]appNamespace\s*}}`)

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

func (r *Reconciler) createDriverIngress(app *v1beta2.SparkApplication, driverIngressConfiguration *v1beta2.DriverIngressConfiguration, service SparkService, ingressURL *url.URL, ingressClassName string) (*SparkIngress, error) {
	if driverIngressConfiguration.ServicePort == nil {
		return nil, fmt.Errorf("cannot create Driver Ingress for application %s/%s due to empty ServicePort on driverIngressConfiguration", app.Namespace, app.Name)
	}
	ingressName := fmt.Sprintf("%s-ing-%d", app.Name, *driverIngressConfiguration.ServicePort)
	if util.IngressCapabilities.Has("networking.k8s.io/v1") {
		return r.createDriverIngressV1(app, service, ingressName, ingressURL, ingressClassName)
	}
	return r.createDriverIngressLegacy(app, service, ingressName, ingressURL)
}

func (r *Reconciler) createDriverIngressV1(app *v1beta2.SparkApplication, service SparkService, ingressName string, ingressURL *url.URL, ingressClassName string) (*SparkIngress, error) {
	ingressResourceAnnotations := util.GetWebUIIngressAnnotations(app)
	ingressTLSHosts := util.GetWebUIIngressTLS(app)

	ingressURLPath := ingressURL.Path
	// If we're serving on a subpath, we need to ensure we create capture groups
	if ingressURLPath != "" && ingressURLPath != "/" {
		ingressURLPath = ingressURLPath + "(/|$)(.*)"
	}

	implementationSpecific := networkingv1.PathTypeImplementationSpecific

	ingress := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:            ingressName,
			Namespace:       app.Namespace,
			Labels:          util.GetResourceLabels(app),
			OwnerReferences: []metav1.OwnerReference{util.GetOwnerReference(app)},
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
	if len(ingressTLSHosts) != 0 {
		ingress.Spec.TLS = ingressTLSHosts
	}
	if len(ingressClassName) != 0 {
		ingress.Spec.IngressClassName = &ingressClassName
	}

	logger.Info("Creating networking.v1/Ingress for SparkApplication web UI", "name", app.Name, "namespace", app.Namespace, "ingressName", ingress.Name)
	if err := r.client.Create(context.TODO(), ingress); err != nil {
		return nil, fmt.Errorf("failed to create ingress %s/%s: %v", ingress.Namespace, ingress.Name, err)
	}
	return &SparkIngress{
		ingressName:      ingress.Name,
		ingressURL:       ingressURL,
		ingressClassName: ingressClassName,
		annotations:      ingress.Annotations,
		ingressTLS:       ingressTLSHosts,
	}, nil
}

func (r *Reconciler) createDriverIngressLegacy(app *v1beta2.SparkApplication, service SparkService, ingressName string, ingressURL *url.URL) (*SparkIngress, error) {
	ingressResourceAnnotations := util.GetWebUIIngressAnnotations(app)
	// var ingressTLSHosts networkingv1.IngressTLS[]
	// That we convert later for extensionsv1beta1, but return as is in SparkIngress.
	ingressTLSHosts := util.GetWebUIIngressTLS(app)

	ingressURLPath := ingressURL.Path
	// If we're serving on a subpath, we need to ensure we create capture groups.
	if ingressURLPath != "" && ingressURLPath != "/" {
		ingressURLPath = ingressURLPath + "(/|$)(.*)"
	}

	ingress := &extensionsv1beta1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:            ingressName,
			Namespace:       app.Namespace,
			Labels:          util.GetResourceLabels(app),
			OwnerReferences: []metav1.OwnerReference{util.GetOwnerReference(app)},
		},
		Spec: extensionsv1beta1.IngressSpec{
			Rules: []extensionsv1beta1.IngressRule{{
				Host: ingressURL.Host,
				IngressRuleValue: extensionsv1beta1.IngressRuleValue{
					HTTP: &extensionsv1beta1.HTTPIngressRuleValue{
						Paths: []extensionsv1beta1.HTTPIngressPath{{
							Backend: extensionsv1beta1.IngressBackend{
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
	if len(ingressTLSHosts) != 0 {
		ingress.Spec.TLS = convertIngressTLSHostsToLegacy(ingressTLSHosts)
	}
	logger.Info("Creating extensions.v1beta1/Ingress for SparkApplication web UI", "name", app.Name, "namespace", app.Namespace, "ingressName", ingress.Name)
	if err := r.client.Create(context.TODO(), ingress); err != nil {
		return nil, fmt.Errorf("failed to create ingress %s/%s: %v", ingress.Namespace, ingress.Name, err)
	}
	return &SparkIngress{
		ingressName: ingress.Name,
		ingressURL:  ingressURL,
		annotations: ingress.Annotations,
		ingressTLS:  ingressTLSHosts,
	}, nil
}

func convertIngressTLSHostsToLegacy(ingressTLSHosts []networkingv1.IngressTLS) []extensionsv1beta1.IngressTLS {
	var ingressTLSHostsLegacy []extensionsv1beta1.IngressTLS
	for _, ingressTLSHost := range ingressTLSHosts {
		ingressTLSHostsLegacy = append(ingressTLSHostsLegacy, extensionsv1beta1.IngressTLS{
			Hosts:      ingressTLSHost.Hosts,
			SecretName: ingressTLSHost.SecretName,
		})
	}
	return ingressTLSHostsLegacy
}

func (r *Reconciler) createDriverIngressService(
	app *v1beta2.SparkApplication,
	portName string,
	port int32,
	targetPort int32,
	serviceName string,
	serviceType corev1.ServiceType,
	serviceAnnotations map[string]string,
	serviceLabels map[string]string,
) (*SparkService, error) {
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            serviceName,
			Namespace:       app.Namespace,
			Labels:          util.GetResourceLabels(app),
			OwnerReferences: []metav1.OwnerReference{util.GetOwnerReference(app)},
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
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
				common.LabelSparkAppName: app.Name,
				common.LabelSparkRole:    common.SparkRoleDriver,
			},
			Type: serviceType,
		},
	}

	if len(serviceLabels) != 0 {
		service.ObjectMeta.Labels = serviceLabels
	}

	if len(serviceAnnotations) != 0 {
		service.ObjectMeta.Annotations = serviceAnnotations
	}

	if err := r.client.Create(context.TODO(), service); err != nil {
		if !errors.IsAlreadyExists(err) {
			return nil, err
		}

		// Update the service if it already exists.
		if err := r.client.Update(context.TODO(), service); err != nil {
			return nil, err
		}
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
		return 0, fmt.Errorf("service port is nil on driver ingress configuration")
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

func getDriverIngressServiceType(driverIngressConfiguration *v1beta2.DriverIngressConfiguration) corev1.ServiceType {
	if driverIngressConfiguration.ServiceType != nil {
		return *driverIngressConfiguration.ServiceType
	}
	return corev1.ServiceTypeClusterIP
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

func (r *Reconciler) createDriverIngressServiceFromConfiguration(
	app *v1beta2.SparkApplication,
	driverIngressConfiguration *v1beta2.DriverIngressConfiguration,
) (*SparkService, error) {
	portName := getDriverIngressServicePortName(driverIngressConfiguration)
	port, err := getDriverIngressServicePort(driverIngressConfiguration)
	if err != nil {
		return nil, err
	}
	serviceName := getDriverIngressServiceName(app, port)
	serviceType := getDriverIngressServiceType(driverIngressConfiguration)
	serviceAnnotations := getDriverIngressServiceAnnotations(driverIngressConfiguration)
	serviceLabels := getDriverIngressServiceLabels(driverIngressConfiguration)
	return r.createDriverIngressService(app, portName, port, port, serviceName, serviceType, serviceAnnotations, serviceLabels)
}
