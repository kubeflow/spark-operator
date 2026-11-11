/*
Copyright The Kubeflow Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package webhook

import (
	"slices"

	corev1 "k8s.io/api/core/v1"
	apivalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

// supportedServiceTypes are the Service types the controller can create, since it never sets spec.externalName.
var supportedServiceTypes = []corev1.ServiceType{
	corev1.ServiceTypeClusterIP,
	corev1.ServiceTypeNodePort,
	corev1.ServiceTypeLoadBalancer,
}

// validateServiceOptions rejects Service fields that the API server would refuse when the controller creates the web UI and driver ingress Services.
func validateServiceOptions(spec *v1beta2.SparkApplicationSpec, root *field.Path) error {
	var errs field.ErrorList
	if ui := spec.SparkUIOptions; ui != nil {
		errs = append(errs, validateServiceFields(root.Child("sparkUIOptions"), ui.ServicePort, ui.ServicePortName, ui.ServiceType, ui.ServiceLabels, ui.ServiceAnnotations)...)
	}
	for i, opt := range spec.DriverIngressOptions {
		errs = append(errs, validateServiceFields(root.Child("driverIngressOptions").Index(i), opt.ServicePort, opt.ServicePortName, opt.ServiceType, opt.ServiceLabels, opt.ServiceAnnotations)...)
	}
	return errs.ToAggregate()
}

func validateServiceFields(path *field.Path, port *int32, portName *string, serviceType *corev1.ServiceType, labels, annotations map[string]string) field.ErrorList {
	var errs field.ErrorList
	if port != nil {
		for _, msg := range validation.IsValidPortNum(int(*port)) {
			errs = append(errs, field.Invalid(path.Child("servicePort"), *port, msg))
		}
	}
	if portName != nil && *portName != "" {
		for _, msg := range validation.IsDNS1123Label(*portName) {
			errs = append(errs, field.Invalid(path.Child("servicePortName"), *portName, msg))
		}
	}
	if serviceType != nil && !slices.Contains(supportedServiceTypes, *serviceType) {
		errs = append(errs, field.NotSupported(path.Child("serviceType"), *serviceType, supportedServiceTypes))
	}
	errs = append(errs, metav1validation.ValidateLabels(labels, path.Child("serviceLabels"))...)
	errs = append(errs, apivalidation.ValidateAnnotations(annotations, path.Child("serviceAnnotations"))...)
	return errs
}
