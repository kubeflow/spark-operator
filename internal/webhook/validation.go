/*
Copyright The Kubeflow Authors.

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

package webhook

import (
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// invalidValueErrors turns the messages an apimachinery validation helper reports about value
// into one field error each, so every reason a value is rejected reaches the client.
func invalidValueErrors(path *field.Path, value string, msgs []string) field.ErrorList {
	var errs field.ErrorList
	for _, msg := range msgs {
		errs = append(errs, field.Invalid(path, value, msg))
	}
	return errs
}

// newInvalidError reports errs as the Invalid status the API server renders as a per-field cause
// list, and nil when nothing failed.
func newInvalidError(gk schema.GroupKind, name string, errs field.ErrorList) error {
	if len(errs) == 0 {
		return nil
	}
	return apierrors.NewInvalid(gk, name, errs)
}

// validateObjectName rejects names the operator cannot derive Service names from. Kubernetes
// requires a DNS-1035 label there, and the failure would otherwise only surface once the
// operator tried to create the Service.
func validateObjectName(name string) field.ErrorList {
	return invalidValueErrors(field.NewPath("metadata", "name"), name, validation.IsDNS1035Label(name))
}
