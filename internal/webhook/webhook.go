/*
Copyright 2024 The Kubeflow authors.

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
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
)

var (
	logger = ctrl.Log.WithName("")
)

type Options struct {
	SparkJobNamespaces             []string
	WebhookName                    string
	WebhookPort                    int
	WebhookSecretName              string
	WebhookSecretNamespace         string
	WebhookServiceName             string
	WebhookServiceNamespace        string
	WebhookMetricsBindAddress      string
	EnableResourceQuotaEnforcement bool
}

// validateNameLength checks if the application name exceeds the limit for Kubernetes labels.
// The RFC 1123 DNS label regex is handled by Kubernetes itself,
// we only need to check the length here to provide a more specific
// and early error message.
func validateNameLength(_ context.Context, appMeta metav1.ObjectMeta) error {
	var allErrs field.ErrorList
	if len(appMeta.Name) > validation.LabelValueMaxLength {
		allErrs = append(allErrs, field.Invalid(field.NewPath("metadata", "name"), appMeta.Name,
			fmt.Sprintf("name length must not exceed %d characters to allow for resource suffixes", maxAppNameLength)))
	}
	if allErrs != nil {
		return allErrs.ToAggregate()
	}
	return nil
}
