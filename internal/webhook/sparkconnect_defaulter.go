/*
Copyright 2026 The Kubeflow authors.

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

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	operatorscheme "github.com/kubeflow/spark-operator/v2/pkg/scheme"
)

// +kubebuilder:webhook:admissionReviewVersions=v1,failurePolicy=fail,groups=sparkoperator.k8s.io,matchPolicy=Exact,mutating=true,name=mutate-sparkconnect.sparkoperator.k8s.io,path=/mutate-sparkoperator-k8s-io-v1alpha1-sparkconnect,reinvocationPolicy=Never,resources=sparkconnects,sideEffects=None,verbs=create;update,versions=v1alpha1,webhookVersions=v1

// SparkConnectDefaulter sets default values for a SparkConnect.
type SparkConnectDefaulter struct{}

// NewSparkConnectDefaulter creates a new SparkConnectDefaulter instance.
func NewSparkConnectDefaulter() *SparkConnectDefaulter {
	return &SparkConnectDefaulter{}
}

// SparkConnectDefaulter implements admission.CustomDefaulter.
var _ admission.CustomDefaulter = &SparkConnectDefaulter{}

// Default implements admission.CustomDefaulter.
func (d *SparkConnectDefaulter) Default(ctx context.Context, obj runtime.Object) error {
	sc, ok := obj.(*v1alpha1.SparkConnect)
	if !ok {
		return nil
	}

	logger := log.FromContext(ctx)
	logger.Info("Defaulting SparkConnect", "name", sc.Name, "namespace", sc.Namespace)

	// Apply scheme defaults
	operatorscheme.WebhookScheme.Default(sc)

	return nil
}
