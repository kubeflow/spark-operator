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

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	operatorscheme "github.com/kubeflow/spark-operator/v2/pkg/scheme"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// +kubebuilder:webhook:admissionReviewVersions=v1,failurePolicy=fail,groups=sparkoperator.k8s.io,matchPolicy=Exact,mutating=true,name=mutate-sparkapplication.sparkoperator.k8s.io,path=/mutate-sparkoperator-k8s-io-v1beta2-sparkapplication,reinvocationPolicy=Never,resources=sparkapplications,sideEffects=None,verbs=create;update,versions=v1beta2,webhookVersions=v1

// SparkApplicationDefaulter sets default values for a SparkApplication.
type SparkApplicationDefaulter struct{}

// NewSparkApplicationValidator creates a new SparkApplicationValidator instance.
func NewSparkApplicationDefaulter() *SparkApplicationDefaulter {
	return &SparkApplicationDefaulter{}
}

// SparkApplicationDefaulter implements admission.CustomDefaulter.
var _ admission.CustomDefaulter = &SparkApplicationDefaulter{}

// Default implements admission.CustomDefaulter.
func (d *SparkApplicationDefaulter) Default(ctx context.Context, obj runtime.Object) error {
	app, ok := obj.(*v1beta2.SparkApplication)
	if !ok {
		return nil
	}

	// Only set the default values for spark applications with new state or invalidating state.
	state := util.GetApplicationState(app)
	if state != v1beta2.ApplicationStateNew && state != v1beta2.ApplicationStateInvalidating {
		return nil
	}

	logger.Info("Defaulting SparkApplication", "name", app.Name, "namespace", app.Namespace, "state", util.GetApplicationState(app))
	operatorscheme.WebhookScheme.Default(app)
	return nil
}
