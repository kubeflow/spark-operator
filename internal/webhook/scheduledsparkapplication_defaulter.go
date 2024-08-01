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

	"github.com/kubeflow/spark-operator/api/v1beta2"
)

// +kubebuilder:webhook:admissionReviewVersions=v1,failurePolicy=fail,groups=sparkoperator.k8s.io,matchPolicy=Exact,mutating=false,name=mutate-scheduledsparkapplication.sparkoperator.k8s.io,path=/validate-sparkoperator-k8s-io-v1beta2-sparkapplication,reinvocationPolicy=Never,resources=scheduledsparkapplications,sideEffects=None,verbs=create;update,versions=v1beta2,webhookVersions=v1

// ScheduledSparkApplicationDefaulter sets default values for a SparkApplication.
type ScheduledSparkApplicationDefaulter struct{}

// NewSparkApplicationValidator creates a new SparkApplicationValidator instance.
func NewScheduledSparkApplicationDefaulter() *ScheduledSparkApplicationDefaulter {
	return &ScheduledSparkApplicationDefaulter{}
}

// SparkApplicationDefaulter implements admission.CustomDefaulter.
var _ admission.CustomDefaulter = &ScheduledSparkApplicationDefaulter{}

// Default implements admission.CustomDefaulter.
func (d *ScheduledSparkApplicationDefaulter) Default(ctx context.Context, obj runtime.Object) error {
	app, ok := obj.(*v1beta2.ScheduledSparkApplication)
	if !ok {
		return nil
	}
	logger.Info("Defaulting ScheduledSparkApplication", "name", app.Name, "namespace", app.Namespace)
	return nil
}
