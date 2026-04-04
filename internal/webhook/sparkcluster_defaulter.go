/*
Copyright 2025 The Kubeflow authors.

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

// +kubebuilder:webhook:admissionReviewVersions=v1,failurePolicy=fail,groups=sparkoperator.k8s.io,matchPolicy=Exact,mutating=true,name=mutate-sparkcluster.sparkoperator.k8s.io,path=/mutate-sparkoperator-k8s-io-v1alpha1-sparkcluster,reinvocationPolicy=Never,resources=sparkclusters,sideEffects=None,verbs=create;update,versions=v1alpha1,webhookVersions=v1

// SparkClusterDefaulter sets default values for SparkCluster resources.
type SparkClusterDefaulter struct{}

// NewSparkClusterDefaulter creates a new SparkClusterDefaulter instance.
func NewSparkClusterDefaulter() *SparkClusterDefaulter {
	return &SparkClusterDefaulter{}
}

var _ admission.CustomDefaulter = &SparkClusterDefaulter{}

// Default implements admission.CustomDefaulter.
func (d *SparkClusterDefaulter) Default(ctx context.Context, obj runtime.Object) error {
	cluster, ok := obj.(*v1alpha1.SparkCluster)
	if !ok {
		return nil
	}

	logger := log.FromContext(ctx)
	logger.Info("Defaulting SparkCluster", "name", cluster.Name, "namespace", cluster.Namespace)

	operatorscheme.WebhookScheme.Default(cluster)

	return nil
}
