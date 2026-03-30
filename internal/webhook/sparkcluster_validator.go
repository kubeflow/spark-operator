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
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
)

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// +kubebuilder:webhook:admissionReviewVersions=v1,failurePolicy=fail,groups=sparkoperator.k8s.io,matchPolicy=Exact,mutating=false,name=validate-sparkcluster.sparkoperator.k8s.io,path=/validate-sparkoperator-k8s-io-v1alpha1-sparkcluster,reinvocationPolicy=Never,resources=sparkclusters,sideEffects=None,verbs=create;update,versions=v1alpha1,webhookVersions=v1

// SparkClusterValidator validates SparkCluster resources.
type SparkClusterValidator struct{}

// NewSparkClusterValidator creates a new SparkClusterValidator instance.
func NewSparkClusterValidator() *SparkClusterValidator {
	return &SparkClusterValidator{}
}

var _ admission.CustomValidator = &SparkClusterValidator{}

// ValidateCreate implements admission.CustomValidator.
func (v *SparkClusterValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (warnings admission.Warnings, err error) {
	cluster, ok := obj.(*v1alpha1.SparkCluster)
	if !ok {
		return nil, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("Validating SparkCluster create", "name", cluster.Name, "namespace", cluster.Namespace)

	if err := v.validateName(cluster.Name); err != nil {
		return nil, err
	}

	if err := v.validateSpec(cluster); err != nil {
		return nil, err
	}

	return nil, nil
}

// ValidateUpdate implements admission.CustomValidator.
func (v *SparkClusterValidator) ValidateUpdate(ctx context.Context, oldObj runtime.Object, newObj runtime.Object) (warnings admission.Warnings, err error) {
	oldCluster, ok := oldObj.(*v1alpha1.SparkCluster)
	if !ok {
		return nil, nil
	}

	newCluster, ok := newObj.(*v1alpha1.SparkCluster)
	if !ok {
		return nil, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("Validating SparkCluster update", "name", newCluster.Name, "namespace", newCluster.Namespace)

	if err := v.validateName(newCluster.Name); err != nil {
		return nil, err
	}

	// Skip validating when spec does not change.
	if equality.Semantic.DeepEqual(oldCluster.Spec, newCluster.Spec) {
		return nil, nil
	}

	if err := v.validateSpec(newCluster); err != nil {
		return nil, err
	}

	return nil, nil
}

// ValidateDelete implements admission.CustomValidator.
func (v *SparkClusterValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (warnings admission.Warnings, err error) {
	cluster, ok := obj.(*v1alpha1.SparkCluster)
	if !ok {
		return nil, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("Validating SparkCluster delete", "name", cluster.Name, "namespace", cluster.Namespace)

	return nil, nil
}

// validateName ensures the SparkCluster name is a valid DNS-1035 label and derived
// resource names (e.g., "<name>-master") fit within the label length limit.
func (v *SparkClusterValidator) validateName(name string) error {
	if errs := validation.IsDNS1035Label(name); len(errs) > 0 {
		return fmt.Errorf("invalid SparkCluster name %q: %s", name, strings.Join(errs, ", "))
	}

	const masterSuffix = "-master"
	maxBaseLen := validation.DNS1035LabelMaxLength - len(masterSuffix)
	if len(name) > maxBaseLen {
		return fmt.Errorf("invalid SparkCluster name %q: must be at most %d characters so that the derived Service name %q does not exceed the DNS-1035 label length limit (%d characters)",
			name, maxBaseLen, name+masterSuffix, validation.DNS1035LabelMaxLength)
	}

	return nil
}

func (v *SparkClusterValidator) validateSpec(cluster *v1alpha1.SparkCluster) error {
	if cluster.Spec.SparkVersion == "" {
		return fmt.Errorf("sparkVersion is required")
	}

	if err := v.validateImage(cluster); err != nil {
		return err
	}

	if err := v.validateWorkerGroups(cluster); err != nil {
		return err
	}

	return nil
}

func (v *SparkClusterValidator) validateImage(cluster *v1alpha1.SparkCluster) error {
	if cluster.Spec.Image != nil && *cluster.Spec.Image != "" {
		return nil
	}

	// Check master template has an image.
	masterImageFound := false
	if cluster.Spec.Master.Template != nil && cluster.Spec.Master.Template.Spec != nil {
		for _, c := range cluster.Spec.Master.Template.Spec.Containers {
			if c.Image != "" {
				masterImageFound = true
				break
			}
		}
	}

	if !masterImageFound {
		return fmt.Errorf("image must be specified in spec.image or in the master pod template")
	}

	// Check all worker groups have images.
	for _, group := range cluster.Spec.WorkerGroups {
		workerImageFound := false
		if group.Template != nil && group.Template.Spec != nil {
			for _, c := range group.Template.Spec.Containers {
				if c.Image != "" {
					workerImageFound = true
					break
				}
			}
		}
		if !workerImageFound {
			return fmt.Errorf("image must be specified in spec.image or in worker group %q pod template", group.Name)
		}
	}

	return nil
}

func (v *SparkClusterValidator) validateWorkerGroups(cluster *v1alpha1.SparkCluster) error {
	names := make(map[string]bool)
	for _, group := range cluster.Spec.WorkerGroups {
		if group.Name == "" {
			return fmt.Errorf("worker group name must not be empty")
		}
		if names[group.Name] {
			return fmt.Errorf("duplicate worker group name %q", group.Name)
		}
		names[group.Name] = true

		if group.Replicas != nil && *group.Replicas < 0 {
			return fmt.Errorf("worker group %q replicas must be non-negative", group.Name)
		}
	}
	return nil
}
