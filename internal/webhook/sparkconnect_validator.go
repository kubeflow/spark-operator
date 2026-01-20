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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
)

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:admissionReviewVersions=v1,failurePolicy=fail,groups=sparkoperator.k8s.io,matchPolicy=Exact,mutating=false,name=validate-sparkconnect.sparkoperator.k8s.io,path=/validate-sparkoperator-k8s-io-v1alpha1-sparkconnect,reinvocationPolicy=Never,resources=sparkconnects,sideEffects=None,verbs=create;update,versions=v1alpha1,webhookVersions=v1

type SparkConnectValidator struct {
	client client.Client

	enableResourceQuotaEnforcement bool
}

// NewSparkConnectValidator creates a new SparkConnectValidator instance.
func NewSparkConnectValidator(client client.Client, enableResourceQuotaEnforcement bool) *SparkConnectValidator {
	return &SparkConnectValidator{
		client:                         client,
		enableResourceQuotaEnforcement: enableResourceQuotaEnforcement,
	}
}

var _ admission.CustomValidator = &SparkConnectValidator{}

// ValidateCreate implements admission.CustomValidator.
func (v *SparkConnectValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (warnings admission.Warnings, err error) {
	conn, ok := obj.(*v1alpha1.SparkConnect)
	if !ok {
		return nil, nil
	}
	logger := log.FromContext(ctx)
	logger.Info("Validating SparkConnect create", "name", conn.Name, "namespace", conn.Namespace)

	// Validate metadata.name early to prevent downstream Service creation failures
	if err := v.validateName(conn.Name); err != nil {
		return nil, err
	}

	if err := v.validateSpec(ctx, conn); err != nil {
		return nil, err
	}

	if v.enableResourceQuotaEnforcement {
		if err := v.validateResourceUsage(ctx, conn); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

// ValidateUpdate implements admission.CustomValidator.
func (v *SparkConnectValidator) ValidateUpdate(ctx context.Context, oldObj runtime.Object, newObj runtime.Object) (warnings admission.Warnings, err error) {
	oldConn, ok := oldObj.(*v1alpha1.SparkConnect)
	if !ok {
		return nil, nil
	}

	newConn, ok := newObj.(*v1alpha1.SparkConnect)
	if !ok {
		return nil, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("Validating SparkConnect update", "name", newConn.Name, "namespace", newConn.Namespace)

	// Name is immutable in Kubernetes, but validate anyway for safety
	if err := v.validateName(newConn.Name); err != nil {
		return nil, err
	}

	// Skip validating when spec does not change significantly
	if oldConn.Spec.SparkVersion == newConn.Spec.SparkVersion &&
		oldConn.Spec.Server.Cores == newConn.Spec.Server.Cores &&
		oldConn.Spec.Server.Memory == newConn.Spec.Server.Memory &&
		oldConn.Spec.Executor.Cores == newConn.Spec.Executor.Cores &&
		oldConn.Spec.Executor.Memory == newConn.Spec.Executor.Memory &&
		oldConn.Spec.Executor.Instances == newConn.Spec.Executor.Instances {
		return nil, nil
	}

	if err := v.validateSpec(ctx, newConn); err != nil {
		return nil, err
	}

	// Validate SparkConnect resource usage when resource quota enforcement is enabled.
	if v.enableResourceQuotaEnforcement {
		if err := v.validateResourceUsage(ctx, newConn); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

// ValidateDelete implements admission.CustomValidator.
func (v *SparkConnectValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (warnings admission.Warnings, err error) {
	conn, ok := obj.(*v1alpha1.SparkConnect)
	if !ok {
		return nil, nil
	}
	logger := log.FromContext(ctx)
	logger.Info("Validating SparkConnect delete", "name", conn.Name, "namespace", conn.Namespace)
	return nil, nil
}

func (v *SparkConnectValidator) validateSpec(ctx context.Context, conn *v1alpha1.SparkConnect) error {
	if conn.Spec.Image == nil || *conn.Spec.Image == "" {
		return fmt.Errorf("image is required")
	}

	if conn.Spec.SparkVersion == "" {
		return fmt.Errorf("sparkVersion is required")
	}

	return nil
}

// validateName ensures the SparkConnect metadata.name is a valid DNS-1035 label
// This prevents failures later when creating related resources like Services which
// require DNS-1035 compliant names.
func (v *SparkConnectValidator) validateName(name string) error {
	if errs := validation.IsDNS1035Label(name); len(errs) > 0 {
		return fmt.Errorf("invalid SparkConnect name %q: %s", name, strings.Join(errs, ", "))
	}
	return nil
}

func (v *SparkConnectValidator) validateResourceUsage(ctx context.Context, conn *v1alpha1.SparkConnect) error {
	requests, err := getSparkConnectResourceList(conn)
	if err != nil {
		return fmt.Errorf("failed to calculate resource requests: %v", err)
	}

	resourceQuotaList := &corev1.ResourceQuotaList{}
	if err := v.client.List(ctx, resourceQuotaList, client.InNamespace(conn.Namespace)); err != nil {
		return fmt.Errorf("failed to list resource quotas: %v", err)
	}

	for _, resourceQuota := range resourceQuotaList.Items {
		// Scope selectors not currently supported, ignore any ResourceQuota that does not match everything.
		// TODO: Add support for scope selectors.
		if resourceQuota.Spec.ScopeSelector != nil || len(resourceQuota.Spec.Scopes) > 0 {
			continue
		}

		if !validateResourceQuota(requests, resourceQuota) {
			return fmt.Errorf("failed to validate resource quota \"%s/%s\": requested resources would exceed quota limits. Server needs %v, Executors need %v (total across %d instances). Available quota: %v",
				resourceQuota.Namespace,
				resourceQuota.Name,
				getServerResources(conn),
				getExecutorResources(conn),
				getExecutorInstances(conn),
				resourceQuota.Status.Hard)
		}
	}

	return nil
}

// Helper functions to get resource details for better error messages
func getServerResources(conn *v1alpha1.SparkConnect) string {
	cores := "default"
	if conn.Spec.Server.Cores != nil {
		cores = fmt.Sprintf("%d cores", *conn.Spec.Server.Cores)
	}
	memory := "default"
	if conn.Spec.Server.Memory != nil {
		memory = *conn.Spec.Server.Memory
	}
	return fmt.Sprintf("%s, %s memory", cores, memory)
}

func getExecutorResources(conn *v1alpha1.SparkConnect) string {
	cores := "default"
	if conn.Spec.Executor.Cores != nil {
		cores = fmt.Sprintf("%d cores", *conn.Spec.Executor.Cores)
	}
	memory := "default"
	if conn.Spec.Executor.Memory != nil {
		memory = *conn.Spec.Executor.Memory
	}
	return fmt.Sprintf("%s, %s memory per executor", cores, memory)
}

func getExecutorInstances(conn *v1alpha1.SparkConnect) int32 {
	if conn.Spec.Executor.Instances != nil {
		return *conn.Spec.Executor.Instances
	}
	return 1
}
