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

package resourceusage

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

func cpuRequest(cores *int32, coreRequest *string) (string, error) {
	// coreRequest takes precedence over cores if specified.
	// coreLimit is not relevant as pods are scheduled based on request values.
	if coreRequest != nil {
		// Fail fast by validating coreRequest before app submission even though
		// both Spark and YuniKorn validate this field anyway.
		if _, err := resource.ParseQuantity(*coreRequest); err != nil {
			return "", fmt.Errorf("failed to parse %s: %w", *coreRequest, err)
		}
		return *coreRequest, nil
	}
	if cores != nil {
		return fmt.Sprintf("%d", *cores), nil
	}
	return "1", nil
}

// DriverPodRequests returns the CPU and memory requests for the driver pod as a
// string map (suitable for YuniKorn task-group minResource). Memory includes
// the correct overhead: explicit memoryOverhead if set, otherwise the default
// memoryOverheadFactor (0.1 for JVM/Scala, 0.4 for Python/R).
func DriverPodRequests(app *v1beta2.SparkApplication) (map[string]string, error) {
	cpuValue, err := cpuRequest(app.Spec.Driver.Cores, app.Spec.Driver.CoreRequest)
	if err != nil {
		return nil, err
	}

	memoryValue, err := driverMemoryRequest(app)
	if err != nil {
		return nil, err
	}

	return map[string]string{
		"cpu":    cpuValue,
		"memory": memoryValue,
	}, nil
}

// ExecutorPodRequests returns the CPU and memory requests for a single executor
// pod as a string map. Memory includes overhead, pyspark memory, and off-heap
// memory where applicable.
func ExecutorPodRequests(app *v1beta2.SparkApplication) (map[string]string, error) {
	cpuValue, err := cpuRequest(app.Spec.Executor.Cores, app.Spec.Executor.CoreRequest)
	if err != nil {
		return nil, err
	}

	memoryValue, err := executorMemoryRequest(app)
	if err != nil {
		return nil, err
	}

	return map[string]string{
		"cpu":    cpuValue,
		"memory": memoryValue,
	}, nil
}

// ToResourceList converts a string map (as returned by DriverPodRequests /
// ExecutorPodRequests) into a corev1.ResourceList. Every value must be a valid
// Kubernetes quantity string; an invalid value returns a non-nil error and a
// nil ResourceList.
func ToResourceList(resources map[string]string) (corev1.ResourceList, error) {
	rl := make(corev1.ResourceList, len(resources))
	for name, value := range resources {
		q, err := resource.ParseQuantity(value)
		if err != nil {
			return nil, fmt.Errorf("failed to parse resource %q value %q: %w", name, value, err)
		}
		rl[corev1.ResourceName(name)] = q
	}
	return rl, nil
}

// DriverPodResourceList returns the driver pod resource requests as a
// corev1.ResourceList, suitable for use in Volcano PodGroup minResources.
// It is equivalent to calling DriverPodRequests followed by ToResourceList.
func DriverPodResourceList(app *v1beta2.SparkApplication) (corev1.ResourceList, error) {
	reqs, err := DriverPodRequests(app)
	if err != nil {
		return nil, err
	}
	return ToResourceList(reqs)
}

// ExecutorPodResourceList returns the resource requests for a single executor
// pod as a corev1.ResourceList. It is equivalent to calling ExecutorPodRequests
// followed by ToResourceList.
func ExecutorPodResourceList(app *v1beta2.SparkApplication) (corev1.ResourceList, error) {
	reqs, err := ExecutorPodRequests(app)
	if err != nil {
		return nil, err
	}
	return ToResourceList(reqs)
}
