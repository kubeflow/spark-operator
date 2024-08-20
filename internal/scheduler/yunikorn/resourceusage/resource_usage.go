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

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/kubeflow/spark-operator/api/v1beta2"
)

func cpuRequest(cores *int32, coreRequest *string) (string, error) {
	// coreRequest takes precedence over cores if specified
	// coreLimit is not relevant as pods are scheduled based on request values
	if coreRequest != nil {
		// Fail fast by validating coreRequest before app submission even though
		// both Spark and Yunikorn validate this field anyway
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
