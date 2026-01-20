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
	"fmt"
	"math"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

const (
	// Default memory overhead factor for SparkConnect
	defaultSparkConnectMemoryOverheadFactor = 0.4
)

// getSparkConnectResourceList returns the resource requests of the given SparkConnect.
func getSparkConnectResourceList(conn *v1alpha1.SparkConnect) (corev1.ResourceList, error) {
	serverCoresRequests, err := getSparkConnectServerCoresRequests(conn)
	if err != nil {
		return nil, err
	}

	serverCoresLimits, err := getSparkConnectServerCoresLimits(conn)
	if err != nil {
		return nil, err
	}

	serverMemoryRequests, err := getSparkConnectServerMemoryRequests(conn)
	if err != nil {
		return nil, err
	}

	executorCoresRequests, err := getSparkConnectExecutorCoresRequests(conn)
	if err != nil {
		return nil, err
	}

	executorCoresLimits, err := getSparkConnectExecutorCoresLimits(conn)
	if err != nil {
		return nil, err
	}

	executorMemoryRequests, err := getSparkConnectExecutorMemoryRequests(conn)
	if err != nil {
		return nil, err
	}

	resourceList := util.SumResourceList([]corev1.ResourceList{
		serverCoresRequests,
		serverCoresLimits,
		serverMemoryRequests,
		executorCoresRequests,
		executorCoresLimits,
		executorMemoryRequests,
	})

	return resourceList, nil
}

func getSparkConnectServerCoresRequests(conn *v1alpha1.SparkConnect) (corev1.ResourceList, error) {
	var milliCores int64
	if conn.Spec.Server.Cores != nil {
		milliCores = int64(*conn.Spec.Server.Cores) * 1000
	} else {
		milliCores = common.DefaultCPUMilliCores
	}

	// Check if template has explicit resource requests
	if conn.Spec.Server.Template != nil && len(conn.Spec.Server.Template.Spec.Containers) > 0 {
		for _, container := range conn.Spec.Server.Template.Spec.Containers {
			if container.Name == common.SparkDriverContainerName {
				if cpuRequest, ok := container.Resources.Requests[corev1.ResourceCPU]; ok {
					milliCores = cpuRequest.MilliValue()
				}
				break
			}
		}
	}

	resourceList := corev1.ResourceList{
		corev1.ResourceCPU:         *resource.NewMilliQuantity(milliCores, resource.DecimalSI),
		corev1.ResourceRequestsCPU: *resource.NewMilliQuantity(milliCores, resource.DecimalSI),
	}
	return resourceList, nil
}

func getSparkConnectServerCoresLimits(conn *v1alpha1.SparkConnect) (corev1.ResourceList, error) {
	var milliCores int64
	if conn.Spec.Server.Cores != nil {
		milliCores = int64(*conn.Spec.Server.Cores) * 1000
	} else {
		milliCores = common.DefaultCPUMilliCores
	}

	// Check if template has explicit resource limits
	if conn.Spec.Server.Template != nil && len(conn.Spec.Server.Template.Spec.Containers) > 0 {
		for _, container := range conn.Spec.Server.Template.Spec.Containers {
			if container.Name == common.SparkDriverContainerName {
				if cpuLimit, ok := container.Resources.Limits[corev1.ResourceCPU]; ok {
					milliCores = cpuLimit.MilliValue()
				}
				break
			}
		}
	}

	resourceList := corev1.ResourceList{
		corev1.ResourceLimitsCPU: *resource.NewMilliQuantity(milliCores, resource.DecimalSI),
	}
	return resourceList, nil
}

func getSparkConnectServerMemoryRequests(conn *v1alpha1.SparkConnect) (corev1.ResourceList, error) {
	var memoryBytes, memoryOverheadBytes int64

	if conn.Spec.Server.Memory != nil {
		parsed, err := parseJavaMemoryString(*conn.Spec.Server.Memory)
		if err != nil {
			return nil, err
		}
		memoryBytes = parsed
	}

	// Check if template has explicit memory requests
	if conn.Spec.Server.Template != nil && len(conn.Spec.Server.Template.Spec.Containers) > 0 {
		for _, container := range conn.Spec.Server.Template.Spec.Containers {
			if container.Name == common.SparkDriverContainerName {
				if memRequest, ok := container.Resources.Requests[corev1.ResourceMemory]; ok {
					resourceList := corev1.ResourceList{
						corev1.ResourceMemory:         memRequest,
						corev1.ResourceRequestsMemory: memRequest,
					}
					return resourceList, nil
				}
				break
			}
		}
	}

	// Calculate memory overhead
	memoryOverheadBytes = int64(math.Max(float64(memoryBytes)*defaultSparkConnectMemoryOverheadFactor, common.MinMemoryOverhead))

	resourceList := corev1.ResourceList{
		corev1.ResourceMemory:         *resource.NewQuantity(memoryBytes+memoryOverheadBytes, resource.BinarySI),
		corev1.ResourceRequestsMemory: *resource.NewQuantity(memoryBytes+memoryOverheadBytes, resource.BinarySI),
	}
	return resourceList, nil
}

func getSparkConnectExecutorCoresRequests(conn *v1alpha1.SparkConnect) (corev1.ResourceList, error) {
	var replicas int64 = 1
	if conn.Spec.Executor.Instances != nil {
		replicas = int64(*conn.Spec.Executor.Instances)
	}

	var milliCores int64
	if conn.Spec.Executor.Cores != nil {
		milliCores = int64(*conn.Spec.Executor.Cores) * 1000
	} else {
		milliCores = common.DefaultCPUMilliCores
	}

	// Check if template has explicit resource requests
	if conn.Spec.Executor.Template != nil && len(conn.Spec.Executor.Template.Spec.Containers) > 0 {
		for _, container := range conn.Spec.Executor.Template.Spec.Containers {
			if cpuRequest, ok := container.Resources.Requests[corev1.ResourceCPU]; ok {
				milliCores = cpuRequest.MilliValue()
			}
			break
		}
	}

	resourceList := corev1.ResourceList{
		corev1.ResourceCPU:         *resource.NewMilliQuantity(milliCores*replicas, resource.DecimalSI),
		corev1.ResourceRequestsCPU: *resource.NewMilliQuantity(milliCores*replicas, resource.DecimalSI),
	}
	return resourceList, nil
}

func getSparkConnectExecutorCoresLimits(conn *v1alpha1.SparkConnect) (corev1.ResourceList, error) {
	var replicas int64 = 1
	if conn.Spec.Executor.Instances != nil {
		replicas = int64(*conn.Spec.Executor.Instances)
	}

	var milliCores int64
	if conn.Spec.Executor.Cores != nil {
		milliCores = int64(*conn.Spec.Executor.Cores) * 1000
	} else {
		milliCores = common.DefaultCPUMilliCores
	}

	// Check if template has explicit resource limits
	if conn.Spec.Executor.Template != nil && len(conn.Spec.Executor.Template.Spec.Containers) > 0 {
		for _, container := range conn.Spec.Executor.Template.Spec.Containers {
			if cpuLimit, ok := container.Resources.Limits[corev1.ResourceCPU]; ok {
				milliCores = cpuLimit.MilliValue()
			}
			break
		}
	}

	resourceList := corev1.ResourceList{
		corev1.ResourceLimitsCPU: *resource.NewMilliQuantity(milliCores*replicas, resource.DecimalSI),
	}
	return resourceList, nil
}

func getSparkConnectExecutorMemoryRequests(conn *v1alpha1.SparkConnect) (corev1.ResourceList, error) {
	var replicas int64 = 1
	if conn.Spec.Executor.Instances != nil {
		replicas = int64(*conn.Spec.Executor.Instances)
	}

	var memoryBytes, memoryOverheadBytes int64

	if conn.Spec.Executor.Memory != nil {
		parsed, err := parseJavaMemoryString(*conn.Spec.Executor.Memory)
		if err != nil {
			return nil, fmt.Errorf("failed to parse executor memory: %v", err)
		}
		memoryBytes = parsed
	}

	// Check if template has explicit memory requests
	if conn.Spec.Executor.Template != nil && len(conn.Spec.Executor.Template.Spec.Containers) > 0 {
		for _, container := range conn.Spec.Executor.Template.Spec.Containers {
			if memRequest, ok := container.Resources.Requests[corev1.ResourceMemory]; ok {
				totalMemory := memRequest.DeepCopy()
				totalMemory.Set(totalMemory.Value() * replicas)
				resourceList := corev1.ResourceList{
					corev1.ResourceMemory:         totalMemory,
					corev1.ResourceRequestsMemory: totalMemory,
				}
				return resourceList, nil
			}
			break
		}
	}

	// Calculate memory overhead
	memoryOverheadBytes = int64(math.Max(float64(memoryBytes)*defaultSparkConnectMemoryOverheadFactor, common.MinMemoryOverhead))

	resourceList := corev1.ResourceList{
		corev1.ResourceMemory:         *resource.NewQuantity((memoryBytes+memoryOverheadBytes)*replicas, resource.BinarySI),
		corev1.ResourceRequestsMemory: *resource.NewQuantity((memoryBytes+memoryOverheadBytes)*replicas, resource.BinarySI),
	}
	return resourceList, nil
}
