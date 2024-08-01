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
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
)

var javaStringSuffixes = map[string]int64{
	"b":  1,
	"kb": 1 << 10,
	"k":  1 << 10,
	"mb": 1 << 20,
	"m":  1 << 20,
	"gb": 1 << 30,
	"g":  1 << 30,
	"tb": 1 << 40,
	"t":  1 << 40,
	"pb": 1 << 50,
	"p":  1 << 50,
}

var javaStringPattern = regexp.MustCompile(`([0-9]+)([a-z]+)?`)
var javaFractionStringPattern = regexp.MustCompile(`([0-9]+\.[0-9]+)([a-z]+)?`)

// getResourceList returns the resource requests of the given SparkApplication.
func getResourceList(app *v1beta2.SparkApplication) (corev1.ResourceList, error) {
	coresRequests, err := getCoresRequests(app)
	if err != nil {
		return nil, err
	}

	coresLimits, err := getCoresLimits(app)
	if err != nil {
		return nil, err
	}

	memoryRequests, err := getMemoryRequests(app)
	if err != nil {
		return nil, err
	}

	memoryLimits, err := getMemoryLimits(app)
	if err != nil {
		return nil, err
	}

	resourceList := util.SumResourceList([]corev1.ResourceList{
		coresRequests,
		coresLimits,
		memoryRequests,
		memoryLimits,
	})

	return resourceList, nil
}

func getCoresRequests(app *v1beta2.SparkApplication) (corev1.ResourceList, error) {
	// Calculate driver cores requests.
	driverCoresRequests, err := getSparkPodCoresRequests(&app.Spec.Driver.SparkPodSpec, 1)
	if err != nil {
		return nil, err
	}

	// Calculate executor cores requests.
	var replicas int64 = 1
	if app.Spec.Executor.Instances != nil {
		replicas = int64(*app.Spec.Executor.Instances)
	}
	executorCoresRequests, err := getSparkPodCoresRequests(&app.Spec.Executor.SparkPodSpec, replicas)
	if err != nil {
		return nil, err
	}

	return util.SumResourceList([]corev1.ResourceList{driverCoresRequests, executorCoresRequests}), nil
}

func getSparkPodCoresRequests(podSpec *v1beta2.SparkPodSpec, replicas int64) (corev1.ResourceList, error) {
	var milliCores int64
	if podSpec.Cores != nil {
		milliCores = int64(*podSpec.Cores) * 1000
	} else {
		milliCores = common.DefaultCPUMilliCores
	}
	resourceList := corev1.ResourceList{
		corev1.ResourceCPU:         *resource.NewMilliQuantity(milliCores*replicas, resource.DecimalSI),
		corev1.ResourceRequestsCPU: *resource.NewMilliQuantity(milliCores*replicas, resource.DecimalSI),
	}
	return resourceList, nil
}

func getCoresLimits(app *v1beta2.SparkApplication) (corev1.ResourceList, error) {
	// Calculate driver cores limits.
	driverCoresLimits, err := getSparkPodCoresLimits(&app.Spec.Driver.SparkPodSpec, 1)
	if err != nil {
		return nil, err
	}

	// Calculate executor cores requests.
	var replicas int64 = 1
	if app.Spec.Executor.Instances != nil {
		replicas = int64(*app.Spec.Executor.Instances)
	}
	executorCoresLimits, err := getSparkPodCoresLimits(&app.Spec.Executor.SparkPodSpec, replicas)
	if err != nil {
		return nil, err
	}

	return util.SumResourceList([]corev1.ResourceList{driverCoresLimits, executorCoresLimits}), nil
}

func getSparkPodCoresLimits(podSpec *v1beta2.SparkPodSpec, replicas int64) (corev1.ResourceList, error) {
	var milliCores int64
	if podSpec.CoreLimit != nil {
		quantity, err := resource.ParseQuantity(*podSpec.CoreLimit)
		if err != nil {
			return nil, err
		}
		milliCores = quantity.MilliValue()
	} else if podSpec.Cores != nil {
		milliCores = int64(*podSpec.Cores) * 1000
	} else {
		milliCores = common.DefaultCPUMilliCores
	}
	resourceList := corev1.ResourceList{
		corev1.ResourceLimitsCPU: *resource.NewMilliQuantity(milliCores*replicas, resource.DecimalSI),
	}
	return resourceList, nil
}

func getMemoryRequests(app *v1beta2.SparkApplication) (corev1.ResourceList, error) {
	// If memory overhead factor is set, use it. Otherwise, use the default value.
	var memoryOverheadFactor float64
	if app.Spec.MemoryOverheadFactor != nil {
		parsed, err := strconv.ParseFloat(*app.Spec.MemoryOverheadFactor, 64)
		if err != nil {
			return nil, err
		}
		memoryOverheadFactor = parsed
	} else if app.Spec.Type == v1beta2.SparkApplicationTypeJava {
		memoryOverheadFactor = common.DefaultJVMMemoryOverheadFactor
	} else {
		memoryOverheadFactor = common.DefaultNonJVMMemoryOverheadFactor
	}

	// Calculate driver pod memory requests.
	driverResourceList, err := getSparkPodMemoryRequests(&app.Spec.Driver.SparkPodSpec, memoryOverheadFactor, 1)
	if err != nil {
		return nil, err
	}

	// Calculate executor pod memory requests.
	var replicas int64 = 1
	if app.Spec.Executor.Instances != nil {
		replicas = int64(*app.Spec.Executor.Instances)
	}
	executorResourceList, err := getSparkPodMemoryRequests(&app.Spec.Executor.SparkPodSpec, memoryOverheadFactor, replicas)
	if err != nil {
		return nil, err
	}

	return util.SumResourceList([]corev1.ResourceList{driverResourceList, executorResourceList}), nil
}

func getSparkPodMemoryRequests(podSpec *v1beta2.SparkPodSpec, memoryOverheadFactor float64, replicas int64) (corev1.ResourceList, error) {
	var memoryBytes, memoryOverheadBytes int64
	if podSpec.Memory != nil {
		parsed, err := parseJavaMemoryString(*podSpec.Memory)
		if err != nil {
			return nil, err
		}
		memoryBytes = parsed
	}

	if podSpec.MemoryOverhead != nil {
		parsed, err := parseJavaMemoryString(*podSpec.MemoryOverhead)
		if err != nil {
			return nil, err
		}
		memoryOverheadBytes = parsed
	} else {
		memoryOverheadBytes = int64(math.Max(float64(memoryBytes)*memoryOverheadFactor, common.MinMemoryOverhead))
	}

	resourceList := corev1.ResourceList{
		corev1.ResourceMemory:         *resource.NewQuantity((memoryBytes+memoryOverheadBytes)*replicas, resource.BinarySI),
		corev1.ResourceRequestsMemory: *resource.NewQuantity((memoryBytes+memoryOverheadBytes)*replicas, resource.BinarySI),
	}
	return resourceList, nil
}

// For Spark pod, memory requests and limits are the same.
func getMemoryLimits(app *v1beta2.SparkApplication) (corev1.ResourceList, error) {
	return getMemoryRequests(app)
}

// Logic copied from https://github.com/apache/spark/blob/5264164a67df498b73facae207eda12ee133be7d/common/network-common/src/main/java/org/apache/spark/network/util/JavaUtils.java#L276
func parseJavaMemoryString(s string) (int64, error) {
	lower := strings.ToLower(s)
	if matches := javaStringPattern.FindStringSubmatch(lower); matches != nil {
		value, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil {
			return 0, err
		}
		suffix := matches[2]
		if multiplier, present := javaStringSuffixes[suffix]; present {
			return multiplier * value, nil
		}
	} else if matches = javaFractionStringPattern.FindStringSubmatch(lower); matches != nil {
		value, err := strconv.ParseFloat(matches[1], 64)
		if err != nil {
			return 0, err
		}
		suffix := matches[2]
		if multiplier, present := javaStringSuffixes[suffix]; present {
			return int64(float64(multiplier) * value), nil
		}
	}
	return 0, fmt.Errorf("could not parse string '%s' as a Java-style memory value. Examples: 100kb, 1.5mb, 1g", s)
}

// Check whether the resource list will satisfy the resource quota.
func validateResourceQuota(resourceList corev1.ResourceList, resourceQuota corev1.ResourceQuota) bool {
	for key, quantity := range resourceList {
		if _, ok := resourceQuota.Status.Hard[key]; !ok {
			continue
		}
		quantity.Add(resourceQuota.Status.Used[key])
		if quantity.Cmp(resourceQuota.Spec.Hard[key]) > 0 {
			return false
		}
	}
	return true
}
