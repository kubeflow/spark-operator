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
	"math"
	"strconv"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/common"
)

func isJavaApp(appType v1beta2.SparkApplicationType) bool {
	return appType == v1beta2.SparkApplicationTypeJava || appType == v1beta2.SparkApplicationTypeScala
}

func getMemoryOverheadFactor(app *v1beta2.SparkApplication) (float64, error) {
	if app.Spec.MemoryOverheadFactor != nil {
		parsed, err := strconv.ParseFloat(*app.Spec.MemoryOverheadFactor, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse memory overhead factor as float: %w", err)
		}
		return parsed, nil
	} else if isJavaApp(app.Spec.Type) {
		return common.DefaultJVMMemoryOverheadFactor, nil
	}

	return common.DefaultNonJVMMemoryOverheadFactor, nil
}

func memoryRequestBytes(podSpec *v1beta2.SparkPodSpec, memoryOverheadFactor float64) (int64, error) {
	var memoryBytes, memoryOverheadBytes int64

	if podSpec.Memory != nil {
		parsed, err := byteStringAsBytes(*podSpec.Memory)
		if err != nil {
			return 0, err
		}
		memoryBytes = parsed
	}

	if podSpec.MemoryOverhead != nil {
		parsed, err := byteStringAsBytes(*podSpec.MemoryOverhead)
		if err != nil {
			return 0, err
		}
		memoryOverheadBytes = parsed
	} else {
		memoryOverheadBytes = int64(math.Max(
			float64(memoryBytes)*memoryOverheadFactor,
			common.MinMemoryOverhead,
		))
	}

	return memoryBytes + memoryOverheadBytes, nil
}

func executorPysparkMemoryBytes(app *v1beta2.SparkApplication) (int64, error) {
	pysparkMemory, found := app.Spec.SparkConf["spark.executor.pyspark.memory"]
	if app.Spec.Type != v1beta2.SparkApplicationTypePython || !found {
		return 0, nil
	}

	// This fields defaults to mebibytes if no resource suffix is specified
	// https://github.com/apache/spark/blob/7de71a2ec78d985c2a045f13c1275101b126cec4/docs/configuration.md?plain=1#L289-L305
	if _, err := strconv.Atoi(pysparkMemory); err == nil {
		pysparkMemory = pysparkMemory + "m"
	}

	pysparkMemoryBytes, err := byteStringAsBytes(pysparkMemory)
	if err != nil {
		return 0, err
	}

	return pysparkMemoryBytes, nil
}

func sparkOffHeapMemoryBytes(app *v1beta2.SparkApplication) (int64, error) {
	offHeapMemoryEnabled, found := app.Spec.SparkConf["spark.memory.offHeap.enabled"]
	if !found || offHeapMemoryEnabled == "false" {
		return 0, nil
	}
	offHeapSize, found := app.Spec.SparkConf["spark.memory.offHeap.size"]
	if !found {
		return 0, nil
	}
	offHeapBytes, err := byteStringAsBytes(offHeapSize)
	if err != nil {
		return 0, err
	}
	return offHeapBytes, nil
}

func bytesToMi(b int64) string {
	// this floors the value to the nearest mebibyte
	return fmt.Sprintf("%dMi", b/1024/1024)
}

func driverMemoryRequest(app *v1beta2.SparkApplication) (string, error) {
	memoryOverheadFactor, err := getMemoryOverheadFactor(app)
	if err != nil {
		return "", err
	}

	requestBytes, err := memoryRequestBytes(&app.Spec.Driver.SparkPodSpec, memoryOverheadFactor)
	if err != nil {
		return "", err
	}

	// Convert memory quantity to mebibytes even if larger than a gibibyte to match Spark
	// https://github.com/apache/spark/blob/11b682cf5b7c5360a02410be288b7905eecc1d28/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/features/BasicDriverFeatureStep.scala#L88
	// https://github.com/apache/spark/blob/11b682cf5b7c5360a02410be288b7905eecc1d28/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/features/BasicExecutorFeatureStep.scala#L121
	return bytesToMi(requestBytes), nil
}

func executorMemoryRequest(app *v1beta2.SparkApplication) (string, error) {
	memoryOverheadFactor, err := getMemoryOverheadFactor(app)
	if err != nil {
		return "", err
	}

	requestBytes, err := memoryRequestBytes(&app.Spec.Executor.SparkPodSpec, memoryOverheadFactor)
	if err != nil {
		return "", err
	}

	pysparkMemoryBytes, err := executorPysparkMemoryBytes(app)
	if err != nil {
		return "", err
	}

	offHeapBytes, err := sparkOffHeapMemoryBytes(app)
	if err != nil {
		return "", err
	}

	// See comment above in driver
	return bytesToMi(requestBytes + pysparkMemoryBytes + offHeapBytes), nil
}
