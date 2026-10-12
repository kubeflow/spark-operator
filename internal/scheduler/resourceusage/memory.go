/*
Copyright The Kubeflow Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package resourceusage

import (
	"fmt"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

func isJavaApp(appType v1beta2.SparkApplicationType) bool {
	return appType == v1beta2.SparkApplicationTypeJava || appType == v1beta2.SparkApplicationTypeScala
}

func bytesToMi(b int64) string {
	// this floors the value to the nearest mebibyte
	return fmt.Sprintf("%dMi", b/1024/1024)
}

// executorPysparkMemoryBytes returns the pyspark memory contribution for Python
// executors.  spark.executor.pyspark.memory: bare number means MiB.
// https://spark.apache.org/docs/latest/configuration.html#execution-behavior
func executorPysparkMemoryBytes(app *v1beta2.SparkApplication) (int64, error) {
	pysparkMemory, found := app.Spec.SparkConf["spark.executor.pyspark.memory"]
	if app.Spec.Type != v1beta2.SparkApplicationTypePython || !found {
		return 0, nil
	}
	b, err := byteStringAsBytes(pysparkMemory, DefaultUnitMiB)
	if err != nil {
		return 0, fmt.Errorf("failed to parse spark.executor.pyspark.memory=%q: %w", pysparkMemory, err)
	}
	return b, nil
}

// sparkOffHeapMemoryBytes returns the off-heap memory contribution.
// spark.memory.offHeap.size: bare number means bytes (not MiB).
// https://spark.apache.org/docs/latest/configuration.html#memory-management
func sparkOffHeapMemoryBytes(app *v1beta2.SparkApplication) (int64, error) {
	offHeapMemoryEnabled, found := app.Spec.SparkConf["spark.memory.offHeap.enabled"]
	if !found || offHeapMemoryEnabled == "false" {
		return 0, nil
	}
	offHeapSize, found := app.Spec.SparkConf["spark.memory.offHeap.size"]
	if !found {
		return 0, nil
	}
	b, err := byteStringAsBytes(offHeapSize, DefaultUnitBytes)
	if err != nil {
		return 0, fmt.Errorf("failed to parse spark.memory.offHeap.size=%q: %w", offHeapSize, err)
	}
	return b, nil
}

// driverMemoryRequest returns the total driver pod memory request as a MiB
// string, using effectiveDriverConfig to resolve the correct heap, overhead, and
// factor values from CRD fields and sparkConf.
//
// Precedence (see effective_config.go for full documentation):
//   - Heap:            CRD > sparkConf > default 1 GiB
//   - MemoryOverhead:  CRD > sparkConf > factor-based calculation
//   - OverheadFactor:  sparkConf > CRD > language default (0.1 JVM / 0.4 non-JVM)
//
// See also Spark source:
// https://github.com/apache/spark/blob/11b682cf5b7c5360a02410be288b7905eecc1d28/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/features/BasicDriverFeatureStep.scala#L88
func driverMemoryRequest(app *v1beta2.SparkApplication) (string, error) {
	cfg, err := effectiveDriverConfig(app)
	if err != nil {
		return "", err
	}
	return bytesToMi(cfg.totalBytes()), nil
}

// executorMemoryRequest returns the total executor pod memory request as a MiB
// string, including pyspark and off-heap contributions.
//
// Precedence: see driverMemoryRequest and effective_config.go.
//
// See also Spark source:
// https://github.com/apache/spark/blob/11b682cf5b7c5360a02410be288b7905eecc1d28/resource-managers/kubernetes/core/src/main/scala/org/apache/spark/deploy/k8s/features/BasicExecutorFeatureStep.scala#L121
func executorMemoryRequest(app *v1beta2.SparkApplication) (string, error) {
	cfg, err := effectiveExecutorConfig(app)
	if err != nil {
		return "", err
	}

	pysparkBytes, err := executorPysparkMemoryBytes(app)
	if err != nil {
		return "", err
	}

	offHeapBytes, err := sparkOffHeapMemoryBytes(app)
	if err != nil {
		return "", err
	}

	return bytesToMi(cfg.totalBytes() + pysparkBytes + offHeapBytes), nil
}
