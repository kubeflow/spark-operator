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
	"strconv"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

// effectiveMemoryConfig holds the resolved memory configuration for a single pod
// (driver or executor) after applying the CRD-vs-sparkConf precedence rules
// documented below.
type effectiveMemoryConfig struct {
	// heapBytes is the effective spark.driver.memory / spark.executor.memory in bytes.
	heapBytes int64
	// memoryOverheadBytes is non-zero only when an explicit memoryOverhead is set;
	// zero means the factor-based calculation should be used.
	memoryOverheadBytes int64
	// overheadExplicit is true when memoryOverheadBytes was sourced from an
	// explicit field rather than the default factor.
	overheadExplicit bool
	// overheadFactor is the effective memoryOverheadFactor to apply when
	// overheadExplicit is false.
	overheadFactor float64
}

// totalBytes returns heapBytes + overhead bytes, where overhead is either the
// explicit value or max(heap*factor, MinMemoryOverhead).
//
// Spark computes overhead in whole MiB: max(int(heapMiB * factor), 384).
// We compute in bytes and floor the total at the end (in bytesToMi), which
// produces the same integer-MiB result because:
//   int64(heapBytes * factor) / 1024 / 1024  ==  int(heapMiB * factor)
// for any heap that is an exact number of MiB (which all Spark memory
// strings produce).  bytesToMi then floors the grand total, matching Spark.
func (e *effectiveMemoryConfig) totalBytes() int64 {
	if e.overheadExplicit {
		return e.heapBytes + e.memoryOverheadBytes
	}
	overhead := int64(float64(e.heapBytes) * e.overheadFactor)
	if overhead < common.MinMemoryOverhead {
		overhead = common.MinMemoryOverhead
	}
	return e.heapBytes + overhead
}

// Precedence rules, verified against
// internal/controller/sparkapplication/submission.go buildSparkSubmitArgs:
//
//	Option order: memoryOverheadFactorOption(10) → sparkConfOption(12) →
//	              driverConfOption(15) → executorConfOption(19)
//
// Spark CLI processes --conf flags in order and the LAST occurrence wins.
// Therefore:
//
//   - spark.driver.memory / spark.executor.memory: CRD field wins over sparkConf
//     (driverConfOption/executorConfOption run after sparkConfOption).
//
//   - spark.driver.memoryOverhead / spark.executor.memoryOverhead: CRD field wins.
//
//   - Factor keys (highest to lowest priority for the factor):
//       1. sparkConf[spark.driver.memoryOverheadFactor] (driver) or
//          sparkConf[spark.executor.memoryOverheadFactor] (executor)  — Spark 3.3+,
//          per-role key, wins over the cluster-wide key.
//       2. sparkConf[spark.kubernetes.memoryOverheadFactor] — cluster-wide key,
//          wins over the CRD structured field (sparkConfOption runs after
//          memoryOverheadFactorOption).
//       3. spec.MemoryOverheadFactor — structured CRD field.
//       4. Language default: 0.1 for JVM (Java/Scala), 0.4 for non-JVM (Python/R).

// effectiveDriverConfig returns the resolved memory configuration for the driver.
func effectiveDriverConfig(app *v1beta2.SparkApplication) (*effectiveMemoryConfig, error) {
	return effectivePodConfig(
		app,
		app.Spec.Driver.Memory,
		app.Spec.Driver.MemoryOverhead,
		common.SparkDriverMemory,
		common.SparkDriverMemoryOverhead,
		common.SparkDriverMemoryOverheadFactor,
	)
}

// effectiveExecutorConfig returns the resolved memory configuration for an executor.
func effectiveExecutorConfig(app *v1beta2.SparkApplication) (*effectiveMemoryConfig, error) {
	return effectivePodConfig(
		app,
		app.Spec.Executor.Memory,
		app.Spec.Executor.MemoryOverhead,
		common.SparkExecutorMemory,
		common.SparkExecutorMemoryOverhead,
		common.SparkExecutorMemoryOverheadFactor,
	)
}

// effectivePodConfig is the shared resolver for driver and executor.
//
// Precedence for heap:
//  1. CRD structured field (Memory *string) — if non-nil it is used as-is.
//  2. sparkConf[heapKey] — raw string from spec.SparkConf.
//  3. Default: common.DefaultMemoryBytes (1 GiB, matching Spark's own default).
//
// Precedence for overhead:
//  1. CRD structured field (MemoryOverhead *string) — explicit, factor not applied.
//  2. sparkConf[overheadKey] — raw string; explicit, factor not applied.
//  3. Default: apply overheadFactor (resolved below).
//
// Precedence for overheadFactor (see effectiveOverheadFactor):
//  1. sparkConf[perRoleFactorKey] — per-role key (spark.driver/executor.memoryOverheadFactor).
//  2. sparkConf[spark.kubernetes.memoryOverheadFactor] — cluster-wide key.
//  3. spec.MemoryOverheadFactor — structured CRD field.
//  4. Language default (0.1 JVM / 0.4 non-JVM).
func effectivePodConfig(
	app *v1beta2.SparkApplication,
	crdMemory *string,
	crdOverhead *string,
	heapKey string,
	overheadKey string,
	perRoleFactorKey string,
) (*effectiveMemoryConfig, error) {
	cfg := &effectiveMemoryConfig{}

	// --- Heap ---
	switch {
	case crdMemory != nil:
		b, err := byteStringAsBytes(*crdMemory, DefaultUnitMiB)
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s CRD field %q: %w", heapKey, *crdMemory, err)
		}
		cfg.heapBytes = b
	case app.Spec.SparkConf[heapKey] != "":
		b, err := byteStringAsBytes(app.Spec.SparkConf[heapKey], DefaultUnitMiB)
		if err != nil {
			return nil, fmt.Errorf("failed to parse sparkConf %s=%q: %w",
				heapKey, app.Spec.SparkConf[heapKey], err)
		}
		cfg.heapBytes = b
	default:
		cfg.heapBytes = common.DefaultMemoryBytes
	}

	// --- Overhead ---
	switch {
	case crdOverhead != nil:
		b, err := byteStringAsBytes(*crdOverhead, DefaultUnitMiB)
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s CRD field %q: %w", overheadKey, *crdOverhead, err)
		}
		cfg.memoryOverheadBytes = b
		cfg.overheadExplicit = true
	case app.Spec.SparkConf[overheadKey] != "":
		b, err := byteStringAsBytes(app.Spec.SparkConf[overheadKey], DefaultUnitMiB)
		if err != nil {
			return nil, fmt.Errorf("failed to parse sparkConf %s=%q: %w",
				overheadKey, app.Spec.SparkConf[overheadKey], err)
		}
		cfg.memoryOverheadBytes = b
		cfg.overheadExplicit = true
	default:
		factor, err := effectiveOverheadFactor(app, perRoleFactorKey)
		if err != nil {
			return nil, err
		}
		cfg.overheadFactor = factor
	}

	return cfg, nil
}

// effectiveOverheadFactor returns the memory overhead factor to use.
//
// Precedence:
//  1. sparkConf[perRoleFactorKey] — spark.driver.memoryOverheadFactor or
//     spark.executor.memoryOverheadFactor (Spark 3.3+, role-specific, highest priority).
//  2. sparkConf[spark.kubernetes.memoryOverheadFactor] — cluster-wide key;
//     wins over the CRD field because sparkConfOption runs after
//     memoryOverheadFactorOption in buildSparkSubmitArgs.
//  3. spec.MemoryOverheadFactor — structured CRD field.
//  4. Language default: 0.1 for JVM (Java/Scala), 0.4 for non-JVM (Python/R).
func effectiveOverheadFactor(app *v1beta2.SparkApplication, perRoleFactorKey string) (float64, error) {
	// 1. Per-role sparkConf key (highest priority).
	if raw, ok := app.Spec.SparkConf[perRoleFactorKey]; ok && raw != "" {
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse sparkConf %s=%q: %w", perRoleFactorKey, raw, err)
		}
		return f, nil
	}
	// 2. Cluster-wide sparkConf key.
	if raw, ok := app.Spec.SparkConf[common.SparkKubernetesMemoryOverheadFactor]; ok && raw != "" {
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse sparkConf %s=%q: %w",
				common.SparkKubernetesMemoryOverheadFactor, raw, err)
		}
		return f, nil
	}
	// 3. CRD structured field.
	if app.Spec.MemoryOverheadFactor != nil && *app.Spec.MemoryOverheadFactor != "" {
		f, err := strconv.ParseFloat(*app.Spec.MemoryOverheadFactor, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse spec.MemoryOverheadFactor %q: %w",
				*app.Spec.MemoryOverheadFactor, err)
		}
		return f, nil
	}
	// 4. Language defaults.
	if isJavaApp(app.Spec.Type) {
		return common.DefaultJVMMemoryOverheadFactor, nil
	}
	return common.DefaultNonJVMMemoryOverheadFactor, nil
}
