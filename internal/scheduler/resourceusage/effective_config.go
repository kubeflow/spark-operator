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
//   - spark.kubernetes.memoryOverheadFactor: sparkConf wins over spec.MemoryOverheadFactor
//     (sparkConfOption runs after memoryOverheadFactorOption).
//
// The per-role keys spark.driver.memoryOverheadFactor /
// spark.executor.memoryOverheadFactor are not supported by this codebase (no
// constants, no CRD field); they are ignored here.

// effectiveDriverConfig returns the resolved memory configuration for the driver,
// applying the precedence rules above.
func effectiveDriverConfig(app *v1beta2.SparkApplication) (*effectiveMemoryConfig, error) {
	return effectivePodConfig(
		app,
		app.Spec.Driver.Memory,
		app.Spec.Driver.MemoryOverhead,
		common.SparkDriverMemory,
		common.SparkDriverMemoryOverhead,
	)
}

// effectiveExecutorConfig returns the resolved memory configuration for an
// executor, applying the precedence rules above.
func effectiveExecutorConfig(app *v1beta2.SparkApplication) (*effectiveMemoryConfig, error) {
	return effectivePodConfig(
		app,
		app.Spec.Executor.Memory,
		app.Spec.Executor.MemoryOverhead,
		common.SparkExecutorMemory,
		common.SparkExecutorMemoryOverhead,
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
// Precedence for overheadFactor:
//  1. sparkConf["spark.kubernetes.memoryOverheadFactor"] — wins because sparkConfOption
//     runs after memoryOverheadFactorOption in the arg builder.
//  2. spec.MemoryOverheadFactor — structured CRD field.
//  3. Default: 0.1 for JVM (Java/Scala), 0.4 for non-JVM (Python/R).
func effectivePodConfig(
	app *v1beta2.SparkApplication,
	crdMemory *string,
	crdOverhead *string,
	heapKey string,
	overheadKey string,
) (*effectiveMemoryConfig, error) {
	cfg := &effectiveMemoryConfig{}

	// --- Heap ---
	switch {
	case crdMemory != nil:
		// CRD wins.
		b, err := byteStringAsBytes(*crdMemory, DefaultUnitMiB)
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s CRD field %q: %w", heapKey, *crdMemory, err)
		}
		cfg.heapBytes = b
	case app.Spec.SparkConf[heapKey] != "":
		// Raw sparkConf fallback.
		b, err := byteStringAsBytes(app.Spec.SparkConf[heapKey], DefaultUnitMiB)
		if err != nil {
			return nil, fmt.Errorf("failed to parse sparkConf %s=%q: %w",
				heapKey, app.Spec.SparkConf[heapKey], err)
		}
		cfg.heapBytes = b
	default:
		// Spark default: 1 GiB.
		cfg.heapBytes = common.DefaultMemoryBytes
	}

	// --- Overhead ---
	switch {
	case crdOverhead != nil:
		// CRD wins; explicit value, skip factor.
		b, err := byteStringAsBytes(*crdOverhead, DefaultUnitMiB)
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s CRD field %q: %w", overheadKey, *crdOverhead, err)
		}
		cfg.memoryOverheadBytes = b
		cfg.overheadExplicit = true
	case app.Spec.SparkConf[overheadKey] != "":
		// Raw sparkConf; explicit, skip factor.
		b, err := byteStringAsBytes(app.Spec.SparkConf[overheadKey], DefaultUnitMiB)
		if err != nil {
			return nil, fmt.Errorf("failed to parse sparkConf %s=%q: %w",
				overheadKey, app.Spec.SparkConf[overheadKey], err)
		}
		cfg.memoryOverheadBytes = b
		cfg.overheadExplicit = true
	default:
		// Use factor; factor itself has its own precedence.
		factor, err := effectiveOverheadFactor(app)
		if err != nil {
			return nil, err
		}
		cfg.overheadFactor = factor
	}

	return cfg, nil
}

// effectiveOverheadFactor returns the memory overhead factor to use, applying:
//  1. sparkConf["spark.kubernetes.memoryOverheadFactor"] (wins over CRD field because
//     sparkConfOption runs after memoryOverheadFactorOption in buildSparkSubmitArgs).
//  2. spec.MemoryOverheadFactor (structured CRD field).
//  3. Default: 0.1 for JVM, 0.4 for Python/R.
func effectiveOverheadFactor(app *v1beta2.SparkApplication) (float64, error) {
	// sparkConf wins for the factor key.
	if raw, ok := app.Spec.SparkConf[common.SparkKubernetesMemoryOverheadFactor]; ok && raw != "" {
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse sparkConf %s=%q: %w",
				common.SparkKubernetesMemoryOverheadFactor, raw, err)
		}
		return f, nil
	}
	// CRD structured field.
	if app.Spec.MemoryOverheadFactor != nil && *app.Spec.MemoryOverheadFactor != "" {
		f, err := strconv.ParseFloat(*app.Spec.MemoryOverheadFactor, 64)
		if err != nil {
			return 0, fmt.Errorf("failed to parse spec.MemoryOverheadFactor %q: %w",
				*app.Spec.MemoryOverheadFactor, err)
		}
		return f, nil
	}
	// Defaults.
	if isJavaApp(app.Spec.Type) {
		return common.DefaultJVMMemoryOverheadFactor, nil
	}
	return common.DefaultNonJVMMemoryOverheadFactor, nil
}
