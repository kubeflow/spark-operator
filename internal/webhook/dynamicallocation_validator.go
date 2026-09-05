/*
Copyright The Kubeflow Authors.

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
	"errors"
	"fmt"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

// fieldRef points at whichever of the CRD field or sparkConf key actually set a merged value.
type fieldRef struct {
	path *field.Path
	key  string // set instead of path when the value was sourced from sparkConf
}

func crdFieldRef(path *field.Path) fieldRef {
	return fieldRef{path: path}
}

func sparkConfFieldRef(key string) fieldRef {
	return fieldRef{key: key}
}

func (r fieldRef) String() string {
	if r.path != nil {
		return r.path.String()
	}
	return fmt.Sprintf("spec.sparkConf[%q]", r.key)
}

// validateDynamicAllocation validates the merged executor bounds, shared by the CRD
// DynamicAllocation field and the equivalent sparkConf values.
func validateDynamicAllocation(minPath, maxPath, initPath, instPath fieldRef, minExecutors, maxExecutors, initialExecutors, executorInstances *int32) (admission.Warnings, error) {
	var errs []error
	minValid, maxValid := true, true

	if minExecutors != nil && *minExecutors < 0 {
		errs = append(errs, fmt.Errorf("%s must be non-negative, got %d", minPath, *minExecutors))
		minValid = false
	}
	if maxExecutors != nil && *maxExecutors <= 0 {
		errs = append(errs, fmt.Errorf("%s must be positive, got %d", maxPath, *maxExecutors))
		maxValid = false
	}

	if minValid && maxValid && minExecutors != nil && maxExecutors != nil && *minExecutors > *maxExecutors {
		errs = append(errs, fmt.Errorf("%s (%d) cannot be greater than %s (%d)", minPath, *minExecutors, maxPath, *maxExecutors))
	}

	var warnings admission.Warnings
	if minValid && minExecutors != nil && initialExecutors != nil && *initialExecutors < *minExecutors {
		warnings = append(warnings, fmt.Sprintf("%s (%d) is less than %s (%d); %s will be used as the initial number of executors",
			initPath, *initialExecutors, minPath, *minExecutors, minPath))
	}

	// Spark's actual initial executor count is max(minExecutors, initialExecutors, executorInstances);
	// that's what must not exceed maxExecutors, not the individual fields.
	if maxValid && maxExecutors != nil {
		var initialTarget int32
		if minValid && minExecutors != nil && *minExecutors > initialTarget {
			initialTarget = *minExecutors
		}
		if initialExecutors != nil && *initialExecutors > initialTarget {
			initialTarget = *initialExecutors
		}
		if executorInstances != nil && *executorInstances > initialTarget {
			initialTarget = *executorInstances
		}
		if initialTarget > *maxExecutors {
			errs = append(errs, fmt.Errorf("the initial number of executors, max(%s, %s, %s) = %d, cannot be greater than %s (%d)",
				minPath, initPath, instPath, initialTarget, maxPath, *maxExecutors))
		}
	}

	return warnings, errors.Join(errs...)
}

// parseSparkBoolean matches Spark's ConfigBuilder.toBoolean, which only accepts "true"/"false"
// (case-insensitive, trimmed) unlike the more permissive strconv.ParseBool.
func parseSparkBoolean(value string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, strconv.ErrSyntax
	}
}

// mergeAndValidateDynamicAllocation merges the CRD DynamicAllocation field and Executor.Instances
// with the equivalent sparkConf values (CRD wins per-field, since dynamicAllocationOption runs
// after sparkConfOption).
func mergeAndValidateDynamicAllocation(root *field.Path, crdEnabled bool, crdMinExecutors, crdMaxExecutors, crdInitialExecutors, crdExecutorInstances *int32, sparkConf map[string]string) (admission.Warnings, error) {
	var confEnabled bool
	if value, ok := sparkConf[common.SparkDynamicAllocationEnabled]; ok {
		var err error
		if confEnabled, err = parseSparkBoolean(value); err != nil {
			return nil, fmt.Errorf("%s must be a boolean, got %q", sparkConfFieldRef(common.SparkDynamicAllocationEnabled), value)
		}
	}
	if !crdEnabled && !confEnabled {
		return nil, nil
	}

	confMinExecutors, confMaxExecutors, confInitialExecutors, confExecutorInstances, err := parseDynamicAllocationSparkConf(sparkConf)
	if err != nil {
		return nil, err
	}

	minExecutors, minRef := confMinExecutors, sparkConfFieldRef(common.SparkDynamicAllocationMinExecutors)
	maxExecutors, maxRef := confMaxExecutors, sparkConfFieldRef(common.SparkDynamicAllocationMaxExecutors)
	initialExecutors, initRef := confInitialExecutors, sparkConfFieldRef(common.SparkDynamicAllocationInitialExecutors)
	executorInstances, instRef := confExecutorInstances, sparkConfFieldRef(common.SparkExecutorInstances)

	if crdEnabled {
		if crdMinExecutors != nil {
			minExecutors, minRef = crdMinExecutors, crdFieldRef(root.Child("dynamicAllocation", "minExecutors"))
		}
		if crdMaxExecutors != nil {
			maxExecutors, maxRef = crdMaxExecutors, crdFieldRef(root.Child("dynamicAllocation", "maxExecutors"))
		}
		if crdInitialExecutors != nil {
			initialExecutors, initRef = crdInitialExecutors, crdFieldRef(root.Child("dynamicAllocation", "initialExecutors"))
		}
	}
	if crdExecutorInstances != nil {
		executorInstances, instRef = crdExecutorInstances, crdFieldRef(root.Child("executor", "instances"))
	}

	return validateDynamicAllocation(minRef, maxRef, initRef, instRef, minExecutors, maxExecutors, initialExecutors, executorInstances)
}

// parseDynamicAllocationSparkConf extracts dynamic allocation executor bounds and executor
// instances from sparkConf, unvalidated.
func parseDynamicAllocationSparkConf(sparkConf map[string]string) (minExecutors, maxExecutors, initialExecutors, executorInstances *int32, err error) {
	var errs []error

	parse := func(key string) *int32 {
		value, ok := sparkConf[key]
		if !ok {
			return nil
		}
		ref := sparkConfFieldRef(key)
		n, parseErr := strconv.ParseInt(strings.TrimSpace(value), 10, 32)
		if parseErr != nil {
			if errors.Is(parseErr, strconv.ErrRange) {
				errs = append(errs, fmt.Errorf("%s must be within the range of a 32-bit integer, got %q", ref, value))
			} else {
				errs = append(errs, fmt.Errorf("%s must be an integer, got %q", ref, value))
			}
			return nil
		}
		result := int32(n)
		return &result
	}

	minExecutors = parse(common.SparkDynamicAllocationMinExecutors)
	maxExecutors = parse(common.SparkDynamicAllocationMaxExecutors)
	initialExecutors = parse(common.SparkDynamicAllocationInitialExecutors)
	executorInstances = parse(common.SparkExecutorInstances)

	return minExecutors, maxExecutors, initialExecutors, executorInstances, errors.Join(errs...)
}
