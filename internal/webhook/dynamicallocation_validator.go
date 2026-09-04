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

	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

// validateDynamicAllocation is the single resolver for dynamic allocation executor bounds, shared
// by the CRD DynamicAllocation field and the equivalent sparkConf values. Spark tolerates
// initialExecutors and executorInstances falling outside [minExecutors, maxExecutors] and uses
// max(minExecutors, initialExecutors, executorInstances) as the initial number of executors, so
// those cases only warrant a warning rather than a hard error.
func validateDynamicAllocation(root *field.Path, minExecutors, maxExecutors, initialExecutors, executorInstances *int32) (admission.Warnings, error) {
	minPath := root.Child("dynamicAllocation", "minExecutors")
	maxPath := root.Child("dynamicAllocation", "maxExecutors")
	initPath := root.Child("dynamicAllocation", "initialExecutors")
	instPath := root.Child("executor", "instances")

	var errs []error
	minValid, maxValid, initValid := true, true, true

	if minExecutors != nil && *minExecutors < 0 {
		errs = append(errs, fmt.Errorf("%s must be non-negative, got %d", minPath, *minExecutors))
		minValid = false
	}
	if maxExecutors != nil && *maxExecutors <= 0 {
		errs = append(errs, fmt.Errorf("%s must be positive, got %d", maxPath, *maxExecutors))
		maxValid = false
	}
	if initialExecutors != nil && *initialExecutors < 0 {
		errs = append(errs, fmt.Errorf("%s must be non-negative, got %d", initPath, *initialExecutors))
		initValid = false
	}

	// Cross-field checks only make sense once the values involved are individually valid.
	if minValid && maxValid && minExecutors != nil && maxExecutors != nil && *minExecutors > *maxExecutors {
		errs = append(errs, fmt.Errorf("%s (%d) cannot be greater than %s (%d)", minPath, *minExecutors, maxPath, *maxExecutors))
	}

	var warnings admission.Warnings
	if initValid && initialExecutors != nil {
		if minValid && minExecutors != nil && *initialExecutors < *minExecutors {
			warnings = append(warnings, fmt.Sprintf("%s (%d) is less than %s (%d); %s will be used as the initial number of executors",
				initPath, *initialExecutors, minPath, *minExecutors, minPath))
		}
		if maxValid && maxExecutors != nil && *initialExecutors > *maxExecutors {
			warnings = append(warnings, fmt.Sprintf("%s (%d) is greater than %s (%d); %s will be used as the initial number of executors",
				initPath, *initialExecutors, maxPath, *maxExecutors, initPath))
		}
	}
	if maxValid && executorInstances != nil && maxExecutors != nil && *executorInstances > *maxExecutors {
		warnings = append(warnings, fmt.Sprintf("%s (%d) is greater than %s (%d); %s will be used as the initial number of executors",
			instPath, *executorInstances, maxPath, *maxExecutors, instPath))
	}

	return warnings, errors.Join(errs...)
}

// mergeAndValidateDynamicAllocation merges the CRD DynamicAllocation field and Executor.Instances
// with the equivalent sparkConf values (CRD wins per-field, since dynamicAllocationOption runs
// after sparkConfOption).
func mergeAndValidateDynamicAllocation(root *field.Path, crdEnabled bool, crdMinExecutors, crdMaxExecutors, crdInitialExecutors, crdExecutorInstances *int32, sparkConf map[string]string) (admission.Warnings, error) {
	var confEnabled bool
	if value, ok := sparkConf[common.SparkDynamicAllocationEnabled]; ok {
		var err error
		if confEnabled, err = strconv.ParseBool(value); err != nil {
			enabledPath := root.Child("dynamicAllocation", "enabled")
			return nil, fmt.Errorf("%s (%s) must be a boolean, got %q", enabledPath, common.SparkDynamicAllocationEnabled, value)
		}
	}
	if !crdEnabled && !confEnabled {
		return nil, nil
	}

	confMinExecutors, confMaxExecutors, confInitialExecutors, confExecutorInstances, err := parseDynamicAllocationSparkConf(root, sparkConf)
	if err != nil {
		return nil, err
	}

	minExecutors, maxExecutors, initialExecutors, executorInstances := confMinExecutors, confMaxExecutors, confInitialExecutors, confExecutorInstances
	if crdEnabled {
		if crdMinExecutors != nil {
			minExecutors = crdMinExecutors
		}
		if crdMaxExecutors != nil {
			maxExecutors = crdMaxExecutors
		}
		if crdInitialExecutors != nil {
			initialExecutors = crdInitialExecutors
		}
	}
	if crdExecutorInstances != nil {
		executorInstances = crdExecutorInstances
	}

	return validateDynamicAllocation(root, minExecutors, maxExecutors, initialExecutors, executorInstances)
}

// parseDynamicAllocationSparkConf extracts dynamic allocation executor bounds and executor
// instances from sparkConf, unvalidated. All malformed values are collected and reported together
// rather than failing on the first one encountered.
func parseDynamicAllocationSparkConf(root *field.Path, sparkConf map[string]string) (minExecutors, maxExecutors, initialExecutors, executorInstances *int32, err error) {
	var errs []error

	parse := func(path *field.Path, key string) *int32 {
		value, ok := sparkConf[key]
		if !ok {
			return nil
		}
		n, parseErr := strconv.ParseInt(value, 10, 32)
		if parseErr != nil {
			if errors.Is(parseErr, strconv.ErrRange) {
				errs = append(errs, fmt.Errorf("%s (%s) must be within the range of a 32-bit integer, got %q", path, key, value))
			} else {
				errs = append(errs, fmt.Errorf("%s (%s) must be an integer, got %q", path, key, value))
			}
			return nil
		}
		result := int32(n)
		return &result
	}

	minExecutors = parse(root.Child("dynamicAllocation", "minExecutors"), common.SparkDynamicAllocationMinExecutors)
	maxExecutors = parse(root.Child("dynamicAllocation", "maxExecutors"), common.SparkDynamicAllocationMaxExecutors)
	initialExecutors = parse(root.Child("dynamicAllocation", "initialExecutors"), common.SparkDynamicAllocationInitialExecutors)
	executorInstances = parse(root.Child("executor", "instances"), common.SparkExecutorInstances)

	return minExecutors, maxExecutors, initialExecutors, executorInstances, errors.Join(errs...)
}
