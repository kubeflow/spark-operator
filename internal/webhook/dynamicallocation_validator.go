/*
Copyright 2026 The Kubeflow authors.

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
	"strconv"

	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

// validateDynamicAllocation is the single resolver for dynamic allocation executor bounds, shared
// by the CRD DynamicAllocation field and the equivalent sparkConf values.
func validateDynamicAllocation(minExecutors, maxExecutors, initialExecutors *int32) (admission.Warnings, error) {
	if minExecutors != nil && maxExecutors != nil && *minExecutors > *maxExecutors {
		return nil, fmt.Errorf("minExecutors (%d) cannot be greater than maxExecutors (%d)", *minExecutors, *maxExecutors)
	}

	var warnings admission.Warnings
	if initialExecutors != nil {
		if minExecutors != nil && *initialExecutors < *minExecutors {
			warnings = append(warnings, fmt.Sprintf("initialExecutors (%d) is less than minExecutors (%d); minExecutors will be used as the initial number of executors",
				*initialExecutors, *minExecutors))
		}
		if maxExecutors != nil && *initialExecutors > *maxExecutors {
			return nil, fmt.Errorf("initialExecutors (%d) cannot be greater than maxExecutors (%d)", *initialExecutors, *maxExecutors)
		}
	}

	if minExecutors != nil && *minExecutors < 0 {
		return nil, fmt.Errorf("minExecutors must be non-negative, got %d", *minExecutors)
	}
	if maxExecutors != nil && *maxExecutors <= 0 {
		return nil, fmt.Errorf("maxExecutors must be positive, got %d", *maxExecutors)
	}
	if initialExecutors != nil && *initialExecutors < 0 {
		return nil, fmt.Errorf("initialExecutors must be non-negative, got %d", *initialExecutors)
	}

	return warnings, nil
}

// mergeAndValidateDynamicAllocation merges the CRD DynamicAllocation field with the equivalent
// sparkConf values (CRD wins per-field, since dynamicAllocationOption runs after sparkConfOption).
func mergeAndValidateDynamicAllocation(crdEnabled bool, crdMinExecutors, crdMaxExecutors, crdInitialExecutors *int32, sparkConf map[string]string) (admission.Warnings, error) {
	confEnabled, confMinExecutors, confMaxExecutors, confInitialExecutors, err := parseDynamicAllocationSparkConf(sparkConf)
	if err != nil {
		return nil, err
	}

	if !crdEnabled && !confEnabled {
		return nil, nil
	}

	minExecutors, maxExecutors, initialExecutors := confMinExecutors, confMaxExecutors, confInitialExecutors
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

	return validateDynamicAllocation(minExecutors, maxExecutors, initialExecutors)
}

// parseDynamicAllocationSparkConf extracts dynamic allocation values from sparkConf, unvalidated.
func parseDynamicAllocationSparkConf(sparkConf map[string]string) (enabled bool, minExecutors, maxExecutors, initialExecutors *int32, err error) {
	enabled, _ = strconv.ParseBool(sparkConf[common.SparkDynamicAllocationEnabled])

	parse := func(key string) (*int32, error) {
		value, ok := sparkConf[key]
		if !ok {
			return nil, nil
		}
		n, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("%s must be an integer, got %q", key, value)
		}
		result := int32(n)
		return &result, nil
	}

	if minExecutors, err = parse(common.SparkDynamicAllocationMinExecutors); err != nil {
		return false, nil, nil, nil, err
	}
	if maxExecutors, err = parse(common.SparkDynamicAllocationMaxExecutors); err != nil {
		return false, nil, nil, nil, err
	}
	if initialExecutors, err = parse(common.SparkDynamicAllocationInitialExecutors); err != nil {
		return false, nil, nil, nil, err
	}

	return enabled, minExecutors, maxExecutors, initialExecutors, nil
}
