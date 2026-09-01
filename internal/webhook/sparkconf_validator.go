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
	"strconv"

	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

type SparkConfKeyDeniedError struct {
	Key     string
	Message string
}

func (e *SparkConfKeyDeniedError) Error() string {
	return fmt.Sprintf("sparkConf key %q is not allowed: %s", e.Key, e.Message)
}

var deniedSparkConfKeys = map[string]string{
	common.SparkKubernetesAuthenticateDriverServiceAccountName:   "configure the service account via the CRD spec or pod template instead",
	common.SparkKubernetesAuthenticateExecutorServiceAccountName: "configure the service account via the CRD spec or pod template instead",
	common.SparkKubernetesAuthenticateOAuthTokenFile:             "authentication credentials are managed by the operator",
	common.SparkKubernetesAuthenticateOAuthToken:                 "authentication credentials are managed by the operator",
	common.SparkKubernetesAuthenticateDriverOAuthTokenFile:       "authentication credentials are managed by the operator",
	common.SparkKubernetesAuthenticateDriverOAuthToken:           "authentication credentials are managed by the operator",
	common.SparkKubernetesAuthenticateExecutorOAuthTokenFile:     "authentication credentials are managed by the operator",
	common.SparkKubernetesAuthenticateExecutorOAuthToken:         "authentication credentials are managed by the operator",
	common.SparkMaster:                           "this value is managed by the operator",
	common.SparkKubernetesDriverMaster:           "this value is managed by the operator",
	common.SparkKubernetesContainerImage:         "use the image field on the CRD instead",
	common.SparkKubernetesDriverContainerImage:   "use the image field on the CRD instead",
	common.SparkKubernetesExecutorContainerImage: "use the image field on the CRD instead",
}

func validateSparkConf(sparkConf map[string]string, namespace string) (admission.Warnings, error) {
	for key, value := range sparkConf {
		if msg, denied := deniedSparkConfKeys[key]; denied {
			return nil, &SparkConfKeyDeniedError{Key: key, Message: msg}
		}
		if key == common.SparkKubernetesNamespace && value != namespace {
			return nil, &SparkConfKeyDeniedError{
				Key:     key,
				Message: fmt.Sprintf("must equal the application namespace %q, got %q", namespace, value),
			}
		}
	}

	return validateDynamicAllocationSparkConf(sparkConf)
}

// validateDynamicAllocationSparkConf validates the dynamic allocation executor
// bounds when they are supplied directly through sparkConf instead of the CRD
// DynamicAllocation fields. It mirrors the checks in
// SparkApplicationValidator.validateDynamicAllocation.
func validateDynamicAllocationSparkConf(sparkConf map[string]string) (admission.Warnings, error) {
	if enabled, _ := strconv.ParseBool(sparkConf[common.SparkDynamicAllocationEnabled]); !enabled {
		return nil, nil
	}

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

	minExecutors, err := parse(common.SparkDynamicAllocationMinExecutors)
	if err != nil {
		return nil, err
	}
	maxExecutors, err := parse(common.SparkDynamicAllocationMaxExecutors)
	if err != nil {
		return nil, err
	}
	initialExecutors, err := parse(common.SparkDynamicAllocationInitialExecutors)
	if err != nil {
		return nil, err
	}

	// Validate minExecutors <= maxExecutors
	if minExecutors != nil && maxExecutors != nil && *minExecutors > *maxExecutors {
		return nil, fmt.Errorf("%s (%d) cannot be greater than %s (%d)",
			common.SparkDynamicAllocationMinExecutors, *minExecutors,
			common.SparkDynamicAllocationMaxExecutors, *maxExecutors)
	}

	// Validate initialExecutors is within range
	var warnings admission.Warnings
	if initialExecutors != nil {
		// initialExecutors below minExecutors is not an error: both Spark and the
		// operator raise the initial executor count to at least minExecutors (see
		// util.GetInitialExecutorNumber), so surface it as a warning instead.
		if minExecutors != nil && *initialExecutors < *minExecutors {
			warnings = append(warnings, fmt.Sprintf("%s (%d) is less than %s (%d); minExecutors will be used as the initial number of executors",
				common.SparkDynamicAllocationInitialExecutors, *initialExecutors,
				common.SparkDynamicAllocationMinExecutors, *minExecutors))
		}
		if maxExecutors != nil && *initialExecutors > *maxExecutors {
			return nil, fmt.Errorf("%s (%d) cannot be greater than %s (%d)",
				common.SparkDynamicAllocationInitialExecutors, *initialExecutors,
				common.SparkDynamicAllocationMaxExecutors, *maxExecutors)
		}
	}

	// Validate non-negative values. maxExecutors must be positive, while 0 is
	// allowed for minExecutors and initialExecutors.
	if minExecutors != nil && *minExecutors < 0 {
		return nil, fmt.Errorf("%s must be non-negative, got %d", common.SparkDynamicAllocationMinExecutors, *minExecutors)
	}
	if maxExecutors != nil && *maxExecutors <= 0 {
		return nil, fmt.Errorf("%s must be positive, got %d", common.SparkDynamicAllocationMaxExecutors, *maxExecutors)
	}
	if initialExecutors != nil && *initialExecutors < 0 {
		return nil, fmt.Errorf("%s must be non-negative, got %d", common.SparkDynamicAllocationInitialExecutors, *initialExecutors)
	}

	return warnings, nil
}
