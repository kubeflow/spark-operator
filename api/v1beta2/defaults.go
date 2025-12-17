/*
Copyright 2017 Google LLC

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

package v1beta2

import (
	"strconv"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
)

func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// RegisterDefaults adds defaulting functions to the given scheme.
// All generated defaulters are covering - they call all nested defaulters.
func RegisterDefaults(scheme *runtime.Scheme) error {
	scheme.AddTypeDefaultingFunc(&SparkApplication{}, func(obj interface{}) {
		SetSparkApplicationDefaults(obj.(*SparkApplication))
	})
	return nil
}

// SetSparkApplicationDefaults sets default values for certain fields of a SparkApplication.
func SetSparkApplicationDefaults(app *SparkApplication) {
	if app == nil {
		return
	}

	if app.Spec.Type == "" {
		app.Spec.Type = SparkApplicationTypeScala
	}

	if app.Spec.Mode == "" {
		app.Spec.Mode = DeployModeCluster
	}

	if app.Spec.RestartPolicy.Type == "" {
		app.Spec.RestartPolicy.Type = RestartPolicyNever
	}

	if app.Spec.RestartPolicy.Type != RestartPolicyNever {
		// Default to 5 sec if the RestartPolicy is OnFailure or Always and these values aren't specified.
		if app.Spec.RestartPolicy.OnFailureRetryInterval == nil {
			app.Spec.RestartPolicy.OnFailureRetryInterval = ptr.To
		}

		if app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval == nil {
			app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval = ptr.To
		}
	}

	setDriverSpecDefaults(&app.Spec.Driver, app.Spec.SparkConf)
	setExecutorSpecDefaults(&app.Spec.Executor, app.Spec.SparkConf, app.Spec.DynamicAllocation)
}

func setDriverSpecDefaults(spec *DriverSpec, sparkConf map[string]string) {
	// Default Cores = 1 if not set
	if _, exists := sparkConf["spark.driver.cores"]; !exists && spec.Cores == nil {
		v := intstr.FromInt(1)
		spec.Cores = &v
	}

	// Default Memory = 1g if not set
	if _, exists := sparkConf["spark.driver.memory"]; !exists && spec.Memory == nil {
		spec.Memory = ptr.To("1g")
	}
}

func setExecutorSpecDefaults(spec *ExecutorSpec, sparkConf map[string]string, allocSpec *DynamicAllocation) {
	// Default Cores = 1 if not set
	if _, exists := sparkConf["spark.executor.cores"]; !exists && spec.Cores == nil {
		v := intstr.FromInt(1)
		spec.Cores = &v
	}

	// Default Memory = 1g if not set
	if _, exists := sparkConf["spark.executor.memory"]; !exists && spec.Memory == nil {
		spec.Memory = ptr.To("1g")
	}

	isDynamicAllocationEnabled := isDynamicAllocationEnabled(sparkConf, allocSpec)

	// Default executor instances = 1 (when not using dynamic allocation)
	if spec.Instances == nil &&
		sparkConf["spark.executor.instances"] == "" &&
		!isDynamicAllocationEnabled {
		spec.Instances = ptr.To
	}

	// ShuffleTrackingEnabled default = true only when dynamic allocation is turned on
	if isDynamicAllocationEnabled && allocSpec != nil && allocSpec.ShuffleTrackingEnabled == nil {
		allocSpec.ShuffleTrackingEnabled = ptr.To(true)
	}
}

func isDynamicAllocationEnabled(sparkConf map[string]string, allocSpec *DynamicAllocation) bool {
	if allocSpec != nil {
		return allocSpec.Enabled
	}
	dynamicAllocationConfVal, _ := strconv.ParseBool(sparkConf["spark.dynamicallocation.enabled"])
	return dynamicAllocationConfVal
}
