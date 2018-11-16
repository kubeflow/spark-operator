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

package v1alpha1

// SetSparkApplicationDefaults sets default values for certain fields of a SparkApplication.
func SetSparkApplicationDefaults(app *SparkApplication) {
	if app == nil {
		return
	}

	if app.Spec.Mode == "" {
		app.Spec.Mode = ClusterMode
	}

	if app.Spec.RestartPolicy.Type == "" {
		app.Spec.RestartPolicy.Type = Never
	}

	if app.Spec.RestartPolicy.Type != Never {
		// Default to 5 sec if the RestartPolicy is OnFailure or Always and these values aren't specified.
		if app.Spec.RestartPolicy.OnFailureRetryInterval == nil {
			app.Spec.RestartPolicy.OnFailureRetryInterval = new(int64)
			*app.Spec.RestartPolicy.OnFailureRetryInterval = 5
		}

		if app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval == nil {
			app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval = new(int64)
			*app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval = 5
		}
	}

	setDriverSpecDefaults(app.Spec.Driver)
	setExecutorSpecDefaults(app.Spec.Executor)
}

func setDriverSpecDefaults(spec DriverSpec) {
	if spec.Cores == nil {
		spec.Cores = new(float32)
		*spec.Cores = 1
	}
	if spec.Memory == nil {
		spec.Memory = new(string)
		*spec.Memory = "1g"
	}
}

func setExecutorSpecDefaults(spec ExecutorSpec) {
	if spec.Cores == nil {
		spec.Cores = new(float32)
		*spec.Cores = 1
	}
	if spec.Memory == nil {
		spec.Memory = new(string)
		*spec.Memory = "1g"
	}
	if spec.Instances == nil {
		spec.Instances = new(int32)
		*spec.Instances = 1
	}
}
