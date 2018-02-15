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

func SetSparkApplicationDefaults(app *SparkApplication) {
	if app == nil {
		return
	}

	if app.Spec.Mode == "" {
		app.Spec.Mode = ClusterMode
	}

	if app.Spec.RestartPolicy == "" {
		app.Spec.RestartPolicy = Never
	}

	if app.Spec.MaxSubmissionRetries == nil {
		app.Spec.MaxSubmissionRetries = new(int32)
		*app.Spec.MaxSubmissionRetries = 3
	}

	if app.Spec.SubmissionRetryInterval == nil {
		app.Spec.SubmissionRetryInterval = new(int64)
		*app.Spec.SubmissionRetryInterval = 300
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
