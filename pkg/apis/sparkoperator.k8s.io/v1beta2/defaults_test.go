/*
Copyright 2019 Google LLC

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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetSparkApplicationDefaultsNilSparkApplicationShouldNotModifySparkApplication(t *testing.T) {
	var app *SparkApplication

	SetSparkApplicationDefaults(app)

	assert.Nil(t, app)
}

func TestSetSparkApplicationDefaultsEmptyModeShouldDefaultToClusterMode(t *testing.T) {
	app := &SparkApplication{
		Spec: SparkApplicationSpec{},
	}

	SetSparkApplicationDefaults(app)

	assert.Equal(t, ClusterMode, app.Spec.Mode)
}

func TestSetSparkApplicationDefaultsModeShouldNotChangeIfSet(t *testing.T) {
	expectedMode := ClientMode
	app := &SparkApplication{
		Spec: SparkApplicationSpec{
			Mode: expectedMode,
		},
	}

	SetSparkApplicationDefaults(app)

	assert.Equal(t, expectedMode, app.Spec.Mode)
}

func TestSetSparkApplicationDefaultsEmptyRestartPolicyShouldDefaultToNever(t *testing.T) {
	app := &SparkApplication{
		Spec: SparkApplicationSpec{},
	}

	SetSparkApplicationDefaults(app)

	assert.Equal(t, Never, app.Spec.RestartPolicy.Type)
}

func TestSetSparkApplicationDefaultsOnFailureRestartPolicyShouldSetDefaultValues(t *testing.T) {
	app := &SparkApplication{
		Spec: SparkApplicationSpec{
			RestartPolicy: RestartPolicy{
				Type: OnFailure,
			},
		},
	}

	SetSparkApplicationDefaults(app)

	assert.Equal(t, OnFailure, app.Spec.RestartPolicy.Type)
	assert.NotNil(t, app.Spec.RestartPolicy.OnFailureRetryInterval)
	assert.Equal(t, int64(5), *app.Spec.RestartPolicy.OnFailureRetryInterval)
	assert.NotNil(t, app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval)
	assert.Equal(t, int64(5), *app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval)
}

func TestSetSparkApplicationDefaultsOnFailureRestartPolicyShouldSetDefaultValueForOnFailureRetryInterval(t *testing.T) {
	expectedOnSubmissionFailureRetryInterval := int64(14)
	app := &SparkApplication{
		Spec: SparkApplicationSpec{
			RestartPolicy: RestartPolicy{
				Type:                             OnFailure,
				OnSubmissionFailureRetryInterval: &expectedOnSubmissionFailureRetryInterval,
			},
		},
	}

	SetSparkApplicationDefaults(app)

	assert.Equal(t, OnFailure, app.Spec.RestartPolicy.Type)
	assert.NotNil(t, app.Spec.RestartPolicy.OnFailureRetryInterval)
	assert.Equal(t, int64(5), *app.Spec.RestartPolicy.OnFailureRetryInterval)
	assert.NotNil(t, app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval)
	assert.Equal(t, expectedOnSubmissionFailureRetryInterval, *app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval)
}

func TestSetSparkApplicationDefaultsOnFailureRestartPolicyShouldSetDefaultValueForOnSubmissionFailureRetryInterval(t *testing.T) {
	expectedOnFailureRetryInterval := int64(10)
	app := &SparkApplication{
		Spec: SparkApplicationSpec{
			RestartPolicy: RestartPolicy{
				Type:                   OnFailure,
				OnFailureRetryInterval: &expectedOnFailureRetryInterval,
			},
		},
	}

	SetSparkApplicationDefaults(app)

	assert.Equal(t, OnFailure, app.Spec.RestartPolicy.Type)
	assert.NotNil(t, app.Spec.RestartPolicy.OnFailureRetryInterval)
	assert.Equal(t, expectedOnFailureRetryInterval, *app.Spec.RestartPolicy.OnFailureRetryInterval)
	assert.NotNil(t, app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval)
	assert.Equal(t, int64(5), *app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval)
}

func TestSetSparkApplicationDefaultsDriverSpecDefaults(t *testing.T) {

	//Case1: Driver config not set.
	app := &SparkApplication{
		Spec: SparkApplicationSpec{},
	}

	SetSparkApplicationDefaults(app)

	if app.Spec.Driver.Cores == nil {
		t.Error("Expected app.Spec.Driver.Cores not to be nil.")
	} else {
		assert.Equal(t, int32(1), *app.Spec.Driver.Cores)
	}

	if app.Spec.Driver.Memory == nil {
		t.Error("Expected app.Spec.Driver.Memory not to be nil.")
	} else {
		assert.Equal(t, "1g", *app.Spec.Driver.Memory)
	}

	//Case2: Driver config set via SparkConf.
	app = &SparkApplication{
		Spec: SparkApplicationSpec{
			SparkConf: map[string]string{
				"spark.driver.memory": "200M",
				"spark.driver.cores":  "1",
			},
		},
	}
	SetSparkApplicationDefaults(app)

	assert.Nil(t, app.Spec.Driver.Cores)
	assert.Nil(t, app.Spec.Driver.Memory)
}

func TestSetSparkApplicationDefaultsExecutorSpecDefaults(t *testing.T) {
	//Case1: Executor config not set.
	app := &SparkApplication{
		Spec: SparkApplicationSpec{},
	}

	SetSparkApplicationDefaults(app)

	if app.Spec.Executor.Cores == nil {
		t.Error("Expected app.Spec.Executor.Cores not to be nil.")
	} else {
		assert.Equal(t, int32(1), *app.Spec.Executor.Cores)
	}

	if app.Spec.Executor.Memory == nil {
		t.Error("Expected app.Spec.Executor.Memory not to be nil.")
	} else {
		assert.Equal(t, "1g", *app.Spec.Executor.Memory)
	}

	if app.Spec.Executor.Instances == nil {
		t.Error("Expected app.Spec.Executor.Instances not to be nil.")
	} else {
		assert.Equal(t, int32(1), *app.Spec.Executor.Instances)
	}

	//Case2: Executor config set via SparkConf.
	app = &SparkApplication{
		Spec: SparkApplicationSpec{
			SparkConf: map[string]string{
				"spark.executor.cores":     "2",
				"spark.executor.memory":    "500M",
				"spark.executor.instances": "3",
			},
		},
	}

	SetSparkApplicationDefaults(app)

	assert.Nil(t, app.Spec.Executor.Cores)
	assert.Nil(t, app.Spec.Executor.Memory)
	assert.Nil(t, app.Spec.Executor.Instances)

}
