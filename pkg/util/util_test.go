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

package util_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
)

var expectedStatusString = `{
  "sparkApplicationId": "test-app",
  "submissionID": "test-app-submission",
  "lastSubmissionAttemptTime": null,
  "terminationTime": null,
  "driverInfo": {},
  "applicationState": {
    "state": "COMPLETED"
  },
  "executorState": {
    "executor-1": "COMPLETED"
  }
}`

func TestGetDriverAnnotationOption(t *testing.T) {
	key := "custom-key"
	value := "custom-value"
	expected := fmt.Sprintf("%s%s=%s", common.SparkKubernetesDriverAnnotationPrefix, key, value)
	actual := util.GetDriverAnnotationOption(key, value)
	assert.Equal(t, expected, actual)
}

func TestGetExecutorAnnotationOption(t *testing.T) {
	key := "custom-key"
	value := "custom-value"
	expected := fmt.Sprintf("%s%s=%s", common.SparkKubernetesExecutorAnnotationPrefix, key, value)
	actual := util.GetExecutorAnnotationOption(key, value)
	assert.Equal(t, expected, actual)
}

func TestPrintStatus(t *testing.T) {
	status := &v1beta2.SparkApplicationStatus{
		SparkApplicationID: "test-app",
		SubmissionID:       "test-app-submission",
		AppState: v1beta2.ApplicationState{
			State: v1beta2.ApplicationStateCompleted,
		},
		ExecutorState: map[string]v1beta2.ExecutorState{
			"executor-1": v1beta2.ExecutorStateCompleted,
		},
	}

	statusString, err := util.PrintStatus(status)
	if err != nil {
		t.Fail()
	}

	if statusString != expectedStatusString {
		t.Errorf("status string\n %s is different from expected status string\n %s", statusString, expectedStatusString)
	}
}
