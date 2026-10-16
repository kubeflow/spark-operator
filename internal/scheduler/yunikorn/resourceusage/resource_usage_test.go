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

package resourceusage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

func TestCpuRequest(t *testing.T) {
	testCases := []struct {
		cores       *int32
		coreRequest *string
		expected    string
	}{
		{nil, nil, "1"},
		{ptr.To[int32](1), nil, "1"},
		{nil, ptr.To("1"), "1"},
		{ptr.To[int32](1), ptr.To("500m"), "500m"},
	}

	for _, tc := range testCases {
		actual, err := cpuRequest(tc.cores, tc.coreRequest)
		assert.Nil(t, err)
		assert.Equal(t, tc.expected, actual)
	}
}

func TestCpuRequestInvalid(t *testing.T) {
	invalidInputs := []string{
		"",
		"asd",
		"Random 500m",
	}

	for _, input := range invalidInputs {
		_, err := cpuRequest(nil, &input)
		assert.NotNil(t, err)
	}
}

func TestDriverPodRequests(t *testing.T) {
	app := &v1beta2.SparkApplication{Spec: v1beta2.SparkApplicationSpec{
		Type: v1beta2.SparkApplicationTypeJava,
		Driver: v1beta2.DriverSpec{
			CoreRequest:  ptr.To("500m"),
			SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("1g")},
		},
	}}

	actual, err := DriverPodRequests(app)
	assert.Nil(t, err)
	assert.Equal(t, map[string]string{"cpu": "500m", "memory": "1408Mi"}, actual)

	_, err = DriverPodRequests(&v1beta2.SparkApplication{Spec: v1beta2.SparkApplicationSpec{
		Driver: v1beta2.DriverSpec{CoreRequest: ptr.To("bad")},
	}})
	assert.NotNil(t, err)
}

func TestExecutorPodRequests(t *testing.T) {
	app := &v1beta2.SparkApplication{Spec: v1beta2.SparkApplicationSpec{
		Type: v1beta2.SparkApplicationTypeJava,
		Executor: v1beta2.ExecutorSpec{
			CoreRequest:  ptr.To("500m"),
			SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("1g")},
		},
	}}

	actual, err := ExecutorPodRequests(app)
	assert.Nil(t, err)
	assert.Equal(t, map[string]string{"cpu": "500m", "memory": "1408Mi"}, actual)

	_, err = ExecutorPodRequests(&v1beta2.SparkApplication{Spec: v1beta2.SparkApplicationSpec{
		Executor: v1beta2.ExecutorSpec{CoreRequest: ptr.To("bad")},
	}})
	assert.NotNil(t, err)
}
