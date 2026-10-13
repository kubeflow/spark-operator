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
	testCases := []struct {
		name     string
		app      *v1beta2.SparkApplication
		expected map[string]string
	}{
		{
			name: "defaults when neither cores nor memory are set",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{Type: v1beta2.SparkApplicationTypeScala},
			},
			expected: map[string]string{"cpu": "1", "memory": "384Mi"},
		},
		{
			name: "cores and memory",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeScala,
					Driver: v1beta2.DriverSpec{SparkPodSpec: v1beta2.SparkPodSpec{
						Cores:  ptr.To[int32](2),
						Memory: ptr.To("4g"),
					}},
				},
			},
			expected: map[string]string{"cpu": "2", "memory": "4505Mi"},
		},
		{
			name: "core request takes precedence over cores",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeScala,
					Driver: v1beta2.DriverSpec{
						CoreRequest:  ptr.To("500m"),
						SparkPodSpec: v1beta2.SparkPodSpec{Cores: ptr.To[int32](2), Memory: ptr.To("512m")},
					},
				},
			},
			expected: map[string]string{"cpu": "500m", "memory": "896Mi"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := DriverPodRequests(tc.app)
			assert.Nil(t, err)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestDriverPodRequestsInvalid(t *testing.T) {
	testCases := []struct {
		name string
		app  *v1beta2.SparkApplication
	}{
		{
			name: "invalid core request",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:   v1beta2.SparkApplicationTypeScala,
					Driver: v1beta2.DriverSpec{CoreRequest: ptr.To("abc")},
				},
			},
		},
		{
			name: "invalid memory",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:   v1beta2.SparkApplicationTypeScala,
					Driver: v1beta2.DriverSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("512ub")}},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DriverPodRequests(tc.app)
			assert.NotNil(t, err)
		})
	}
}

func TestExecutorPodRequests(t *testing.T) {
	testCases := []struct {
		name     string
		app      *v1beta2.SparkApplication
		expected map[string]string
	}{
		{
			name: "defaults when neither cores nor memory are set",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{Type: v1beta2.SparkApplicationTypeScala},
			},
			expected: map[string]string{"cpu": "1", "memory": "384Mi"},
		},
		{
			name: "cores and memory",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeScala,
					Executor: v1beta2.ExecutorSpec{SparkPodSpec: v1beta2.SparkPodSpec{
						Cores:  ptr.To[int32](4),
						Memory: ptr.To("4g"),
					}},
				},
			},
			expected: map[string]string{"cpu": "4", "memory": "4505Mi"},
		},
		{
			name: "python executor includes pyspark memory",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:      v1beta2.SparkApplicationTypePython,
					SparkConf: map[string]string{"spark.executor.pyspark.memory": "512"},
					Executor: v1beta2.ExecutorSpec{
						CoreRequest:  ptr.To("500m"),
						SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("512m")},
					},
				},
			},
			expected: map[string]string{"cpu": "500m", "memory": "1408Mi"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := ExecutorPodRequests(tc.app)
			assert.Nil(t, err)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestExecutorPodRequestsInvalid(t *testing.T) {
	testCases := []struct {
		name string
		app  *v1beta2.SparkApplication
	}{
		{
			name: "invalid core request",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:     v1beta2.SparkApplicationTypeScala,
					Executor: v1beta2.ExecutorSpec{CoreRequest: ptr.To("abc")},
				},
			},
		},
		{
			name: "invalid memory",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:     v1beta2.SparkApplicationTypeScala,
					Executor: v1beta2.ExecutorSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("512ub")}},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ExecutorPodRequests(tc.app)
			assert.NotNil(t, err)
		})
	}
}
