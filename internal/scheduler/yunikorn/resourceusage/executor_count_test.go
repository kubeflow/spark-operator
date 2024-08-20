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

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/util"
)

func TestNumInitialExecutors(t *testing.T) {
	testCases := []struct {
		name     string
		app      *v1beta2.SparkApplication
		expected int32
	}{
		{
			name:     "Nothing specified",
			app:      &v1beta2.SparkApplication{},
			expected: 0,
		},
		{
			name: "Only instances",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Executor: v1beta2.ExecutorSpec{
						Instances: util.Int32Ptr(1),
					},
				},
			},
			expected: 1,
		},
		{
			name: "Only initial",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					DynamicAllocation: &v1beta2.DynamicAllocation{
						InitialExecutors: util.Int32Ptr(1),
					},
				},
			},
			expected: 1,
		},
		{
			name: "Only min",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					DynamicAllocation: &v1beta2.DynamicAllocation{
						MinExecutors: util.Int32Ptr(1),
					},
				},
			},
			expected: 1,
		},
		{
			name: "Instances and initial",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Executor: v1beta2.ExecutorSpec{
						Instances: util.Int32Ptr(2),
					},
					DynamicAllocation: &v1beta2.DynamicAllocation{
						InitialExecutors: util.Int32Ptr(1),
					},
				},
			},
			expected: 2,
		},
		{
			name: "Instances and min",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Executor: v1beta2.ExecutorSpec{
						Instances: util.Int32Ptr(2),
					},
					DynamicAllocation: &v1beta2.DynamicAllocation{
						MinExecutors: util.Int32Ptr(1),
					},
				},
			},
			expected: 2,
		},
		{
			name: "Initial and min",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					DynamicAllocation: &v1beta2.DynamicAllocation{
						InitialExecutors: util.Int32Ptr(2),
						MinExecutors:     util.Int32Ptr(1),
					},
				},
			},
			expected: 2,
		},
		{
			name: "All",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Executor: v1beta2.ExecutorSpec{
						Instances: util.Int32Ptr(3),
					},
					DynamicAllocation: &v1beta2.DynamicAllocation{
						InitialExecutors: util.Int32Ptr(2),
						MinExecutors:     util.Int32Ptr(1),
					},
				},
			},
			expected: 3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, NumInitialExecutors(tc.app))
		})
	}
}
