package yunikorn

import (
	"github.com/kubeflow/spark-operator/pkg/util"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/kubeflow/spark-operator/api/v1beta2"
)

func TestGetInitialExecutors(t *testing.T) {
	testCases := []struct {
		Name     string
		App      *v1beta2.SparkApplication
		Expected int32
	}{
		{
			Name:     "Nothing specified",
			App:      &v1beta2.SparkApplication{},
			Expected: 0,
		},
		{
			Name: "Only instances",
			App: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Executor: v1beta2.ExecutorSpec{
						Instances: util.Int32Ptr(1),
					},
				},
			},
			Expected: 1,
		},
		{
			Name: "Only initial",
			App: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					DynamicAllocation: &v1beta2.DynamicAllocation{
						InitialExecutors: util.Int32Ptr(1),
					},
				},
			},
			Expected: 1,
		},
		{
			Name: "Only min",
			App: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					DynamicAllocation: &v1beta2.DynamicAllocation{
						MinExecutors: util.Int32Ptr(1),
					},
				},
			},
			Expected: 1,
		},
		{
			Name: "Instances and initial",
			App: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Executor: v1beta2.ExecutorSpec{
						Instances: util.Int32Ptr(2),
					},
					DynamicAllocation: &v1beta2.DynamicAllocation{
						InitialExecutors: util.Int32Ptr(1),
					},
				},
			},
			Expected: 2,
		},
		{
			Name: "Instances and min",
			App: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Executor: v1beta2.ExecutorSpec{
						Instances: util.Int32Ptr(2),
					},
					DynamicAllocation: &v1beta2.DynamicAllocation{
						MinExecutors: util.Int32Ptr(1),
					},
				},
			},
			Expected: 2,
		},
		{
			Name: "Initial and min",
			App: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					DynamicAllocation: &v1beta2.DynamicAllocation{
						InitialExecutors: util.Int32Ptr(2),
						MinExecutors:     util.Int32Ptr(1),
					},
				},
			},
			Expected: 2,
		},
		{
			Name: "All",
			App: &v1beta2.SparkApplication{
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
			Expected: 3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			assert.Equal(t, tc.Expected, getInitialExecutors(tc.App))
		})
	}
}
