package yunikorn

import (
	"encoding/json"
	"github.com/kubeflow/spark-operator/pkg/util"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kubeflow/spark-operator/api/v1beta2"
)

func TestSchedule(t *testing.T) {
	testCases := []struct {
		Name     string
		App      *v1beta2.SparkApplication
		Expected []taskGroup
	}{
		{
			Name: "Only driver",
			App: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores: util.Int32Ptr(2),
						},
					},
				},
			},
			Expected: []taskGroup{
				{
					Name:      "spark-driver",
					MinMember: 1,
				},
			},
		},
	}

	scheduler := &Scheduler{}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			marshalledExpected, err := json.Marshal(tc.Expected)
			if err != nil {
				t.Fatalf("Failed to marshal expected task groups: %v", err)
			}

			err = scheduler.Schedule(tc.App)
			assert.Nil(t, err)
			assert.JSONEq(t, string(marshalledExpected), tc.App.Spec.Driver.Annotations[TaskGroupsAnnotation])
		})
	}
}
