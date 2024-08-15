package yunikorn

import (
	"encoding/json"
	"testing"

	"github.com/kubeflow/spark-operator/pkg/util"

	"github.com/stretchr/testify/assert"

	"github.com/kubeflow/spark-operator/api/v1beta2"
)

func TestSchedule(t *testing.T) {
	testCases := []struct {
		name     string
		app      *v1beta2.SparkApplication
		expected []taskGroup
	}{
		{
			name: "Only driver",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypePython,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:  util.Int32Ptr(1),
							Memory: util.StringPtr("1g"),
						},
					},
				},
			},
			expected: []taskGroup{
				{
					Name:      "spark-driver",
					MinMember: 1,
					MinResource: map[string]string{
						"cpu":    "1",
						"memory": "1433Mi", // 1024Mi * 1.4 non-JVM overhead
					},
				},
			},
		},
	}

	scheduler := &Scheduler{}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			marshalledExpected, err := json.Marshal(tc.expected)
			if err != nil {
				t.Fatalf("Failed to marshal expected task groups: %v", err)
			}

			err = scheduler.Schedule(tc.app)
			assert.Nil(t, err)
			assert.JSONEq(t, string(marshalledExpected), tc.app.Spec.Driver.Annotations[taskGroupsAnnotation])
		})
	}
}
