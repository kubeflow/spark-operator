package yunikorn

import (
	"encoding/json"
	"testing"

	v1 "k8s.io/api/core/v1"

	"github.com/stretchr/testify/assert"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/util"
)

func TestSchedule(t *testing.T) {
	testCases := []struct {
		name     string
		app      *v1beta2.SparkApplication
		expected []taskGroup
	}{
		{
			name: "Driver only",
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
		{
			name: "spark-pi-yunikorn",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeScala,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:     util.Int32Ptr(1),
							CoreLimit: util.StringPtr("1200m"),
							Memory:    util.StringPtr("512m"),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: util.Int32Ptr(2),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:     util.Int32Ptr(1),
							CoreLimit: util.StringPtr("1200m"),
							Memory:    util.StringPtr("512m"),
						},
					},
					BatchSchedulerOptions: &v1beta2.BatchSchedulerConfiguration{
						Queue: util.StringPtr("root.default"),
					},
				},
			},
			expected: []taskGroup{
				{
					Name:      "spark-driver",
					MinMember: 1,
					MinResource: map[string]string{
						"cpu":    "1",
						"memory": "896Mi", // 512Mi + 384Mi min overhead
					},
				},
				{
					Name:      "spark-executor",
					MinMember: 2,
					MinResource: map[string]string{
						"cpu":    "1",
						"memory": "896Mi", // 512Mi + 384Mi min overhead
					},
				},
			},
		},
		{
			name: "Dynamic allocation and memory overhead",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:                 v1beta2.SparkApplicationTypePython,
					MemoryOverheadFactor: util.StringPtr("0.3"),
					Driver: v1beta2.DriverSpec{
						CoreRequest: util.StringPtr("2000m"),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:  util.Int32Ptr(4),
							Memory: util.StringPtr("8g"),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: util.Int32Ptr(4),
						SparkPodSpec: v1beta2.SparkPodSpec{
							MemoryOverhead: util.StringPtr("2g"),
							Cores:          util.Int32Ptr(8),
							Memory:         util.StringPtr("64g"),
						},
					},
					DynamicAllocation: &v1beta2.DynamicAllocation{
						InitialExecutors: util.Int32Ptr(8),
						MinExecutors:     util.Int32Ptr(2),
					},
					BatchSchedulerOptions: &v1beta2.BatchSchedulerConfiguration{
						Queue: util.StringPtr("root.default"),
					},
				},
			},
			expected: []taskGroup{
				{
					Name:      "spark-driver",
					MinMember: 1,
					MinResource: map[string]string{
						"cpu":    "2000m",   // CoreRequest takes precedence over Cores
						"memory": "10649Mi", // 1024Mi * 8 * 1.3 (manually specified overhead)
					},
				},
				{
					Name:      "spark-executor",
					MinMember: 8, // Max of instances, dynamic allocation min and initial
					MinResource: map[string]string{
						"cpu":    "8",
						"memory": "67584Mi", // 1024Mi * 64 + 1024 * 2 (executor memory overhead takes precedence)
					},
				},
			},
		},
		{
			name: "Node selectors, tolerations, affinity and labels",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:         v1beta2.SparkApplicationTypePython,
					NodeSelector: map[string]string{"key": "value"},
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:        util.Int32Ptr(1),
							Memory:       util.StringPtr("1g"),
							NodeSelector: map[string]string{"key": "newvalue", "key2": "value2"},
							Tolerations: []v1.Toleration{
								{
									Key:      "example-key",
									Operator: v1.TolerationOpEqual,
									Value:    "example-value",
									Effect:   v1.TaintEffectNoSchedule,
								},
							},
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: util.Int32Ptr(1),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:  util.Int32Ptr(1),
							Memory: util.StringPtr("1g"),
							Affinity: &v1.Affinity{
								NodeAffinity: &v1.NodeAffinity{
									RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
										NodeSelectorTerms: []v1.NodeSelectorTerm{
											{
												MatchExpressions: []v1.NodeSelectorRequirement{
													{
														Key:      "another-key",
														Operator: v1.NodeSelectorOpIn,
														Values:   []string{"value1", "value2"},
													},
												},
											},
										},
									},
								},
							},
							Labels: map[string]string{"label": "value"},
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
					NodeSelector: map[string]string{"key": "newvalue", "key2": "value2"},
					Tolerations: []v1.Toleration{
						{
							Key:      "example-key",
							Operator: v1.TolerationOpEqual,
							Value:    "example-value",
							Effect:   v1.TaintEffectNoSchedule,
						},
					},
				},
				{
					Name:      "spark-executor",
					MinMember: 1,
					MinResource: map[string]string{
						"cpu":    "1",
						"memory": "1433Mi", // 1024Mi * 1.4 non-JVM overhead
					},
					NodeSelector: map[string]string{"key": "value"}, // No executor specific node-selector
					Affinity: &v1.Affinity{
						NodeAffinity: &v1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{
								NodeSelectorTerms: []v1.NodeSelectorTerm{
									{
										MatchExpressions: []v1.NodeSelectorRequirement{
											{
												Key:      "another-key",
												Operator: v1.NodeSelectorOpIn,
												Values:   []string{"value1", "value2"},
											},
										},
									},
								},
							},
						},
					},
					Labels: map[string]string{"label": "value"},
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

			options := tc.app.Spec.BatchSchedulerOptions
			if options != nil && options.Queue != nil {
				assert.Equal(t, *options.Queue, tc.app.Spec.Driver.Labels[queueLabel])
				assert.Equal(t, *options.Queue, tc.app.Spec.Executor.Labels[queueLabel])
			}
		})
	}
}
