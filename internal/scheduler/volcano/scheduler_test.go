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

package volcano

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	fakevolcanoclientset "volcano.sh/apis/pkg/client/clientset/versioned/fake"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/internal/scheduler/resourceusage"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

func TestSchedule(t *testing.T) {
	testCases := []struct {
		name                 string
		app                  *v1beta2.SparkApplication
		expectedQueue        string
		expectedPriorityName string
		expectedMode         string
	}{
		{
			name: "Client mode with queue and priority",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeClient,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances:    ptr.To[int32](1),
						SparkPodSpec: v1beta2.SparkPodSpec{},
					},
					BatchSchedulerOptions: &v1beta2.BatchSchedulerConfiguration{
						Queue:             ptr.To("high-priority"),
						PriorityClassName: ptr.To("high"),
					},
				},
			},
			expectedQueue:        "high-priority",
			expectedPriorityName: "high",
			expectedMode:         "client",
		},
		{
			name: "Cluster mode with queue",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app-cluster",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeCluster,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances:    ptr.To[int32](1),
						SparkPodSpec: v1beta2.SparkPodSpec{},
					},
					BatchSchedulerOptions: &v1beta2.BatchSchedulerConfiguration{
						Queue: ptr.To("batch-queue"),
					},
				},
			},
			expectedQueue:        "batch-queue",
			expectedPriorityName: "",
			expectedMode:         "cluster",
		},
		{
			name: "Client mode with custom resources",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app-resources",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeClient,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("2g"),
							Cores:  ptr.To[int32](1),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: ptr.To[int32](2),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("1g"),
							Cores:  ptr.To[int32](1),
						},
					},
					BatchSchedulerOptions: &v1beta2.BatchSchedulerConfiguration{
						Resources: corev1.ResourceList{
							corev1.ResourceCPU:         resource.MustParse("4"),
							corev1.ResourceMemory:      resource.MustParse("8Gi"),
							corev1.ResourceName("gpu"): resource.MustParse("2"),
						},
					},
				},
			},
			expectedQueue:        "",
			expectedPriorityName: "",
			expectedMode:         "client",
		},
		{
			name: "Client mode with nil BatchSchedulerOptions",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app-client-nil-options",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeClient,
					Type: v1beta2.SparkApplicationTypeJava,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("2g"),
							Cores:  ptr.To[int32](1),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: ptr.To[int32](2),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("1g"),
							Cores:  ptr.To[int32](1),
						},
					},
					BatchSchedulerOptions: nil,
				},
			},
			expectedQueue:        "",
			expectedPriorityName: "",
			expectedMode:         "client",
		},
		{
			name: "Cluster mode with custom resources",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app-cluster-resources",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeCluster,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("2g"),
							Cores:  ptr.To[int32](1),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: ptr.To[int32](3),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("1g"),
							Cores:  ptr.To[int32](1),
						},
					},
					BatchSchedulerOptions: &v1beta2.BatchSchedulerConfiguration{
						Queue: ptr.To("gpu-queue"),
						Resources: corev1.ResourceList{
							corev1.ResourceCPU:         resource.MustParse("6"),
							corev1.ResourceMemory:      resource.MustParse("12Gi"),
							corev1.ResourceName("gpu"): resource.MustParse("4"),
						},
					},
				},
			},
			expectedQueue:        "gpu-queue",
			expectedPriorityName: "",
			expectedMode:         "cluster",
		},
		{
			name: "Cluster mode with nil BatchSchedulerOptions",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app-cluster-nil-options",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeCluster,
					Type: v1beta2.SparkApplicationTypeJava,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("2g"),
							Cores:  ptr.To[int32](1),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: ptr.To[int32](2),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("1g"),
							Cores:  ptr.To[int32](1),
						},
					},
					BatchSchedulerOptions: nil,
				},
			},
			expectedQueue:        "",
			expectedPriorityName: "",
			expectedMode:         "cluster",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.app.Annotations = make(map[string]string)
			tc.app.Spec.Driver.Annotations = make(map[string]string)
			tc.app.Spec.Executor.Annotations = make(map[string]string)

			var capturedPodGroup *v1beta1.PodGroup
			mockVolcanoClient := fakevolcanoclientset.NewSimpleClientset()

			mockVolcanoClient.PrependReactor("create", "podgroups", func(action clienttesting.Action) (bool, runtime.Object, error) {
				createAction := action.(clienttesting.CreateAction)
				capturedPodGroup = createAction.GetObject().(*v1beta1.PodGroup)
				return false, capturedPodGroup, nil
			})

			scheduler := &Scheduler{
				volcanoClient: mockVolcanoClient,
			}

			err := scheduler.Schedule(tc.app)

			assert.NoError(t, err)
			assert.NotNil(t, capturedPodGroup)

			if tc.expectedQueue != "" {
				assert.Equal(t, tc.expectedQueue, capturedPodGroup.Spec.Queue)
			}
			if tc.expectedPriorityName != "" {
				assert.Equal(t, tc.expectedPriorityName, capturedPodGroup.Spec.PriorityClassName)
			}

			switch tc.expectedMode {
			case "client":
				assert.Contains(t, tc.app.Spec.Executor.Annotations, v1beta1.KubeGroupNameAnnotationKey)
				assert.NotContains(t, tc.app.Spec.Driver.Annotations, v1beta1.KubeGroupNameAnnotationKey)
			case "cluster":
				assert.Contains(t, tc.app.Spec.Driver.Annotations, v1beta1.KubeGroupNameAnnotationKey)
				assert.Contains(t, tc.app.Spec.Executor.Annotations, v1beta1.KubeGroupNameAnnotationKey)
			}

			expectedPodGroupName := getPodGroupName(tc.app)
			assert.Equal(t, expectedPodGroupName, capturedPodGroup.Name)

			assert.Len(t, capturedPodGroup.OwnerReferences, 1)
			assert.Equal(t, tc.app.Name, capturedPodGroup.OwnerReferences[0].Name)
			assert.Equal(t, "SparkApplication", capturedPodGroup.OwnerReferences[0].Kind)

			// Verify custom resources if specified — these bypass the computed path entirely.
			if tc.app.Spec.BatchSchedulerOptions != nil && len(tc.app.Spec.BatchSchedulerOptions.Resources) > 0 {
				assert.NotNil(t, capturedPodGroup.Spec.MinResources)
				for resourceName, expectedQuantity := range tc.app.Spec.BatchSchedulerOptions.Resources {
					actualQuantity := capturedPodGroup.Spec.MinResources.Name(resourceName, resource.DecimalSI)
					assert.Equal(t, expectedQuantity.Value(), actualQuantity.Value(),
						"Resource %s quantity should match in PodGroup MinResources", resourceName)
				}
			}

			// Expected values come from the resourceusage-based helpers, which apply the default memoryOverheadFactor.
			if tc.app.Spec.BatchSchedulerOptions == nil {
				assert.NotNil(t, capturedPodGroup.Spec.MinResources)

				var expectedResources corev1.ResourceList
				if tc.expectedMode == "cluster" {
					driverRes, err := resourceusage.DriverPodResourceList(tc.app)
					require.NoError(t, err)
					execRes, err := executorMinResources(tc.app)
					require.NoError(t, err)
					expectedResources = util.SumResourceList([]corev1.ResourceList{driverRes, execRes})
				} else {
					var err error
					expectedResources, err = executorMinResources(tc.app)
					require.NoError(t, err)
				}

				for resourceName, expectedQuantity := range expectedResources {
					actualQuantity := capturedPodGroup.Spec.MinResources.Name(resourceName, resource.DecimalSI)
					assert.Equal(t, expectedQuantity.Value(), actualQuantity.Value(),
						"Resource %s quantity should match calculated resources", resourceName)
				}
			}
		})
	}
}

// TestScheduleOverheadFactor is the regression test for issue #2244.
// It verifies that Volcano PodGroup minResources includes the default
// memoryOverheadFactor (0.1 for JVM, 0.4 for non-JVM) when no explicit
// memoryOverhead is set on the SparkApplication.
func TestScheduleOverheadFactor(t *testing.T) {
	testCases := []struct {
		name string
		app  *v1beta2.SparkApplication
		// expectedDriverMemMi is the expected driver memory in the PodGroup in MiB.
		// Formula: heap + max(heap*factor, 384Mi)
		expectedDriverMemMi int64
		// expectedExecTotalMemMi is total executor memory = instances * per-pod memory.
		expectedExecTotalMemMi int64
		mode                   v1beta2.DeployMode
	}{
		{
			// JVM app, 2g driver, 1g executor × 2 instances, default factor 0.1
			// driver:   2048 + max(2048*0.1, 384) = 2048 + 384 = 2432 Mi
			// executor: (1024 + max(1024*0.1, 384)) * 2 = (1024+384)*2 = 2816 Mi
			name: "JVM default overhead factor client mode",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{Name: "jvm-default", Namespace: "default"},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeClient,
					Type: v1beta2.SparkApplicationTypeJava,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("2g"),
							Cores:  ptr.To[int32](1),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: ptr.To[int32](2),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("1g"),
							Cores:  ptr.To[int32](1),
						},
					},
				},
			},
			expectedDriverMemMi:    2432,
			expectedExecTotalMemMi: 2816,
			mode:                   v1beta2.DeployModeClient,
		},
		{
			// JVM app cluster mode — minResources = driver + 2 executors
			// driver: 2048 + 384 = 2432 Mi; executors: 2816 Mi; total: 5248 Mi
			name: "JVM default overhead factor cluster mode",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{Name: "jvm-cluster", Namespace: "default"},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeCluster,
					Type: v1beta2.SparkApplicationTypeJava,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("2g"),
							Cores:  ptr.To[int32](2),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: ptr.To[int32](2),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("1g"),
							Cores:  ptr.To[int32](1),
						},
					},
				},
			},
			expectedDriverMemMi:    2432,
			expectedExecTotalMemMi: 2816,
			mode:                   v1beta2.DeployModeCluster,
		},
		{
			// Python app, 1g executor × 1 instance, default factor 0.4
			// executor: 1024 + max(1024*0.4, 384) = 1024 + 409 = 1433 Mi  (floor)
			name: "Python default overhead factor client mode",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{Name: "python-default", Namespace: "default"},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeClient,
					Type: v1beta2.SparkApplicationTypePython,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("1g"),
							Cores:  ptr.To[int32](1),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: ptr.To[int32](1),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("1g"),
							Cores:  ptr.To[int32](1),
						},
					},
				},
			},
			expectedDriverMemMi:    1433,
			expectedExecTotalMemMi: 1433,
			mode:                   v1beta2.DeployModeClient,
		},
		{
			// Dynamic allocation: GetInitialExecutorNumber returns
			//   max(Instances, InitialExecutors, MinExecutors) from app.Spec.DynamicAllocation.
			// Here InitialExecutors=4 > MinExecutors=2, so executor count = 4.
			// executor per pod: 1024 + max(1024*0.1, 384) = 1024 + 384 = 1408 Mi
			// total = 1408 * 4 = 5632 Mi
			name: "JVM dynamic allocation uses initial executor count",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{Name: "jvm-dynalloc", Namespace: "default"},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeClient,
					Type: v1beta2.SparkApplicationTypeJava,
					DynamicAllocation: &v1beta2.DynamicAllocation{
						Enabled:          true,
						MinExecutors:     ptr.To[int32](2),
						MaxExecutors:     ptr.To[int32](10),
						InitialExecutors: ptr.To[int32](4),
					},
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("1g"),
							Cores:  ptr.To[int32](1),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						// Instances intentionally nil — dynamic allocation controls count.
						// InitialExecutors=4 wins over MinExecutors=2.
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("1g"),
							Cores:  ptr.To[int32](1),
						},
					},
				},
			},
			expectedDriverMemMi:    1408,
			expectedExecTotalMemMi: 5632,
			mode:                   v1beta2.DeployModeClient,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.app.Annotations = make(map[string]string)
			tc.app.Spec.Driver.Annotations = make(map[string]string)
			tc.app.Spec.Executor.Annotations = make(map[string]string)

			var capturedPodGroup *v1beta1.PodGroup
			mockVolcanoClient := fakevolcanoclientset.NewSimpleClientset()
			mockVolcanoClient.PrependReactor("create", "podgroups", func(action clienttesting.Action) (bool, runtime.Object, error) {
				createAction := action.(clienttesting.CreateAction)
				capturedPodGroup = createAction.GetObject().(*v1beta1.PodGroup)
				return false, capturedPodGroup, nil
			})

			sched := &Scheduler{volcanoClient: mockVolcanoClient}
			require.NoError(t, sched.Schedule(tc.app))
			require.NotNil(t, capturedPodGroup)
			require.NotNil(t, capturedPodGroup.Spec.MinResources)

			minResources := *capturedPodGroup.Spec.MinResources
			actualMemory := minResources[corev1.ResourceMemory]

			switch tc.mode {
			case v1beta2.DeployModeClient:
				// Client mode: minResources = executor total only
				expectedMem := resource.MustParse(fmt.Sprintf("%dMi", tc.expectedExecTotalMemMi))
				assert.Equal(t, expectedMem.Value(), actualMemory.Value(),
					"client mode: PodGroup memory should equal total executor memory (with overhead); got %s, want %s",
					actualMemory.String(), expectedMem.String())

			case v1beta2.DeployModeCluster:
				// Cluster mode: minResources = driver + executor total
				expectedMem := resource.MustParse(fmt.Sprintf("%dMi", tc.expectedDriverMemMi+tc.expectedExecTotalMemMi))
				assert.Equal(t, expectedMem.Value(), actualMemory.Value(),
					"cluster mode: PodGroup memory should equal driver + total executor memory (with overhead); got %s, want %s",
					actualMemory.String(), expectedMem.String())
			}
		})
	}
}

// TestBatchSchedulerOptionsOverride verifies that batchSchedulerOptions.resources is checked
// before the resource calculation runs, in both client and cluster mode.
func TestBatchSchedulerOptionsOverride(t *testing.T) {
	// explicitResources is the fixed override that all "override set" cases use.
	explicitResources := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("10"),
		corev1.ResourceMemory: resource.MustParse("20Gi"),
	}

	// badMemory is an invalid memory string that would cause executorMinResources to
	// return an error if it were ever called.
	badMemory := ptr.To("not-a-valid-memory-value")

	testCases := []struct {
		name            string
		app             *v1beta2.SparkApplication
		mode            v1beta2.DeployMode
		expectError     bool
		expectResources corev1.ResourceList // nil means "computed, not overridden"
		expectMemoryMiB int64               // used only when expectResources is nil
	}{
		// ---- Client mode ----

		{
			// Override set → minResources equals the override verbatim; no calculation runs.
			name: "client mode: override set",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{Name: "app-co", Namespace: "default"},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeClient,
					Type: v1beta2.SparkApplicationTypeJava,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("1g"), Cores: ptr.To[int32](1)},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances:    ptr.To[int32](2),
						SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("1g"), Cores: ptr.To[int32](1)},
					},
					BatchSchedulerOptions: &v1beta2.BatchSchedulerConfiguration{
						Resources: explicitResources,
					},
				},
			},
			mode:            v1beta2.DeployModeClient,
			expectResources: explicitResources,
		},
		{
			// Override set AND spec has invalid memory → must still succeed, use override.
			name: "client mode: override set, invalid memory string",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{Name: "app-ci", Namespace: "default"},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeClient,
					Type: v1beta2.SparkApplicationTypeJava,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{Memory: badMemory, Cores: ptr.To[int32](1)},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances:    ptr.To[int32](2),
						SparkPodSpec: v1beta2.SparkPodSpec{Memory: badMemory, Cores: ptr.To[int32](1)},
					},
					BatchSchedulerOptions: &v1beta2.BatchSchedulerConfiguration{
						Resources: explicitResources,
					},
				},
			},
			mode:            v1beta2.DeployModeClient,
			expectResources: explicitResources,
		},
		{
			// No override, invalid memory string → must return error.
			name: "client mode: no override, invalid memory string → error",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{Name: "app-ce", Namespace: "default"},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeClient,
					Type: v1beta2.SparkApplicationTypeJava,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("1g"), Cores: ptr.To[int32](1)},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances:    ptr.To[int32](2),
						SparkPodSpec: v1beta2.SparkPodSpec{Memory: badMemory, Cores: ptr.To[int32](1)},
					},
				},
			},
			mode:        v1beta2.DeployModeClient,
			expectError: true,
		},
		{
			// Empty override map (len == 0) → falls through to calculation.
			// JVM, 1g executor × 1, overhead floor:
			//   1024 + max(1024*0.1, 384) = 1024 + 384 = 1408 MiB
			name: "client mode: empty override map falls through to calculation",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{Name: "app-cem", Namespace: "default"},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeClient,
					Type: v1beta2.SparkApplicationTypeJava,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("1g"), Cores: ptr.To[int32](1)},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances:    ptr.To[int32](1),
						SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("1g"), Cores: ptr.To[int32](1)},
					},
					BatchSchedulerOptions: &v1beta2.BatchSchedulerConfiguration{
						Resources: corev1.ResourceList{}, // explicitly empty
					},
				},
			},
			mode:            v1beta2.DeployModeClient,
			expectResources: nil,
			expectMemoryMiB: 1408,
		},
		{
			// Sanity: JVM default overhead, client mode.
			// 1g executor × 2: (1024 + 384) * 2 = 2816 MiB
			name: "client mode: no override, JVM default overhead",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{Name: "app-cj", Namespace: "default"},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeClient,
					Type: v1beta2.SparkApplicationTypeJava,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("1g"), Cores: ptr.To[int32](1)},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances:    ptr.To[int32](2),
						SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("1g"), Cores: ptr.To[int32](1)},
					},
				},
			},
			mode:            v1beta2.DeployModeClient,
			expectMemoryMiB: 2816,
		},

		// ---- Cluster mode ----

		{
			// Override set → minResources equals the override verbatim; no calculation runs.
			name: "cluster mode: override set",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{Name: "app-ko", Namespace: "default"},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeCluster,
					Type: v1beta2.SparkApplicationTypeJava,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("1g"), Cores: ptr.To[int32](1)},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances:    ptr.To[int32](2),
						SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("1g"), Cores: ptr.To[int32](1)},
					},
					BatchSchedulerOptions: &v1beta2.BatchSchedulerConfiguration{
						Resources: explicitResources,
					},
				},
			},
			mode:            v1beta2.DeployModeCluster,
			expectResources: explicitResources,
		},
		{
			// Override set AND spec has invalid memory → must still succeed, use override.
			name: "cluster mode: override set, invalid memory string",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{Name: "app-ki", Namespace: "default"},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeCluster,
					Type: v1beta2.SparkApplicationTypeJava,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{Memory: badMemory, Cores: ptr.To[int32](1)},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances:    ptr.To[int32](2),
						SparkPodSpec: v1beta2.SparkPodSpec{Memory: badMemory, Cores: ptr.To[int32](1)},
					},
					BatchSchedulerOptions: &v1beta2.BatchSchedulerConfiguration{
						Resources: explicitResources,
					},
				},
			},
			mode:            v1beta2.DeployModeCluster,
			expectResources: explicitResources,
		},
		{
			// No override, invalid executor memory → error.
			name: "cluster mode: no override, invalid executor memory → error",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{Name: "app-ke", Namespace: "default"},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeCluster,
					Type: v1beta2.SparkApplicationTypeJava,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("1g"), Cores: ptr.To[int32](1)},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances:    ptr.To[int32](2),
						SparkPodSpec: v1beta2.SparkPodSpec{Memory: badMemory, Cores: ptr.To[int32](1)},
					},
				},
			},
			mode:        v1beta2.DeployModeCluster,
			expectError: true,
		},
		{
			// Empty override map → falls through to calculation.
			// driver: 1024 + 384 = 1408 MiB; executors: 1408 * 2 = 2816 MiB; total = 4224 MiB
			name: "cluster mode: empty override map falls through to calculation",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{Name: "app-kem", Namespace: "default"},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeCluster,
					Type: v1beta2.SparkApplicationTypeJava,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("1g"), Cores: ptr.To[int32](1)},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances:    ptr.To[int32](2),
						SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("1g"), Cores: ptr.To[int32](1)},
					},
					BatchSchedulerOptions: &v1beta2.BatchSchedulerConfiguration{
						Resources: corev1.ResourceList{}, // explicitly empty
					},
				},
			},
			mode:            v1beta2.DeployModeCluster,
			expectResources: nil,
			expectMemoryMiB: 4224, // 1408 (driver) + 2816 (2 × 1408 executor)
		},
		{
			// Sanity: Python default overhead, cluster mode.
			// driver: 1024 + max(1024*0.4, 384) = 1024 + 409 = 1433 MiB
			// executor × 1: 1433 MiB
			// total: 2866 MiB
			name: "cluster mode: no override, Python default overhead",
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{Name: "app-kp", Namespace: "default"},
				Spec: v1beta2.SparkApplicationSpec{
					Mode: v1beta2.DeployModeCluster,
					Type: v1beta2.SparkApplicationTypePython,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("1g"), Cores: ptr.To[int32](1)},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances:    ptr.To[int32](1),
						SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("1g"), Cores: ptr.To[int32](1)},
					},
				},
			},
			mode:            v1beta2.DeployModeCluster,
			expectMemoryMiB: 2866,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.app.Annotations = make(map[string]string)
			tc.app.Spec.Driver.Annotations = make(map[string]string)
			tc.app.Spec.Executor.Annotations = make(map[string]string)

			var capturedPodGroup *v1beta1.PodGroup
			mockVolcanoClient := fakevolcanoclientset.NewSimpleClientset()
			mockVolcanoClient.PrependReactor("create", "podgroups", func(action clienttesting.Action) (bool, runtime.Object, error) {
				createAction := action.(clienttesting.CreateAction)
				capturedPodGroup = createAction.GetObject().(*v1beta1.PodGroup)
				return false, capturedPodGroup, nil
			})

			sched := &Scheduler{volcanoClient: mockVolcanoClient}
			err := sched.Schedule(tc.app)

			if tc.expectError {
				assert.Error(t, err, "expected an error but got none")
				return
			}
			require.NoError(t, err)
			require.NotNil(t, capturedPodGroup)
			require.NotNil(t, capturedPodGroup.Spec.MinResources)
			got := *capturedPodGroup.Spec.MinResources

			if tc.expectResources != nil {
				// Override path: resources must match the override verbatim.
				for name, want := range tc.expectResources {
					actual := got.Name(name, resource.DecimalSI)
					assert.Equal(t, want.Value(), actual.Value(),
						"resource %s: got %s, want %s", name, actual.String(), want.String())
				}
				return
			}

			// Computed path: check the memory total only.
			expected := resource.MustParse(fmt.Sprintf("%dMi", tc.expectMemoryMiB))
			actualMem := got[corev1.ResourceMemory]
			assert.Equal(t, expected.Value(), actualMem.Value(),
				"memory: got %s, want %dMi", actualMem.String(), tc.expectMemoryMiB)
		})
	}
}
