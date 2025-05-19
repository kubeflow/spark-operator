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
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clienttesting "k8s.io/client-go/testing"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	fakevolcanoclientset "volcano.sh/apis/pkg/client/clientset/versioned/fake"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/util"
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
						Instances:    util.Int32Ptr(1),
						SparkPodSpec: v1beta2.SparkPodSpec{},
					},
					BatchSchedulerOptions: &v1beta2.BatchSchedulerConfiguration{
						Queue:             util.StringPtr("high-priority"),
						PriorityClassName: util.StringPtr("high"),
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
						Instances:    util.Int32Ptr(1),
						SparkPodSpec: v1beta2.SparkPodSpec{},
					},
					BatchSchedulerOptions: &v1beta2.BatchSchedulerConfiguration{
						Queue: util.StringPtr("batch-queue"),
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
							Memory: util.StringPtr("2g"),
							Cores:  util.Int32Ptr(1),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: util.Int32Ptr(2),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: util.StringPtr("1g"),
							Cores:  util.Int32Ptr(1),
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
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: util.StringPtr("2g"),
							Cores:  util.Int32Ptr(1),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: util.Int32Ptr(2),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: util.StringPtr("1g"),
							Cores:  util.Int32Ptr(1),
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
							Memory: util.StringPtr("2g"),
							Cores:  util.Int32Ptr(1),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: util.Int32Ptr(3),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: util.StringPtr("1g"),
							Cores:  util.Int32Ptr(1),
						},
					},
					BatchSchedulerOptions: &v1beta2.BatchSchedulerConfiguration{
						Queue: util.StringPtr("gpu-queue"),
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
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: util.StringPtr("2g"),
							Cores:  util.Int32Ptr(1),
						},
					},
					Executor: v1beta2.ExecutorSpec{
						Instances: util.Int32Ptr(2),
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: util.StringPtr("1g"),
							Cores:  util.Int32Ptr(1),
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
			tc.app.ObjectMeta.Annotations = make(map[string]string)
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

			if tc.expectedMode == "client" {
				assert.Contains(t, tc.app.Spec.Executor.Annotations, v1beta1.KubeGroupNameAnnotationKey)
				assert.NotContains(t, tc.app.Spec.Driver.Annotations, v1beta1.KubeGroupNameAnnotationKey)
			} else if tc.expectedMode == "cluster" {
				assert.Contains(t, tc.app.Spec.Driver.Annotations, v1beta1.KubeGroupNameAnnotationKey)
				assert.Contains(t, tc.app.Spec.Executor.Annotations, v1beta1.KubeGroupNameAnnotationKey)
			}

			expectedPodGroupName := getPodGroupName(tc.app)
			assert.Equal(t, expectedPodGroupName, capturedPodGroup.Name)

			assert.Len(t, capturedPodGroup.OwnerReferences, 1)
			assert.Equal(t, tc.app.Name, capturedPodGroup.OwnerReferences[0].Name)
			assert.Equal(t, "SparkApplication", capturedPodGroup.OwnerReferences[0].Kind)

			// Verify custom resources if specified
			if tc.app.Spec.BatchSchedulerOptions != nil && len(tc.app.Spec.BatchSchedulerOptions.Resources) > 0 {
				assert.NotNil(t, capturedPodGroup.Spec.MinResources)
				for resourceName, expectedQuantity := range tc.app.Spec.BatchSchedulerOptions.Resources {
					actualQuantity := capturedPodGroup.Spec.MinResources.Name(resourceName, resource.DecimalSI)
					assert.Equal(t, expectedQuantity.Value(), actualQuantity.Value(),
						"Resource %s quantity should match in PodGroup MinResources", resourceName)
				}
			}
			if tc.app.Spec.BatchSchedulerOptions == nil {
				assert.NotNil(t, capturedPodGroup.Spec.MinResources)

				var expectedResources corev1.ResourceList
				if tc.expectedMode == "cluster" {
					// For cluster mode, check that the resources are the sum of driver and executor resources
					driverResources := util.GetDriverRequestResource(tc.app)
					executorResources := util.GetExecutorRequestResource(tc.app)
					expectedResources = util.SumResourceList([]corev1.ResourceList{driverResources, executorResources})
				} else {
					// For client mode, check that only executor resources are used
					expectedResources = util.GetExecutorRequestResource(tc.app)
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
