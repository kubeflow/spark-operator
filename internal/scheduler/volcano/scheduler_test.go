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

package volcano_test

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/util"
)

func TestGetDriverResource(t *testing.T) {
	var oneCore int32 = 1
	oneCoreStr := "1"
	oneGB := "1024m"
	twoCoresStr := "2"

	result := corev1.ResourceList{}
	result[corev1.ResourceCPU] = resource.MustParse("1")
	result[corev1.ResourceMemory] = resource.MustParse("2048m")

	testCases := []struct {
		Name   string
		app    v1beta2.SparkApplication
		result corev1.ResourceList
	}{
		{
			Name: "Validate Core and memory",
			app: v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:          &oneCore,
							CoreLimit:      &twoCoresStr,
							Memory:         &oneGB,
							MemoryOverhead: &oneGB,
						},
					},
				},
			},
			result: result,
		},
		{
			Name: "Validate CoreLimit and memory",
			app: v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							CoreLimit:      &oneCoreStr,
							Memory:         &oneGB,
							MemoryOverhead: &oneGB,
						},
					},
				},
			},
			result: result,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			r := util.GetDriverRequestResource(&tc.app)
			for name, quantity := range tc.result {
				if actual, ok := r[name]; !ok {
					t.Errorf("expecting driver pod to have resource %s, while get none", name)
				} else {
					if quantity.Cmp(actual) != 0 {
						t.Errorf("expecting driver pod to have resource %s with value %s, while get %s",
							name, quantity.String(), actual.String())
					}
				}
			}
		})
	}
}

func TestGetExecutorResource(t *testing.T) {
	oneCore := int32(1)
	oneCoreStr := "1"
	oneGB := "1024m"
	twoCores := int32(2)
	instances := int32(2)

	result := corev1.ResourceList{}
	result[corev1.ResourceCPU] = resource.MustParse("2")
	result[corev1.ResourceMemory] = resource.MustParse("4096m")

	testCases := []struct {
		Name   string
		app    v1beta2.SparkApplication
		result corev1.ResourceList
	}{
		{
			Name: "Validate Core and memory",
			app: v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Executor: v1beta2.ExecutorSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:          &oneCore,
							Memory:         &oneGB,
							MemoryOverhead: &oneGB,
						},
						Instances: &instances,
					},
				},
			},
			result: result,
		},
		{
			Name: "Validate CoreRequest and memory",
			app: v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Executor: v1beta2.ExecutorSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Cores:          &twoCores,
							Memory:         &oneGB,
							MemoryOverhead: &oneGB,
						},
						CoreRequest: &oneCoreStr,
						Instances:   &instances,
					},
				},
			},
			result: result,
		},
		{
			Name: "Validate CoreLimit and memory",
			app: v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Executor: v1beta2.ExecutorSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							CoreLimit:      &oneCoreStr,
							Memory:         &oneGB,
							MemoryOverhead: &oneGB,
						},
						Instances: &instances,
					},
				},
			},
			result: result,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			r := util.GetExecutorRequestResource(&tc.app)
			for name, quantity := range tc.result {
				if actual, ok := r[name]; !ok {
					t.Errorf("expecting executor pod to have resource %s, while get none", name)
				} else {
					if quantity.Cmp(actual) != 0 {
						t.Errorf("expecting executor pod to have resource %s with value %s, while get %s",
							name, quantity.String(), actual.String())
					}
				}
			}
		})
	}
}
