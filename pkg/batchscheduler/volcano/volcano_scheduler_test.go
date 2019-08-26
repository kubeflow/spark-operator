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

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta1"
)

func TestGetDriverResource(t *testing.T) {

	halfCore := float32(0.5)
	halfCoreStr := "0.5"
	oneGB := "1024m"
	oneCoreStr := "1"

	result := v1.ResourceList{}
	result[v1.ResourceCPU] = resource.MustParse("0.5")
	result[v1.ResourceMemory] = resource.MustParse("2048m")

	testcases := []struct {
		Name   string
		app    v1beta1.SparkApplication
		result v1.ResourceList
	}{
		{
			Name: "Validate Core and memory",
			app: v1beta1.SparkApplication{
				Spec: v1beta1.SparkApplicationSpec{
					Driver: v1beta1.DriverSpec{
						SparkPodSpec: v1beta1.SparkPodSpec{
							Cores:          &halfCore,
							CoreLimit:      &oneCoreStr,
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
			app: v1beta1.SparkApplication{
				Spec: v1beta1.SparkApplicationSpec{
					Driver: v1beta1.DriverSpec{
						SparkPodSpec: v1beta1.SparkPodSpec{
							CoreLimit:      &halfCoreStr,
							Memory:         &oneGB,
							MemoryOverhead: &oneGB,
						},
					},
				},
			},
			result: result,
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.Name, func(t *testing.T) {
			r := getDriverRequestResource(&testcase.app)
			for name, quantity := range testcase.result {
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

	halfCore := float32(0.5)
	halfCoreStr := "0.5"
	oneGB := "1024m"
	oneCore := float32(1)
	instances := int32(2)

	result := v1.ResourceList{}
	result[v1.ResourceCPU] = resource.MustParse("1")
	result[v1.ResourceMemory] = resource.MustParse("4096m")

	testcases := []struct {
		Name   string
		app    v1beta1.SparkApplication
		result v1.ResourceList
	}{
		{
			Name: "Validate Core and memory",
			app: v1beta1.SparkApplication{
				Spec: v1beta1.SparkApplicationSpec{
					Executor: v1beta1.ExecutorSpec{
						SparkPodSpec: v1beta1.SparkPodSpec{
							Cores:          &halfCore,
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
			app: v1beta1.SparkApplication{
				Spec: v1beta1.SparkApplicationSpec{
					Executor: v1beta1.ExecutorSpec{
						SparkPodSpec: v1beta1.SparkPodSpec{
							Cores:          &oneCore,
							Memory:         &oneGB,
							MemoryOverhead: &oneGB,
						},
						CoreRequest: &halfCoreStr,
						Instances:   &instances,
					},
				},
			},
			result: result,
		},
		{
			Name: "Validate CoreLimit and memory",
			app: v1beta1.SparkApplication{
				Spec: v1beta1.SparkApplicationSpec{
					Executor: v1beta1.ExecutorSpec{
						SparkPodSpec: v1beta1.SparkPodSpec{
							CoreLimit:      &halfCoreStr,
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

	for _, testcase := range testcases {
		t.Run(testcase.Name, func(t *testing.T) {
			r := getExecutorRequestResource(&testcase.app)
			for name, quantity := range testcase.result {
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
