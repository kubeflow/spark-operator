/*
Copyright 2024 The Kubeflow Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package webhook

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

func assertMemory(memoryString string, expectedBytes int64, t *testing.T) {
	m, err := parseJavaMemoryString(memoryString)
	if err != nil {
		t.Error(err)
		return
	}
	if m != expectedBytes {
		t.Errorf("%s: expected %v bytes, got %v bytes", memoryString, expectedBytes, m)
		return
	}
}

func TestJavaMemoryString(t *testing.T) {
	assertMemory("1b", 1, t)
	assertMemory("100k", 100*1024, t)
	assertMemory("1gb", 1024*1024*1024, t)
	assertMemory("10TB", 10*1024*1024*1024*1024, t)
	assertMemory("10PB", 10*1024*1024*1024*1024*1024, t)
}

func TestGetResourceListCoreRequest(t *testing.T) {
	newApp := func(driverCores, executorCores int32, instances int32, driverCoreRequest, executorCoreRequest *string) *v1beta2.SparkApplication {
		return &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Driver: v1beta2.DriverSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{Cores: ptr.To(driverCores), Memory: ptr.To("1g")},
					CoreRequest:  driverCoreRequest,
				},
				Executor: v1beta2.ExecutorSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{Cores: ptr.To(executorCores), Memory: ptr.To("1g")},
					Instances:    ptr.To(instances),
					CoreRequest:  executorCoreRequest,
				},
			},
		}
	}

	testCases := []struct {
		name           string
		app            *v1beta2.SparkApplication
		expectedCPU    string
		expectedLimits string
	}{
		{
			name:           "cores only",
			app:            newApp(1, 2, 3, nil, nil),
			expectedCPU:    "7",
			expectedLimits: "7",
		},
		{
			name:           "coreRequest lower than cores",
			app:            newApp(4, 4, 2, ptr.To("500m"), ptr.To("1")),
			expectedCPU:    "2500m",
			expectedLimits: "12",
		},
		{
			name:           "coreRequest higher than cores",
			app:            newApp(1, 1, 2, ptr.To("2"), ptr.To("2")),
			expectedCPU:    "6",
			expectedLimits: "3",
		},
		{
			name:           "decimal coreRequest",
			app:            newApp(1, 1, 3, nil, ptr.To("0.5")),
			expectedCPU:    "2500m",
			expectedLimits: "4",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resourceList, err := getResourceList(tc.app)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceRequestsCPU} {
				got := resourceList[name]
				if got.Cmp(resource.MustParse(tc.expectedCPU)) != 0 {
					t.Errorf("%s: expected %s, got %s", name, tc.expectedCPU, got.String())
				}
			}
			got := resourceList[corev1.ResourceLimitsCPU]
			if got.Cmp(resource.MustParse(tc.expectedLimits)) != 0 {
				t.Errorf("%s: expected %s, got %s", corev1.ResourceLimitsCPU, tc.expectedLimits, got.String())
			}
		})
	}
}

func TestGetResourceListInvalidCoreRequest(t *testing.T) {
	app := &v1beta2.SparkApplication{
		Spec: v1beta2.SparkApplicationSpec{
			Executor: v1beta2.ExecutorSpec{CoreRequest: ptr.To("one")},
		},
	}
	if _, err := getResourceList(app); err == nil {
		t.Fatal("expected an error for an invalid executor coreRequest")
	}
}
