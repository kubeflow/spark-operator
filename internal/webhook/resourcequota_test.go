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

// newQuotaTestApp returns an application with a 4g driver and one 4g executor.
func newQuotaTestApp(appType v1beta2.SparkApplicationType, memoryOverheadFactor *string) *v1beta2.SparkApplication {
	return &v1beta2.SparkApplication{
		Spec: v1beta2.SparkApplicationSpec{
			Type:                 appType,
			MemoryOverheadFactor: memoryOverheadFactor,
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Memory: ptr.To("4g"),
				},
			},
			Executor: v1beta2.ExecutorSpec{
				Instances: ptr.To(int32(1)),
				SparkPodSpec: v1beta2.SparkPodSpec{
					Memory: ptr.To("4g"),
				},
			},
		},
	}
}

func TestGetMemoryRequests(t *testing.T) {
	const gi = int64(1 << 30)

	// Each pod requests its memory plus max(memory * overheadFactor, 384Mi).
	// With 4g of memory the factor term is always above the 384Mi floor, so the
	// driver and the single executor each request 4g plus 4g * factor.
	testCases := []struct {
		name                 string
		appType              v1beta2.SparkApplicationType
		memoryOverheadFactor *string
		expectedBytes        int64
	}{
		{
			name:          "Java app uses the JVM overhead factor",
			appType:       v1beta2.SparkApplicationTypeJava,
			expectedBytes: 2 * (4*gi + 4*gi/10),
		},
		{
			name:          "Scala app uses the JVM overhead factor",
			appType:       v1beta2.SparkApplicationTypeScala,
			expectedBytes: 2 * (4*gi + 4*gi/10),
		},
		{
			name:          "Python app uses the non-JVM overhead factor",
			appType:       v1beta2.SparkApplicationTypePython,
			expectedBytes: 2 * (4*gi + 4*gi*4/10),
		},
		{
			name:                 "explicit memoryOverheadFactor overrides the type default",
			appType:              v1beta2.SparkApplicationTypeScala,
			memoryOverheadFactor: ptr.To("0.2"),
			expectedBytes:        2 * (4*gi + 4*gi*2/10),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			requests, err := getMemoryRequests(newQuotaTestApp(tc.appType, tc.memoryOverheadFactor))
			if err != nil {
				t.Fatal(err)
			}
			got := requests[corev1.ResourceRequestsMemory]
			if got.Value() != tc.expectedBytes {
				t.Errorf("expected requests.memory of %d bytes, got %d bytes", tc.expectedBytes, got.Value())
			}
		})
	}
}

func TestValidateResourceQuota(t *testing.T) {
	// A 10Gi quota fits a 4g driver plus one 4g executor with the 10% JVM
	// overhead (about 9011Mi) but not with the 40% non-JVM overhead (about
	// 11468Mi).
	hard := corev1.ResourceList{
		corev1.ResourceRequestsMemory: resource.MustParse("10Gi"),
	}
	quota := corev1.ResourceQuota{
		Spec: corev1.ResourceQuotaSpec{
			Hard: hard,
		},
		Status: corev1.ResourceQuotaStatus{
			Hard: hard,
			Used: corev1.ResourceList{
				corev1.ResourceRequestsMemory: resource.MustParse("0"),
			},
		},
	}

	testCases := []struct {
		name     string
		appType  v1beta2.SparkApplicationType
		expected bool
	}{
		{
			name:     "Scala app fits the quota",
			appType:  v1beta2.SparkApplicationTypeScala,
			expected: true,
		},
		{
			name:     "Python app exceeds the quota",
			appType:  v1beta2.SparkApplicationTypePython,
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			requests, err := getMemoryRequests(newQuotaTestApp(tc.appType, nil))
			if err != nil {
				t.Fatal(err)
			}
			if got := validateResourceQuota(requests, quota); got != tc.expected {
				t.Errorf("expected validateResourceQuota to return %v, got %v", tc.expected, got)
			}
		})
	}
}
