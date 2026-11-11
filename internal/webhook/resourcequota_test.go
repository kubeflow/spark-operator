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

func TestGetResourceListMemory(t *testing.T) {
	app := &v1beta2.SparkApplication{
		Spec: v1beta2.SparkApplicationSpec{
			Type: v1beta2.SparkApplicationTypePython,
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Memory: ptr.To("512m"),
				},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Memory: ptr.To("512m"),
				},
				Instances: ptr.To[int32](2),
			},
		},
	}

	resourceList, err := getResourceList(app)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Each pod asks for 512Mi plus the 384Mi minimum overhead, and there are three pods.
	// Memory requests and limits are the same for Spark pods, so every key gets the same value.
	expected := resource.MustParse("2688Mi")
	for _, name := range []corev1.ResourceName{
		corev1.ResourceMemory,
		corev1.ResourceRequestsMemory,
		corev1.ResourceLimitsMemory,
	} {
		got, ok := resourceList[name]
		if !ok {
			t.Errorf("%s: missing from the resource list", name)
			continue
		}
		if got.Cmp(expected) != 0 {
			t.Errorf("%s: expected %s, got %s", name, expected.String(), got.String())
		}
	}
}
