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

package resourceusage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

func TestCpuRequest(t *testing.T) {
	testCases := []struct {
		cores       *int32
		coreRequest *string
		expected    string
	}{
		{nil, nil, "1"},
		{ptr.To[int32](1), nil, "1"},
		{nil, ptr.To("1"), "1"},
		{ptr.To[int32](1), ptr.To("500m"), "500m"},
	}

	for _, tc := range testCases {
		actual, err := cpuRequest(tc.cores, tc.coreRequest)
		assert.Nil(t, err)
		assert.Equal(t, tc.expected, actual)
	}
}

func TestCpuRequestInvalid(t *testing.T) {
	invalidInputs := []string{
		"",
		"asd",
		"Random 500m",
	}

	for _, input := range invalidInputs {
		_, err := cpuRequest(nil, &input)
		assert.NotNil(t, err)
	}
}

func TestToResourceList(t *testing.T) {
	t.Run("valid CPU quantity", func(t *testing.T) {
		rl, err := ToResourceList(map[string]string{"cpu": "2"})
		require.NoError(t, err)
		require.NotNil(t, rl)
		expected := resource.MustParse("2")
		actual := rl[corev1.ResourceCPU]
		assert.Equal(t, (&expected).Value(), (&actual).Value())
	})

	t.Run("valid memory quantity", func(t *testing.T) {
		rl, err := ToResourceList(map[string]string{"memory": "1408Mi"})
		require.NoError(t, err)
		require.NotNil(t, rl)
		expected := resource.MustParse("1408Mi")
		actual := rl[corev1.ResourceMemory]
		assert.Equal(t, (&expected).Value(), (&actual).Value())
	})

	t.Run("valid CPU and memory together", func(t *testing.T) {
		rl, err := ToResourceList(map[string]string{"cpu": "500m", "memory": "2Gi"})
		require.NoError(t, err)
		require.NotNil(t, rl)
		expCPU := resource.MustParse("500m")
		gotCPU := rl[corev1.ResourceCPU]
		expMem := resource.MustParse("2Gi")
		gotMem := rl[corev1.ResourceMemory]
		assert.Equal(t, (&expCPU).MilliValue(), (&gotCPU).MilliValue())
		assert.Equal(t, (&expMem).Value(), (&gotMem).Value())
	})

	t.Run("invalid value returns error and nil result", func(t *testing.T) {
		rl, err := ToResourceList(map[string]string{"cpu": "not-a-number"})
		assert.Error(t, err, "expected a parse error for invalid quantity")
		assert.Nil(t, rl, "expected nil ResourceList on error")
	})

	t.Run("empty map returns empty ResourceList", func(t *testing.T) {
		rl, err := ToResourceList(map[string]string{})
		require.NoError(t, err)
		assert.Empty(t, rl)
	})
}

func TestDriverPodResourceList(t *testing.T) {
	testCases := []struct {
		name           string
		app            *v1beta2.SparkApplication
		expectMemoryMi int64
		expectCPU      string
	}{
		{
			// heap = 1g = 1024 Mi
			// factor = 0.1 (JVM default)
			// overhead = max(1024 MiB * 0.1, 384 MiB) = max(102.4 MiB, 384 MiB) = 384 MiB
			//   (102.4 < 384, so floor kicks in at the min value, not the factor)
			// total = 1024 + 384 = 1408 Mi
			name: "JVM default overhead factor — floor kicks in",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeJava,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("1g"),
							Cores:  ptr.To[int32](2),
						},
					},
				},
			},
			expectMemoryMi: 1408,
			expectCPU:      "2",
		},
		{
			// heap = 4g = 4096 Mi
			// factor = 0.1 (JVM default)
			// overhead = max(4096 * 0.1, 384) = max(409.6, 384) = 409.6 → int64 truncation → 409 MiB
			//   Note: int64(math.Max(...)) truncates toward zero (floors), NOT rounds.
			//   4096 * 1024 * 1024 * 0.1 = 429496729.6 bytes → int64 = 429496729 bytes
			//   429496729 / 1024 / 1024 = 409 Mi (integer division floors again)
			// total = 4096 + 409 = 4505 Mi
			name: "JVM default overhead factor — factor dominates (above 384Mi floor)",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeJava,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("4g"),
							Cores:  ptr.To[int32](1),
						},
					},
				},
			},
			expectMemoryMi: 4505,
			expectCPU:      "1",
		},
		{
			// heap = 2g = 2048 Mi, explicit overhead = 512m = 512 Mi
			// explicit overhead is used as-is; factor is NOT applied
			// total = 2048 + 512 = 2560 Mi
			name: "JVM explicit memoryOverhead — factor not applied",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeJava,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory:         ptr.To("2g"),
							MemoryOverhead: ptr.To("512m"),
							Cores:          ptr.To[int32](1),
						},
					},
				},
			},
			expectMemoryMi: 2560,
			expectCPU:      "1",
		},
		{
			// heap = 1g = 1024 Mi
			// factor = 0.4 (Python/non-JVM default)
			// overhead = max(1024 * 1024 * 1024 * 0.4, 384 * 1024 * 1024)
			//          = max(429496729.6 bytes, 402653184 bytes)
			//   int64(429496729.6) = 429496729 bytes (truncated)
			//   429496729 / 1024 / 1024 = 409 Mi (integer division floors)
			// total = 1024 + 409 = 1433 Mi
			//
			// int64() conversion of float64 also truncates toward zero.
			// So 0.4 * 1024 MiB = 409.6 MiB → 409 MiB (NOT 410).
			name: "Python default overhead factor — confirmed floor not round",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypePython,
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("1g"),
							Cores:  ptr.To[int32](1),
						},
					},
				},
			},
			expectMemoryMi: 1433,
			expectCPU:      "1",
		},
		{
			// heap = 1g = 1024 Mi, custom factor = 0.2
			// overhead = max(1024 * 0.2, 384) = max(204.8, 384) = 384 Mi (floor dominates)
			// total = 1024 + 384 = 1408 Mi
			name: "custom overhead factor below 384Mi floor",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:                 v1beta2.SparkApplicationTypeJava,
					MemoryOverheadFactor: ptr.To("0.2"),
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("1g"),
							Cores:  ptr.To[int32](1),
						},
					},
				},
			},
			expectMemoryMi: 1408,
			expectCPU:      "1",
		},
		{
			// heap = 4g = 4096 Mi, custom factor = 0.5
			// overhead = max(4096 * 1024 * 1024 * 0.5, 384 * 1024 * 1024)
			//          = max(2147483648 bytes, 402653184 bytes)
			//   int64(2147483648.0) = 2147483648 bytes
			//   2147483648 / 1024 / 1024 = 2048 Mi
			// total = 4096 + 2048 = 6144 Mi
			name: "custom overhead factor 0.5 above floor",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:                 v1beta2.SparkApplicationTypeJava,
					MemoryOverheadFactor: ptr.To("0.5"),
					Driver: v1beta2.DriverSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("4g"),
							Cores:  ptr.To[int32](4),
						},
					},
				},
			},
			expectMemoryMi: 6144,
			expectCPU:      "4",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rl, err := DriverPodResourceList(tc.app)
			require.NoError(t, err)
			require.NotNil(t, rl)

			expectedMem := resource.MustParse(fmt.Sprintf("%dMi", tc.expectMemoryMi))
			actualMem := rl[corev1.ResourceMemory]
			assert.Equal(t, expectedMem.Value(), actualMem.Value(),
				"driver memory mismatch: got %s, want %dMi", actualMem.String(), tc.expectMemoryMi)

			expectedCPU := resource.MustParse(tc.expectCPU)
			actualCPU := rl[corev1.ResourceCPU]
			assert.Equal(t, expectedCPU.Value(), actualCPU.Value(),
				"driver CPU mismatch: got %s, want %s", actualCPU.String(), tc.expectCPU)
		})
	}
}

func TestExecutorPodResourceList(t *testing.T) {
	testCases := []struct {
		name           string
		app            *v1beta2.SparkApplication
		expectMemoryMi int64
	}{
		{
			// heap = 1g = 1024 Mi; factor = 0.1 (JVM); overhead = 384 Mi (floor)
			// total = 1024 + 384 = 1408 Mi
			name: "JVM executor default overhead",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeJava,
					Executor: v1beta2.ExecutorSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("1g"),
							Cores:  ptr.To[int32](1),
						},
					},
				},
			},
			expectMemoryMi: 1408,
		},
		{
			// heap = 1g = 1024 Mi; factor = 0.4 (Python); overhead = 409 Mi (truncated)
			// pyspark = 256m = 256 Mi
			// total = 1024 + 409 + 256 = 1689 Mi
			name: "Python executor with pyspark memory",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypePython,
					SparkConf: map[string]string{
						"spark.executor.pyspark.memory": "256m",
					},
					Executor: v1beta2.ExecutorSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory: ptr.To("1g"),
							Cores:  ptr.To[int32](1),
						},
					},
				},
			},
			expectMemoryMi: 1689,
		},
		{
			// heap = 2g = 2048 Mi; explicit overhead = 256m = 256 Mi (factor not applied)
			// off-heap = 512m = 512 Mi
			// total = 2048 + 256 + 512 = 2816 Mi
			name: "JVM executor with explicit overhead and off-heap",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeJava,
					SparkConf: map[string]string{
						"spark.memory.offHeap.enabled": "true",
						"spark.memory.offHeap.size":    "512m",
					},
					Executor: v1beta2.ExecutorSpec{
						SparkPodSpec: v1beta2.SparkPodSpec{
							Memory:         ptr.To("2g"),
							MemoryOverhead: ptr.To("256m"),
							Cores:          ptr.To[int32](2),
						},
					},
				},
			},
			expectMemoryMi: 2816,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rl, err := ExecutorPodResourceList(tc.app)
			require.NoError(t, err)
			require.NotNil(t, rl)

			expectedMem := resource.MustParse(fmt.Sprintf("%dMi", tc.expectMemoryMi))
			actualMem := rl[corev1.ResourceMemory]
			assert.Equal(t, expectedMem.Value(), actualMem.Value(),
				"executor memory mismatch: got %s, want %dMi", actualMem.String(), tc.expectMemoryMi)
		})
	}
}

// TestUnitlessMemoryValues verifies that a bare number in a memory field is treated
// correctly per Spark's unit rules:
//   - spark.driver.memory / spark.executor.memory / spark.*.memoryOverhead /
//     spark.executor.pyspark.memory: bare number means MiB.
//   - spark.memory.offHeap.size: bare number means bytes.
func TestUnitlessMemoryValues(t *testing.T) {
	t.Run("driver.memory bare number treated as MiB", func(t *testing.T) {
		// "1024" with no suffix means 1024 MiB = 1 GiB.
		// factor = 0.1 (JVM); overhead = max(1024*0.1, 384) = 384 MiB (floor)
		// total = 1024 + 384 = 1408 MiB
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type: v1beta2.SparkApplicationTypeJava,
				Driver: v1beta2.DriverSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Memory: ptr.To("1024"),
						Cores:  ptr.To[int32](1),
					},
				},
			},
		}
		rl, err := DriverPodResourceList(app)
		require.NoError(t, err)
		expected := resource.MustParse("1408Mi")
		actual := rl[corev1.ResourceMemory]
		assert.Equal(t, expected.Value(), actual.Value(),
			"bare-number driver.memory: got %s, want 1408Mi", actual.String())
	})

	t.Run("executor.memoryOverhead bare number treated as MiB", func(t *testing.T) {
		// heap = 2g = 2048 MiB; explicit overhead = "512" = 512 MiB (bare number, MiB)
		// total = 2048 + 512 = 2560 MiB
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type: v1beta2.SparkApplicationTypeJava,
				Executor: v1beta2.ExecutorSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Memory:         ptr.To("2g"),
						MemoryOverhead: ptr.To("512"),
						Cores:          ptr.To[int32](1),
					},
				},
			},
		}
		rl, err := ExecutorPodResourceList(app)
		require.NoError(t, err)
		expected := resource.MustParse("2560Mi")
		actual := rl[corev1.ResourceMemory]
		assert.Equal(t, expected.Value(), actual.Value(),
			"bare-number executor.memoryOverhead: got %s, want 2560Mi", actual.String())
	})

	t.Run("spark.memory.offHeap.size bare number treated as bytes", func(t *testing.T) {
		// heap = 1g = 1024 MiB; overhead = 384 MiB (JVM floor)
		// off-heap = 1024 bytes (NOT MiB) = 0 MiB when floored by bytesToMi
		// total visible in MiB = (1024 + 384 + 0) = 1408 MiB
		// But the raw bytes sum = 1408*1024*1024 + 1024 — bytesToMi floors, so still 1408 MiB.
		// This test verifies the off-heap value is parsed as bytes (1024 bytes, not 1024 MiB).
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type: v1beta2.SparkApplicationTypeJava,
				SparkConf: map[string]string{
					"spark.memory.offHeap.enabled": "true",
					"spark.memory.offHeap.size":    "1024", // 1024 bytes (not MiB)
				},
				Executor: v1beta2.ExecutorSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Memory: ptr.To("1g"),
						Cores:  ptr.To[int32](1),
					},
				},
			},
		}
		rl, err := ExecutorPodResourceList(app)
		require.NoError(t, err)
		// 1024 MiB heap + 384 MiB overhead + 1024 bytes off-heap.
		// bytesToMi floors: (1024+384)*1024*1024 + 1024 bytes = 1468006424 bytes
		// 1468006424 / 1024 / 1024 = 1399 MiB (floors, not 1408)
		// Wait — this tells us the off-heap is additive BEFORE the floor:
		// total bytes = (1408 * 1024 * 1024) + 1024 = 1476396032 + 1024 = 1476397056
		// 1476397056 / 1024 / 1024 = 1408 Mi (integer division: 1476397056/1048576 = 1408.0009...)
		// So the result is still 1408 MiB — 1024 bytes is below the MiB floor.
		expected := resource.MustParse("1408Mi")
		actual := rl[corev1.ResourceMemory]
		assert.Equal(t, expected.Value(), actual.Value(),
			"offHeap.size as bytes: got %s, want 1408Mi", actual.String())
	})

	t.Run("spark.memory.offHeap.size large bare number stays bytes not MiB", func(t *testing.T) {
		// heap = 1g = 1024 MiB; overhead = 384 MiB (JVM floor)
		// off-heap = 536870912 bytes = 512 MiB (exactly 512 * 1024 * 1024)
		// total = (1024 + 384) MiB + 512 MiB = 1920 MiB
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type: v1beta2.SparkApplicationTypeJava,
				SparkConf: map[string]string{
					"spark.memory.offHeap.enabled": "true",
					"spark.memory.offHeap.size":    "536870912", // 512 MiB in bytes
				},
				Executor: v1beta2.ExecutorSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Memory: ptr.To("1g"),
						Cores:  ptr.To[int32](1),
					},
				},
			},
		}
		rl, err := ExecutorPodResourceList(app)
		require.NoError(t, err)
		expected := resource.MustParse("1920Mi")
		actual := rl[corev1.ResourceMemory]
		assert.Equal(t, expected.Value(), actual.Value(),
			"offHeap.size 536870912 bytes = 512MiB: got %s, want 1920Mi", actual.String())
	})
}
