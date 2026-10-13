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
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

func TestBytesToMi(t *testing.T) {
	testCases := []struct {
		input    int64
		expected string
	}{
		{(2 * 1024 * 1024) - 1, "1Mi"},
		{2 * 1024 * 1024, "2Mi"},
		{(1024 * 1024 * 1024) - 1, "1023Mi"},
		{1024 * 1024 * 1024, "1024Mi"},
	}

	for _, tc := range testCases {
		assert.Equal(t, tc.expected, bytesToMi(tc.input))
	}
}

func TestIsJavaApp(t *testing.T) {
	testCases := []struct {
		name     string
		appType  v1beta2.SparkApplicationType
		expected bool
	}{
		{"java", v1beta2.SparkApplicationTypeJava, true},
		{"scala", v1beta2.SparkApplicationTypeScala, true},
		{"python", v1beta2.SparkApplicationTypePython, false},
		{"r", v1beta2.SparkApplicationTypeR, false},
		{"empty", v1beta2.SparkApplicationType(""), false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, isJavaApp(tc.appType))
		})
	}
}

func TestGetMemoryOverheadFactor(t *testing.T) {
	testCases := []struct {
		name     string
		app      *v1beta2.SparkApplication
		expected float64
	}{
		{
			name: "explicit factor takes precedence over the application type",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:                 v1beta2.SparkApplicationTypeJava,
					MemoryOverheadFactor: ptr.To("0.3"),
				},
			},
			expected: 0.3,
		},
		{
			name: "java application defaults to the JVM factor",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{Type: v1beta2.SparkApplicationTypeJava},
			},
			expected: common.DefaultJVMMemoryOverheadFactor,
		},
		{
			name: "scala application defaults to the JVM factor",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{Type: v1beta2.SparkApplicationTypeScala},
			},
			expected: common.DefaultJVMMemoryOverheadFactor,
		},
		{
			name: "python application defaults to the non-JVM factor",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{Type: v1beta2.SparkApplicationTypePython},
			},
			expected: common.DefaultNonJVMMemoryOverheadFactor,
		},
		{
			name: "r application defaults to the non-JVM factor",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{Type: v1beta2.SparkApplicationTypeR},
			},
			expected: common.DefaultNonJVMMemoryOverheadFactor,
		},
		{
			name: "zero factor is parsed and passed through",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:                 v1beta2.SparkApplicationTypeScala,
					MemoryOverheadFactor: ptr.To("0"),
				},
			},
			expected: 0,
		},
		{
			name: "negative factor is parsed and passed through",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:                 v1beta2.SparkApplicationTypeScala,
					MemoryOverheadFactor: ptr.To("-0.5"),
				},
			},
			expected: -0.5,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := getMemoryOverheadFactor(tc.app)
			assert.Nil(t, err)
			assert.InDelta(t, tc.expected, actual, 1e-9)
		})
	}
}

func TestGetMemoryOverheadFactorInvalid(t *testing.T) {
	invalidInputs := []string{
		"",
		"abc",
		"0.1.2",
		"10%",
	}

	for _, input := range invalidInputs {
		t.Run(input, func(t *testing.T) {
			app := &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:                 v1beta2.SparkApplicationTypeJava,
					MemoryOverheadFactor: ptr.To(input),
				},
			}
			_, err := getMemoryOverheadFactor(app)
			assert.NotNil(t, err)
		})
	}
}

func TestMemoryRequestBytes(t *testing.T) {
	testCases := []struct {
		name                 string
		podSpec              *v1beta2.SparkPodSpec
		memoryOverheadFactor float64
		expected             int64
	}{
		{
			name:                 "no memory set falls back to the minimum overhead",
			podSpec:              &v1beta2.SparkPodSpec{},
			memoryOverheadFactor: common.DefaultJVMMemoryOverheadFactor,
			expected:             384 * 1024 * 1024,
		},
		{
			name:                 "minimum overhead wins when the factor yields less",
			podSpec:              &v1beta2.SparkPodSpec{Memory: ptr.To("512m")},
			memoryOverheadFactor: common.DefaultJVMMemoryOverheadFactor,
			expected:             (512 + 384) * 1024 * 1024,
		},
		{
			// 4Gi + floor(4Gi * 0.1)
			name:                 "factor wins once it exceeds the minimum overhead",
			podSpec:              &v1beta2.SparkPodSpec{Memory: ptr.To("4g")},
			memoryOverheadFactor: common.DefaultJVMMemoryOverheadFactor,
			expected:             4724464025,
		},
		{
			name:                 "explicit overhead bypasses the factor",
			podSpec:              &v1beta2.SparkPodSpec{Memory: ptr.To("4g"), MemoryOverhead: ptr.To("1g")},
			memoryOverheadFactor: common.DefaultJVMMemoryOverheadFactor,
			expected:             5 * 1024 * 1024 * 1024,
		},
		{
			name:                 "non-JVM factor still floors at the minimum overhead",
			podSpec:              &v1beta2.SparkPodSpec{Memory: ptr.To("512m")},
			memoryOverheadFactor: common.DefaultNonJVMMemoryOverheadFactor,
			expected:             (512 + 384) * 1024 * 1024,
		},
		{
			name:                 "explicit overhead with no memory set",
			podSpec:              &v1beta2.SparkPodSpec{MemoryOverhead: ptr.To("512m")},
			memoryOverheadFactor: common.DefaultJVMMemoryOverheadFactor,
			expected:             512 * 1024 * 1024,
		},
		{
			// 4Gi * 0 is below the floor, so the 384Mi minimum applies
			name:                 "zero factor floors at the minimum overhead",
			podSpec:              &v1beta2.SparkPodSpec{Memory: ptr.To("4g")},
			memoryOverheadFactor: 0,
			expected:             (4096 + 384) * 1024 * 1024,
		},
		{
			// math.Max clamps the negative product to the 384Mi minimum
			name:                 "negative factor floors at the minimum overhead",
			podSpec:              &v1beta2.SparkPodSpec{Memory: ptr.To("4g")},
			memoryOverheadFactor: -0.5,
			expected:             (4096 + 384) * 1024 * 1024,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := memoryRequestBytes(tc.podSpec, tc.memoryOverheadFactor)
			assert.Nil(t, err)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestMemoryRequestBytesInvalid(t *testing.T) {
	testCases := []struct {
		name    string
		podSpec *v1beta2.SparkPodSpec
	}{
		{
			name:    "invalid memory",
			podSpec: &v1beta2.SparkPodSpec{Memory: ptr.To("512ub")},
		},
		{
			name:    "invalid memory overhead",
			podSpec: &v1beta2.SparkPodSpec{Memory: ptr.To("512m"), MemoryOverhead: ptr.To("abc")},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := memoryRequestBytes(tc.podSpec, common.DefaultJVMMemoryOverheadFactor)
			assert.NotNil(t, err)
		})
	}
}

func TestExecutorPysparkMemoryBytes(t *testing.T) {
	testCases := []struct {
		name     string
		appType  v1beta2.SparkApplicationType
		conf     map[string]string
		expected int64
	}{
		{
			name:     "bare number defaults to mebibytes",
			appType:  v1beta2.SparkApplicationTypePython,
			conf:     map[string]string{"spark.executor.pyspark.memory": "512"},
			expected: 512 * 1024 * 1024,
		},
		{
			name:     "explicit suffix is honoured",
			appType:  v1beta2.SparkApplicationTypePython,
			conf:     map[string]string{"spark.executor.pyspark.memory": "1g"},
			expected: 1024 * 1024 * 1024,
		},
		{
			name:     "conf is ignored for non-python applications",
			appType:  v1beta2.SparkApplicationTypeJava,
			conf:     map[string]string{"spark.executor.pyspark.memory": "512"},
			expected: 0,
		},
		{
			name:     "python application without the conf",
			appType:  v1beta2.SparkApplicationTypePython,
			conf:     map[string]string{},
			expected: 0,
		},
		{
			name:     "python application with a nil spark conf",
			appType:  v1beta2.SparkApplicationTypePython,
			conf:     nil,
			expected: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			app := &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{Type: tc.appType, SparkConf: tc.conf},
			}
			actual, err := executorPysparkMemoryBytes(app)
			assert.Nil(t, err)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestExecutorPysparkMemoryBytesInvalid(t *testing.T) {
	app := &v1beta2.SparkApplication{
		Spec: v1beta2.SparkApplicationSpec{
			Type:      v1beta2.SparkApplicationTypePython,
			SparkConf: map[string]string{"spark.executor.pyspark.memory": "512ub"},
		},
	}

	_, err := executorPysparkMemoryBytes(app)
	assert.NotNil(t, err)
}

func TestSparkOffHeapMemoryBytes(t *testing.T) {
	testCases := []struct {
		name     string
		conf     map[string]string
		expected int64
	}{
		{
			name:     "no off-heap conf",
			conf:     map[string]string{},
			expected: 0,
		},
		{
			name:     "off-heap explicitly disabled",
			conf:     map[string]string{"spark.memory.offHeap.enabled": "false", "spark.memory.offHeap.size": "1g"},
			expected: 0,
		},
		{
			name:     "off-heap enabled without a size",
			conf:     map[string]string{"spark.memory.offHeap.enabled": "true"},
			expected: 0,
		},
		{
			name:     "off-heap enabled with a size",
			conf:     map[string]string{"spark.memory.offHeap.enabled": "true", "spark.memory.offHeap.size": "1g"},
			expected: 1024 * 1024 * 1024,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			app := &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:      v1beta2.SparkApplicationTypeScala,
					SparkConf: tc.conf,
				},
			}
			actual, err := sparkOffHeapMemoryBytes(app)
			assert.Nil(t, err)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestSparkOffHeapMemoryBytesInvalid(t *testing.T) {
	app := &v1beta2.SparkApplication{
		Spec: v1beta2.SparkApplicationSpec{
			Type: v1beta2.SparkApplicationTypeScala,
			SparkConf: map[string]string{
				"spark.memory.offHeap.enabled": "true",
				"spark.memory.offHeap.size":    "abc",
			},
		},
	}

	_, err := sparkOffHeapMemoryBytes(app)
	assert.NotNil(t, err)
}

func TestDriverMemoryRequest(t *testing.T) {
	testCases := []struct {
		name     string
		app      *v1beta2.SparkApplication
		expected string
	}{
		{
			name: "jvm application where the minimum overhead dominates",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:   v1beta2.SparkApplicationTypeScala,
					Driver: v1beta2.DriverSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("512m")}},
				},
			},
			// 512Mi + max(512Mi * 0.1, 384Mi) = 512 + 384
			expected: "896Mi",
		},
		{
			name: "jvm application where the overhead factor dominates",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:   v1beta2.SparkApplicationTypeScala,
					Driver: v1beta2.DriverSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("4g")}},
				},
			},
			// 4Gi + floor(4Gi * 0.1) = 4096 + 409.6, floored to MiB
			expected: "4505Mi",
		},
		{
			name: "python application uses the larger non-jvm factor",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:   v1beta2.SparkApplicationTypePython,
					Driver: v1beta2.DriverSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("4g")}},
				},
			},
			// 4Gi + floor(4Gi * 0.4) = 4096 + 1638.4, floored to MiB
			expected: "5734Mi",
		},
		{
			name: "explicit overhead bypasses the factor",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeScala,
					Driver: v1beta2.DriverSpec{SparkPodSpec: v1beta2.SparkPodSpec{
						Memory:         ptr.To("4g"),
						MemoryOverhead: ptr.To("1g"),
					}},
				},
			},
			// 4Gi + 1Gi, the factor is not consulted
			expected: "5120Mi",
		},
		{
			name: "explicit overhead factor",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:                 v1beta2.SparkApplicationTypeScala,
					MemoryOverheadFactor: ptr.To("0.3"),
					Driver:               v1beta2.DriverSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("4g")}},
				},
			},
			// 4Gi + floor(4Gi * 0.3) = 4096 + 1228.8, floored to MiB
			expected: "5324Mi",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := driverMemoryRequest(tc.app)
			assert.Nil(t, err)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestDriverMemoryRequestInvalid(t *testing.T) {
	testCases := []struct {
		name string
		app  *v1beta2.SparkApplication
	}{
		{
			name: "invalid overhead factor",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:                 v1beta2.SparkApplicationTypeScala,
					MemoryOverheadFactor: ptr.To("abc"),
				},
			},
		},
		{
			name: "invalid driver memory",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:   v1beta2.SparkApplicationTypeScala,
					Driver: v1beta2.DriverSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("512ub")}},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := driverMemoryRequest(tc.app)
			assert.NotNil(t, err)
		})
	}
}

func TestExecutorMemoryRequest(t *testing.T) {
	testCases := []struct {
		name     string
		app      *v1beta2.SparkApplication
		expected string
	}{
		{
			name: "jvm application with no extra memory",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:     v1beta2.SparkApplicationTypeScala,
					Executor: v1beta2.ExecutorSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("512m")}},
				},
			},
			// 512Mi + max(512Mi * 0.1, 384Mi) = 512 + 384
			expected: "896Mi",
		},
		{
			name: "python application adds pyspark memory",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:      v1beta2.SparkApplicationTypePython,
					SparkConf: map[string]string{"spark.executor.pyspark.memory": "512"},
					Executor:  v1beta2.ExecutorSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("512m")}},
				},
			},
			// 512 + 384 overhead + 512 pyspark
			expected: "1408Mi",
		},
		{
			name: "jvm application adds off-heap memory",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeScala,
					SparkConf: map[string]string{
						"spark.memory.offHeap.enabled": "true",
						"spark.memory.offHeap.size":    "1g",
					},
					Executor: v1beta2.ExecutorSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("1g")}},
				},
			},
			// 1024 + 384 overhead + 1024 off-heap
			expected: "2432Mi",
		},
		{
			name: "python application adds both pyspark and off-heap memory",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypePython,
					SparkConf: map[string]string{
						"spark.executor.pyspark.memory": "1g",
						"spark.memory.offHeap.enabled":  "true",
						"spark.memory.offHeap.size":     "512m",
					},
					Executor: v1beta2.ExecutorSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("2g")}},
				},
			},
			// 2048 + floor(2048 * 0.4) + 1024 pyspark + 512 off-heap
			expected: "4403Mi",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := executorMemoryRequest(tc.app)
			assert.Nil(t, err)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestExecutorMemoryRequestInvalid(t *testing.T) {
	testCases := []struct {
		name string
		app  *v1beta2.SparkApplication
	}{
		{
			name: "invalid executor memory",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:     v1beta2.SparkApplicationTypeScala,
					Executor: v1beta2.ExecutorSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("512ub")}},
				},
			},
		},
		{
			name: "invalid pyspark memory",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:      v1beta2.SparkApplicationTypePython,
					SparkConf: map[string]string{"spark.executor.pyspark.memory": "512ub"},
					Executor:  v1beta2.ExecutorSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("512m")}},
				},
			},
		},
		{
			name: "invalid off-heap size",
			app: &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeScala,
					SparkConf: map[string]string{
						"spark.memory.offHeap.enabled": "true",
						"spark.memory.offHeap.size":    "abc",
					},
					Executor: v1beta2.ExecutorSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("512m")}},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := executorMemoryRequest(tc.app)
			assert.NotNil(t, err)
		})
	}
}
