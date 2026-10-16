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

func TestGetMemoryOverheadFactor(t *testing.T) {
	testCases := []struct {
		name     string
		app      *v1beta2.SparkApplication
		expected float64
	}{
		{
			"explicit factor overrides type default",
			&v1beta2.SparkApplication{Spec: v1beta2.SparkApplicationSpec{
				Type: v1beta2.SparkApplicationTypeJava, MemoryOverheadFactor: ptr.To("0.2"),
			}},
			0.2,
		},
		{
			"java app defaults to jvm factor",
			&v1beta2.SparkApplication{Spec: v1beta2.SparkApplicationSpec{Type: v1beta2.SparkApplicationTypeJava}},
			common.DefaultJVMMemoryOverheadFactor,
		},
		{
			"scala app defaults to jvm factor",
			&v1beta2.SparkApplication{Spec: v1beta2.SparkApplicationSpec{Type: v1beta2.SparkApplicationTypeScala}},
			common.DefaultJVMMemoryOverheadFactor,
		},
		{
			"python app defaults to non-jvm factor",
			&v1beta2.SparkApplication{Spec: v1beta2.SparkApplicationSpec{Type: v1beta2.SparkApplicationTypePython}},
			common.DefaultNonJVMMemoryOverheadFactor,
		},
		{
			"r app defaults to non-jvm factor",
			&v1beta2.SparkApplication{Spec: v1beta2.SparkApplicationSpec{Type: v1beta2.SparkApplicationTypeR}},
			common.DefaultNonJVMMemoryOverheadFactor,
		},
	}

	for _, tc := range testCases {
		actual, err := getMemoryOverheadFactor(tc.app)
		assert.Nil(t, err)
		assert.Equal(t, tc.expected, actual)
	}

	_, err := getMemoryOverheadFactor(&v1beta2.SparkApplication{Spec: v1beta2.SparkApplicationSpec{
		MemoryOverheadFactor: ptr.To("not-a-float"),
	}})
	assert.NotNil(t, err)
}

func TestMemoryRequestBytes(t *testing.T) {
	testCases := []struct {
		name     string
		podSpec  *v1beta2.SparkPodSpec
		factor   float64
		expected int64
	}{
		{
			"overhead computed from factor",
			&v1beta2.SparkPodSpec{Memory: ptr.To("1g")},
			0.1,
			1*1024*1024*1024 + common.MinMemoryOverhead,
		},
		{
			"explicit overhead takes precedence",
			&v1beta2.SparkPodSpec{Memory: ptr.To("1g"), MemoryOverhead: ptr.To("100m")},
			0.1,
			1*1024*1024*1024 + 100*1024*1024,
		},
	}

	for _, tc := range testCases {
		actual, err := memoryRequestBytes(tc.podSpec, tc.factor)
		assert.Nil(t, err, tc.name)
		assert.Equal(t, tc.expected, actual, tc.name)
	}

	_, err := memoryRequestBytes(&v1beta2.SparkPodSpec{Memory: ptr.To("bad")}, 0.1)
	assert.NotNil(t, err)
}

func TestExecutorPysparkMemoryBytes(t *testing.T) {
	testCases := []struct {
		name     string
		app      *v1beta2.SparkApplication
		expected int64
	}{
		{
			"non-python app is ignored",
			&v1beta2.SparkApplication{Spec: v1beta2.SparkApplicationSpec{
				Type:      v1beta2.SparkApplicationTypeJava,
				SparkConf: map[string]string{"spark.executor.pyspark.memory": "512"},
			}},
			0,
		},
		{
			"python app without the config is ignored",
			&v1beta2.SparkApplication{Spec: v1beta2.SparkApplicationSpec{Type: v1beta2.SparkApplicationTypePython}},
			0,
		},
		{
			"bare number defaults to mebibytes",
			&v1beta2.SparkApplication{Spec: v1beta2.SparkApplicationSpec{
				Type:      v1beta2.SparkApplicationTypePython,
				SparkConf: map[string]string{"spark.executor.pyspark.memory": "512"},
			}},
			512 * 1024 * 1024,
		},
	}

	for _, tc := range testCases {
		actual, err := executorPysparkMemoryBytes(tc.app)
		assert.Nil(t, err, tc.name)
		assert.Equal(t, tc.expected, actual, tc.name)
	}

	_, err := executorPysparkMemoryBytes(&v1beta2.SparkApplication{Spec: v1beta2.SparkApplicationSpec{
		Type:      v1beta2.SparkApplicationTypePython,
		SparkConf: map[string]string{"spark.executor.pyspark.memory": "bad"},
	}})
	assert.NotNil(t, err)
}

func TestSparkOffHeapMemoryBytes(t *testing.T) {
	testCases := []struct {
		name      string
		sparkConf map[string]string
		expected  int64
	}{
		{"not configured", nil, 0},
		{"disabled", map[string]string{"spark.memory.offHeap.enabled": "false"}, 0},
		{"enabled without size", map[string]string{"spark.memory.offHeap.enabled": "true"}, 0},
		{
			"enabled with size",
			map[string]string{"spark.memory.offHeap.enabled": "true", "spark.memory.offHeap.size": "256m"},
			256 * 1024 * 1024,
		},
	}

	for _, tc := range testCases {
		app := &v1beta2.SparkApplication{Spec: v1beta2.SparkApplicationSpec{SparkConf: tc.sparkConf}}
		actual, err := sparkOffHeapMemoryBytes(app)
		assert.Nil(t, err, tc.name)
		assert.Equal(t, tc.expected, actual, tc.name)
	}

	_, err := sparkOffHeapMemoryBytes(&v1beta2.SparkApplication{Spec: v1beta2.SparkApplicationSpec{
		SparkConf: map[string]string{"spark.memory.offHeap.enabled": "true", "spark.memory.offHeap.size": "bad"},
	}})
	assert.NotNil(t, err)
}

func TestDriverMemoryRequest(t *testing.T) {
	app := &v1beta2.SparkApplication{Spec: v1beta2.SparkApplicationSpec{
		Type:   v1beta2.SparkApplicationTypeJava,
		Driver: v1beta2.DriverSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("1g")}},
	}}

	actual, err := driverMemoryRequest(app)
	assert.Nil(t, err)
	assert.Equal(t, "1408Mi", actual)
}

func TestExecutorMemoryRequest(t *testing.T) {
	app := &v1beta2.SparkApplication{Spec: v1beta2.SparkApplicationSpec{
		Type:     v1beta2.SparkApplicationTypePython,
		Executor: v1beta2.ExecutorSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("1g")}},
		SparkConf: map[string]string{
			"spark.executor.pyspark.memory": "512m",
			"spark.memory.offHeap.enabled":  "true",
			"spark.memory.offHeap.size":     "256m",
		},
	}}

	actual, err := executorMemoryRequest(app)
	assert.Nil(t, err)
	assert.Equal(t, "2201Mi", actual)
}
