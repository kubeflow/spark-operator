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

package resourceusage

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

var _ = Describe("bytesToMi", func() {
	DescribeTable("Should floor the value to the nearest mebibyte",
		func(input int64, expected string) {
			Expect(bytesToMi(input)).To(Equal(expected))
		},
		Entry("one byte below two mebibytes", int64((2*1024*1024)-1), "1Mi"),
		Entry("exactly two mebibytes", int64(2*1024*1024), "2Mi"),
		Entry("one byte below one gibibyte", int64((1024*1024*1024)-1), "1023Mi"),
		Entry("exactly one gibibyte", int64(1024*1024*1024), "1024Mi"),
	)
})

var _ = Describe("isJavaApp", func() {
	DescribeTable("Should report whether the application runs on the JVM",
		func(appType v1beta2.SparkApplicationType, expected bool) {
			Expect(isJavaApp(appType)).To(Equal(expected))
		},
		Entry("java", v1beta2.SparkApplicationTypeJava, true),
		Entry("scala", v1beta2.SparkApplicationTypeScala, true),
		Entry("python", v1beta2.SparkApplicationTypePython, false),
		Entry("r", v1beta2.SparkApplicationTypeR, false),
		Entry("empty", v1beta2.SparkApplicationType(""), false),
	)
})

var _ = Describe("getMemoryOverheadFactor", func() {
	DescribeTable("Should return the expected memory overhead factor",
		func(app *v1beta2.SparkApplication, expected float64) {
			actual, err := getMemoryOverheadFactor(app)
			Expect(err).NotTo(HaveOccurred())
			Expect(actual).To(BeNumerically("~", expected, 1e-9))
		},
		Entry("explicit factor takes precedence over the application type",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:                 v1beta2.SparkApplicationTypeJava,
					MemoryOverheadFactor: ptr.To("0.3"),
				},
			},
			0.3,
		),
		Entry("java application defaults to the JVM factor",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{Type: v1beta2.SparkApplicationTypeJava},
			},
			common.DefaultJVMMemoryOverheadFactor,
		),
		Entry("scala application defaults to the JVM factor",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{Type: v1beta2.SparkApplicationTypeScala},
			},
			common.DefaultJVMMemoryOverheadFactor,
		),
		Entry("python application defaults to the non-JVM factor",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{Type: v1beta2.SparkApplicationTypePython},
			},
			common.DefaultNonJVMMemoryOverheadFactor,
		),
		Entry("r application defaults to the non-JVM factor",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{Type: v1beta2.SparkApplicationTypeR},
			},
			common.DefaultNonJVMMemoryOverheadFactor,
		),
		Entry("zero factor is parsed and passed through",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:                 v1beta2.SparkApplicationTypeScala,
					MemoryOverheadFactor: ptr.To("0"),
				},
			},
			0.0,
		),
		Entry("negative factor is parsed and passed through",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:                 v1beta2.SparkApplicationTypeScala,
					MemoryOverheadFactor: ptr.To("-0.5"),
				},
			},
			-0.5,
		),
	)

	DescribeTable("Should return an error for a factor that cannot be parsed",
		func(factor string) {
			app := &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:                 v1beta2.SparkApplicationTypeJava,
					MemoryOverheadFactor: ptr.To(factor),
				},
			}
			_, err := getMemoryOverheadFactor(app)
			Expect(err).To(HaveOccurred())
		},
		Entry("empty string", ""),
		Entry("non-numeric string", "abc"),
		Entry("two decimal points", "0.1.2"),
		Entry("percentage", "10%"),
	)
})

var _ = Describe("memoryRequestBytes", func() {
	DescribeTable("Should return the expected memory request in bytes",
		func(podSpec *v1beta2.SparkPodSpec, memoryOverheadFactor float64, expected int64) {
			actual, err := memoryRequestBytes(podSpec, memoryOverheadFactor)
			Expect(err).NotTo(HaveOccurred())
			Expect(actual).To(Equal(expected))
		},
		Entry("no memory set falls back to the minimum overhead",
			&v1beta2.SparkPodSpec{},
			common.DefaultJVMMemoryOverheadFactor,
			int64(384*1024*1024),
		),
		Entry("minimum overhead wins when the factor yields less",
			&v1beta2.SparkPodSpec{Memory: ptr.To("512m")},
			common.DefaultJVMMemoryOverheadFactor,
			int64((512+384)*1024*1024),
		),
		// 4Gi + floor(4Gi * 0.1)
		Entry("factor wins once it exceeds the minimum overhead",
			&v1beta2.SparkPodSpec{Memory: ptr.To("4g")},
			common.DefaultJVMMemoryOverheadFactor,
			int64(4724464025),
		),
		Entry("explicit overhead bypasses the factor",
			&v1beta2.SparkPodSpec{Memory: ptr.To("4g"), MemoryOverhead: ptr.To("1g")},
			common.DefaultJVMMemoryOverheadFactor,
			int64(5*1024*1024*1024),
		),
		Entry("non-JVM factor still floors at the minimum overhead",
			&v1beta2.SparkPodSpec{Memory: ptr.To("512m")},
			common.DefaultNonJVMMemoryOverheadFactor,
			int64((512+384)*1024*1024),
		),
		Entry("explicit overhead with no memory set",
			&v1beta2.SparkPodSpec{MemoryOverhead: ptr.To("512m")},
			common.DefaultJVMMemoryOverheadFactor,
			int64(512*1024*1024),
		),
		// 4Gi * 0 is below the floor, so the 384Mi minimum applies
		Entry("zero factor floors at the minimum overhead",
			&v1beta2.SparkPodSpec{Memory: ptr.To("4g")},
			0.0,
			int64((4096+384)*1024*1024),
		),
		// math.Max clamps the negative product to the 384Mi minimum
		Entry("negative factor floors at the minimum overhead",
			&v1beta2.SparkPodSpec{Memory: ptr.To("4g")},
			-0.5,
			int64((4096+384)*1024*1024),
		),
	)

	DescribeTable("Should return an error for memory values that cannot be parsed",
		func(podSpec *v1beta2.SparkPodSpec) {
			_, err := memoryRequestBytes(podSpec, common.DefaultJVMMemoryOverheadFactor)
			Expect(err).To(HaveOccurred())
		},
		Entry("invalid memory", &v1beta2.SparkPodSpec{Memory: ptr.To("512ub")}),
		Entry("invalid memory overhead",
			&v1beta2.SparkPodSpec{Memory: ptr.To("512m"), MemoryOverhead: ptr.To("abc")},
		),
	)
})

var _ = Describe("executorPysparkMemoryBytes", func() {
	DescribeTable("Should return the expected PySpark memory in bytes",
		func(appType v1beta2.SparkApplicationType, conf map[string]string, expected int64) {
			app := &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{Type: appType, SparkConf: conf},
			}
			actual, err := executorPysparkMemoryBytes(app)
			Expect(err).NotTo(HaveOccurred())
			Expect(actual).To(Equal(expected))
		},
		Entry("bare number defaults to mebibytes",
			v1beta2.SparkApplicationTypePython,
			map[string]string{"spark.executor.pyspark.memory": "512"},
			int64(512*1024*1024),
		),
		Entry("explicit suffix is honoured",
			v1beta2.SparkApplicationTypePython,
			map[string]string{"spark.executor.pyspark.memory": "1g"},
			int64(1024*1024*1024),
		),
		Entry("conf is ignored for non-python applications",
			v1beta2.SparkApplicationTypeJava,
			map[string]string{"spark.executor.pyspark.memory": "512"},
			int64(0),
		),
		Entry("python application without the conf",
			v1beta2.SparkApplicationTypePython,
			map[string]string{},
			int64(0),
		),
		Entry("python application with a nil spark conf",
			v1beta2.SparkApplicationTypePython,
			nil,
			int64(0),
		),
	)

	It("Should return an error for a PySpark memory value that cannot be parsed", func() {
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type:      v1beta2.SparkApplicationTypePython,
				SparkConf: map[string]string{"spark.executor.pyspark.memory": "512ub"},
			},
		}

		_, err := executorPysparkMemoryBytes(app)
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("sparkOffHeapMemoryBytes", func() {
	DescribeTable("Should return the expected off-heap memory in bytes",
		func(conf map[string]string, expected int64) {
			app := &v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:      v1beta2.SparkApplicationTypeScala,
					SparkConf: conf,
				},
			}
			actual, err := sparkOffHeapMemoryBytes(app)
			Expect(err).NotTo(HaveOccurred())
			Expect(actual).To(Equal(expected))
		},
		Entry("no off-heap conf", map[string]string{}, int64(0)),
		Entry("off-heap explicitly disabled",
			map[string]string{"spark.memory.offHeap.enabled": "false", "spark.memory.offHeap.size": "1g"},
			int64(0),
		),
		Entry("off-heap enabled without a size",
			map[string]string{"spark.memory.offHeap.enabled": "true"},
			int64(0),
		),
		Entry("off-heap enabled with a size",
			map[string]string{"spark.memory.offHeap.enabled": "true", "spark.memory.offHeap.size": "1g"},
			int64(1024*1024*1024),
		),
	)

	It("Should return an error for an off-heap size that cannot be parsed", func() {
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
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("driverMemoryRequest", func() {
	DescribeTable("Should return the expected driver memory request",
		func(app *v1beta2.SparkApplication, expected string) {
			actual, err := driverMemoryRequest(app)
			Expect(err).NotTo(HaveOccurred())
			Expect(actual).To(Equal(expected))
		},
		// 512Mi + max(512Mi * 0.1, 384Mi) = 512 + 384
		Entry("jvm application where the minimum overhead dominates",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:   v1beta2.SparkApplicationTypeScala,
					Driver: v1beta2.DriverSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("512m")}},
				},
			},
			"896Mi",
		),
		// 4Gi + floor(4Gi * 0.1) = 4096 + 409.6, floored to MiB
		Entry("jvm application where the overhead factor dominates",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:   v1beta2.SparkApplicationTypeScala,
					Driver: v1beta2.DriverSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("4g")}},
				},
			},
			"4505Mi",
		),
		// 4Gi + floor(4Gi * 0.4) = 4096 + 1638.4, floored to MiB
		Entry("python application uses the larger non-jvm factor",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:   v1beta2.SparkApplicationTypePython,
					Driver: v1beta2.DriverSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("4g")}},
				},
			},
			"5734Mi",
		),
		// 4Gi + 1Gi, the factor is not consulted
		Entry("explicit overhead bypasses the factor",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeScala,
					Driver: v1beta2.DriverSpec{SparkPodSpec: v1beta2.SparkPodSpec{
						Memory:         ptr.To("4g"),
						MemoryOverhead: ptr.To("1g"),
					}},
				},
			},
			"5120Mi",
		),
		// 4Gi + floor(4Gi * 0.3) = 4096 + 1228.8, floored to MiB
		Entry("explicit overhead factor",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:                 v1beta2.SparkApplicationTypeScala,
					MemoryOverheadFactor: ptr.To("0.3"),
					Driver:               v1beta2.DriverSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("4g")}},
				},
			},
			"5324Mi",
		),
	)

	DescribeTable("Should return an error when the driver memory cannot be resolved",
		func(app *v1beta2.SparkApplication) {
			_, err := driverMemoryRequest(app)
			Expect(err).To(HaveOccurred())
		},
		Entry("invalid overhead factor",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:                 v1beta2.SparkApplicationTypeScala,
					MemoryOverheadFactor: ptr.To("abc"),
				},
			},
		),
		Entry("invalid driver memory",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:   v1beta2.SparkApplicationTypeScala,
					Driver: v1beta2.DriverSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("512ub")}},
				},
			},
		),
	)
})

var _ = Describe("executorMemoryRequest", func() {
	DescribeTable("Should return the expected executor memory request",
		func(app *v1beta2.SparkApplication, expected string) {
			actual, err := executorMemoryRequest(app)
			Expect(err).NotTo(HaveOccurred())
			Expect(actual).To(Equal(expected))
		},
		// 512Mi + max(512Mi * 0.1, 384Mi) = 512 + 384
		Entry("jvm application with no extra memory",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:     v1beta2.SparkApplicationTypeScala,
					Executor: v1beta2.ExecutorSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("512m")}},
				},
			},
			"896Mi",
		),
		// 512 + 384 overhead + 512 pyspark
		Entry("python application adds pyspark memory",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:      v1beta2.SparkApplicationTypePython,
					SparkConf: map[string]string{"spark.executor.pyspark.memory": "512"},
					Executor:  v1beta2.ExecutorSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("512m")}},
				},
			},
			"1408Mi",
		),
		// 1024 + 384 overhead + 1024 off-heap
		Entry("jvm application adds off-heap memory",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeScala,
					SparkConf: map[string]string{
						"spark.memory.offHeap.enabled": "true",
						"spark.memory.offHeap.size":    "1g",
					},
					Executor: v1beta2.ExecutorSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("1g")}},
				},
			},
			"2432Mi",
		),
		// 2048 + floor(2048 * 0.4) + 1024 pyspark + 512 off-heap
		Entry("python application adds both pyspark and off-heap memory",
			&v1beta2.SparkApplication{
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
			"4403Mi",
		),
	)

	DescribeTable("Should return an error when the executor memory cannot be resolved",
		func(app *v1beta2.SparkApplication) {
			_, err := executorMemoryRequest(app)
			Expect(err).To(HaveOccurred())
		},
		Entry("invalid executor memory",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:     v1beta2.SparkApplicationTypeScala,
					Executor: v1beta2.ExecutorSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("512ub")}},
				},
			},
		),
		Entry("invalid pyspark memory",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:      v1beta2.SparkApplicationTypePython,
					SparkConf: map[string]string{"spark.executor.pyspark.memory": "512ub"},
					Executor:  v1beta2.ExecutorSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("512m")}},
				},
			},
		),
		Entry("invalid off-heap size",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeScala,
					SparkConf: map[string]string{
						"spark.memory.offHeap.enabled": "true",
						"spark.memory.offHeap.size":    "abc",
					},
					Executor: v1beta2.ExecutorSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("512m")}},
				},
			},
		),
	)
})
