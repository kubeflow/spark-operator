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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

var _ = Describe("cpuRequest", func() {
	DescribeTable("Should return the expected cpu request",
		func(cores *int32, coreRequest *string, expected string) {
			actual, err := cpuRequest(cores, coreRequest)
			Expect(err).NotTo(HaveOccurred())
			Expect(actual).To(Equal(expected))
		},
		Entry("neither cores nor core request set", nil, nil, "1"),
		Entry("only cores set", ptr.To[int32](1), nil, "1"),
		Entry("only core request set", nil, ptr.To("1"), "1"),
		Entry("core request takes precedence over cores", ptr.To[int32](1), ptr.To("500m"), "500m"),
	)

	DescribeTable("Should return an error for a core request that cannot be parsed",
		func(coreRequest string) {
			_, err := cpuRequest(nil, &coreRequest)
			Expect(err).To(HaveOccurred())
		},
		Entry("empty string", ""),
		Entry("non-numeric string", "asd"),
		Entry("trailing quantity", "Random 500m"),
	)
})

var _ = Describe("DriverPodRequests", func() {
	DescribeTable("Should return the expected driver pod requests",
		func(app *v1beta2.SparkApplication, expected map[string]string) {
			actual, err := DriverPodRequests(app)
			Expect(err).NotTo(HaveOccurred())
			Expect(actual).To(Equal(expected))
		},
		Entry("defaults when neither cores nor memory are set",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{Type: v1beta2.SparkApplicationTypeScala},
			},
			map[string]string{"cpu": "1", "memory": "384Mi"},
		),
		Entry("cores and memory",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeScala,
					Driver: v1beta2.DriverSpec{SparkPodSpec: v1beta2.SparkPodSpec{
						Cores:  ptr.To[int32](2),
						Memory: ptr.To("4g"),
					}},
				},
			},
			map[string]string{"cpu": "2", "memory": "4505Mi"},
		),
		Entry("core request takes precedence over cores",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeScala,
					Driver: v1beta2.DriverSpec{
						CoreRequest:  ptr.To("500m"),
						SparkPodSpec: v1beta2.SparkPodSpec{Cores: ptr.To[int32](2), Memory: ptr.To("512m")},
					},
				},
			},
			map[string]string{"cpu": "500m", "memory": "896Mi"},
		),
	)

	DescribeTable("Should return an error when the driver requests cannot be resolved",
		func(app *v1beta2.SparkApplication) {
			_, err := DriverPodRequests(app)
			Expect(err).To(HaveOccurred())
		},
		Entry("invalid core request",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:   v1beta2.SparkApplicationTypeScala,
					Driver: v1beta2.DriverSpec{CoreRequest: ptr.To("abc")},
				},
			},
		),
		Entry("invalid memory",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:   v1beta2.SparkApplicationTypeScala,
					Driver: v1beta2.DriverSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("512ub")}},
				},
			},
		),
	)
})

var _ = Describe("ExecutorPodRequests", func() {
	DescribeTable("Should return the expected executor pod requests",
		func(app *v1beta2.SparkApplication, expected map[string]string) {
			actual, err := ExecutorPodRequests(app)
			Expect(err).NotTo(HaveOccurred())
			Expect(actual).To(Equal(expected))
		},
		Entry("defaults when neither cores nor memory are set",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{Type: v1beta2.SparkApplicationTypeScala},
			},
			map[string]string{"cpu": "1", "memory": "384Mi"},
		),
		Entry("cores and memory",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type: v1beta2.SparkApplicationTypeScala,
					Executor: v1beta2.ExecutorSpec{SparkPodSpec: v1beta2.SparkPodSpec{
						Cores:  ptr.To[int32](4),
						Memory: ptr.To("4g"),
					}},
				},
			},
			map[string]string{"cpu": "4", "memory": "4505Mi"},
		),
		Entry("python executor includes pyspark memory",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:      v1beta2.SparkApplicationTypePython,
					SparkConf: map[string]string{"spark.executor.pyspark.memory": "512"},
					Executor: v1beta2.ExecutorSpec{
						CoreRequest:  ptr.To("500m"),
						SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("512m")},
					},
				},
			},
			map[string]string{"cpu": "500m", "memory": "1408Mi"},
		),
	)

	DescribeTable("Should return an error when the executor requests cannot be resolved",
		func(app *v1beta2.SparkApplication) {
			_, err := ExecutorPodRequests(app)
			Expect(err).To(HaveOccurred())
		},
		Entry("invalid core request",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:     v1beta2.SparkApplicationTypeScala,
					Executor: v1beta2.ExecutorSpec{CoreRequest: ptr.To("abc")},
				},
			},
		),
		Entry("invalid memory",
			&v1beta2.SparkApplication{
				Spec: v1beta2.SparkApplicationSpec{
					Type:     v1beta2.SparkApplicationTypeScala,
					Executor: v1beta2.ExecutorSpec{SparkPodSpec: v1beta2.SparkPodSpec{Memory: ptr.To("512ub")}},
				},
			},
		),
	)
})
