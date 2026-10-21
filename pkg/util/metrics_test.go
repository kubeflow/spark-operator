/*
Copyright The Kubeflow Authors.

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

package util_test

import (
	"github.com/kubeflow/spark-operator/v2/pkg/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("CreateValidMetricNameLabel", func() {
	DescribeTable("sanitizes prefix+name",
		func(prefix, name, expected string) {
			Expect(util.CreateValidMetricNameLabel(prefix, name)).To(Equal(expected))
		},
		Entry("replaces dashes in prefix and name", "spark-app-", "my-job", "spark_app_my_job"),
		Entry("leaves dash-free input unchanged", "spark_app_", "myjob", "spark_app_myjob"),
		Entry("empty prefix is the operator default", "", "spark-app-count", "spark_app_count"),
		Entry("both empty", "", "", ""),
		Entry("consecutive dashes", "", "a--b", "a__b"),
	)
})
