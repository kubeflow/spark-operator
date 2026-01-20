/*
Copyright 2025 The Kubeflow authors.

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

package sparkconnect

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

var _ = Describe("Options functions", func() {
	Context("imageOption", func() {
		It("handles nil executor template and falls back to spec.image", func() {
			image := "apache/spark:3.5.0"
			conn := &v1alpha1.SparkConnect{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-spark",
					Namespace: "default",
				},
				Spec: v1alpha1.SparkConnectSpec{
					SparkVersion: "3.5.0",
					Image:        &image,
					Server:       v1alpha1.ServerSpec{},
					Executor:     v1alpha1.ExecutorSpec{},
				},
			}

			args, err := imageOption(conn)
			Expect(err).NotTo(HaveOccurred())
			Expect(args).To(ContainElements(
				"--conf",
				SatisfyAll(
					ContainSubstring(common.SparkKubernetesContainerImage+"="+image),
				),
				"--conf",
				SatisfyAll(
					ContainSubstring(common.SparkKubernetesExecutorContainerImage+"="+image),
				),
			))
		})
	})
})
