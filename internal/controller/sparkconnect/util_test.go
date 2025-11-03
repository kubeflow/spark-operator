/*
Copyright 2025 The kubeflow authors.

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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

var _ = Describe("Util functions", func() {
	var conn *v1alpha1.SparkConnect

	BeforeEach(func() {
		conn = &v1alpha1.SparkConnect{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-spark-connect",
				Namespace: "test-namespace",
			},
			Spec: v1alpha1.SparkConnectSpec{
				SparkVersion: "4.0.0",
				Server: v1alpha1.ServerSpec{
					SparkPodSpec: v1alpha1.SparkPodSpec{},
				},
				Executor: v1alpha1.ExecutorSpec{
					SparkPodSpec: v1alpha1.SparkPodSpec{},
				},
			},
		}
	})

	Context("GetCommonLabels", func() {
		It("should return correct common labels", func() {
			labels := GetCommonLabels(conn)
			Expect(labels).To(HaveLen(2))
			Expect(labels).To(HaveKeyWithValue(common.LabelCreatedBySparkOperator, "true"))
			Expect(labels).To(HaveKeyWithValue(common.LabelSparkConnectName, "test-spark-connect"))
		})
	})

	Context("GetServerSelectorLabels", func() {
		It("should return correct server selector labels", func() {
			labels := GetServerSelectorLabels(conn)
			Expect(labels).To(HaveLen(4))
			Expect(labels).To(HaveKeyWithValue(common.LabelLaunchedBySparkOperator, "true"))
			Expect(labels).To(HaveKeyWithValue(common.LabelSparkConnectName, "test-spark-connect"))
			Expect(labels).To(HaveKeyWithValue(common.LabelSparkRole, common.SparkRoleConnectServer))
			Expect(labels).To(HaveKeyWithValue(common.LabelSparkVersion, "4.0.0"))
		})
	})

	Context("GetExecutorSelectorLabels", func() {
		It("should return correct executor selector labels", func() {
			labels := GetExecutorSelectorLabels(conn)
			Expect(labels).To(HaveLen(3))
			Expect(labels).To(HaveKeyWithValue(common.LabelLaunchedBySparkOperator, "true"))
			Expect(labels).To(HaveKeyWithValue(common.LabelSparkConnectName, "test-spark-connect"))
			Expect(labels).To(HaveKeyWithValue(common.LabelSparkRole, common.SparkRoleExecutor))
		})
	})

	Context("GetConfigMapName", func() {
		It("should return correct config map name", func() {
			name := GetConfigMapName(conn)
			Expect(name).To(Equal("test-spark-connect-conf"))
		})
	})

	Context("GetServerPodName", func() {
		It("should return correct server pod name", func() {
			name := GetServerPodName(conn)
			Expect(name).To(Equal("test-spark-connect-server"))
		})
	})

	Context("GetServerServiceName", func() {
		When("service is not specified in server spec", func() {
			It("should return default server service name", func() {
				name := GetServerServiceName(conn)
				Expect(name).To(Equal("test-spark-connect-server"))
			})
		})

		When("service is specified in server spec", func() {
			BeforeEach(func() {
				conn.Spec.Server.Service = &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{
						Name: "custom-service-name",
					},
				}
			})

			It("should return the specified service name", func() {
				name := GetServerServiceName(conn)
				Expect(name).To(Equal("custom-service-name"))
			})
		})
	})

	Context("GetServerServiceHost", func() {
		It("should return correct server service host", func() {
			host := GetServerServiceHost(conn)
			Expect(host).To(Equal("test-spark-connect-server.test-namespace.svc.cluster.local"))
		})
	})
})
