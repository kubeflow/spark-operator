/*
Copyright 2026 The Kubeflow authors.

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

package e2e_test

import (
	"context"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/internal/controller/sparkconnect"
)

var _ = Describe("SparkConnect CPU Resources", func() {
	// Test the new CoreRequest/CoreLimit fields at the API/runtime level.
	// These tests create a SparkConnect in-memory (rather than loading the
	// example yaml) so that they can assert the specific values they wrote
	// are actually applied to the operator-created server pod and surfaced
	// in the spark-submit args for executor pods.
	Context("Apply server CoreRequest/CoreLimit to the server pod", func() {
		ctx := context.Background()

		var conn *v1alpha1.SparkConnect

		BeforeEach(func() {
			image := "apache/spark:4.0.0"
			conn = &v1alpha1.SparkConnect{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "spark-connect-resources",
					Namespace: "default",
				},
				Spec: v1alpha1.SparkConnectSpec{
					Image:        &image,
					SparkVersion: "4.0.0",
					Server: v1alpha1.ServerSpec{
						SparkPodSpec: v1alpha1.SparkPodSpec{
							Cores:       ptr.To[int32](1),
							CoreRequest: ptr.To("500m"),
							CoreLimit:   ptr.To("1"),
						},
					},
					Executor: v1alpha1.ExecutorSpec{
						SparkPodSpec: v1alpha1.SparkPodSpec{
							Cores:       ptr.To[int32](1),
							CoreRequest: ptr.To("500m"),
							CoreLimit:   ptr.To("1500m"),
						},
						Instances: ptr.To[int32](1),
					},
				},
			}
		})

		AfterEach(func() {
			key := types.NamespacedName{Namespace: conn.Namespace, Name: conn.Name}
			if err := k8sClient.Get(ctx, key, conn); err == nil {
				Expect(k8sClient.Delete(ctx, conn)).To(Succeed())
			}
		})

		It("reflects server.coreRequest and server.coreLimit on the operator-created server pod", func() {
			By("Creating the SparkConnect")
			Expect(k8sClient.Create(ctx, conn)).To(Succeed())

			serverPodName := sparkconnect.GetServerPodName(conn)

			By("Waiting for the server pod to be created by the operator")
			serverPod := &corev1.Pod{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Namespace: conn.Namespace, Name: serverPodName}, serverPod)
			}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(Succeed())

			By("Asserting the server container CPU request matches spec.server.coreRequest")
			cpuReq, ok := serverPod.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU]
			Expect(ok).To(BeTrue(), "server pod should have a CPU request set")
			Expect(cpuReq.Equal(resource.MustParse("500m"))).To(BeTrue(),
				"expected server CPU request 500m, got %s", cpuReq.String())

			By("Asserting the server container CPU limit matches spec.server.coreLimit")
			cpuLim, ok := serverPod.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU]
			Expect(ok).To(BeTrue(), "server pod should have a CPU limit set")
			Expect(cpuLim.Equal(resource.MustParse("1"))).To(BeTrue(),
				"expected server CPU limit 1, got %s", cpuLim.String())
		})

		It("emits spark.kubernetes.executor.{request,limit}.cores via the operator-created server pod args", func() {
			By("Creating the SparkConnect")
			Expect(k8sClient.Create(ctx, conn)).To(Succeed())

			serverPodName := sparkconnect.GetServerPodName(conn)

			By("Waiting for the server pod to be created by the operator")
			serverPod := &corev1.Pod{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Namespace: conn.Namespace, Name: serverPodName}, serverPod)
			}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(Succeed())

			By("Asserting the server pod args contain the executor CPU conf keys")
			// The server pod's args string is built from buildStartConnectServerArgs and includes
			// --conf spark.kubernetes.executor.request.cores=... and --conf spark.kubernetes.executor.limit.cores=...
			// generated from executor.coreRequest / executor.coreLimit.
			Expect(serverPod.Spec.Containers).NotTo(BeEmpty())
			args := serverPod.Spec.Containers[0].Args
			Expect(args).NotTo(BeEmpty(), "server pod args should be set by the operator")
			allArgs := strings.Join(args, " ")

			Expect(allArgs).To(ContainSubstring("spark.kubernetes.executor.request.cores=500m"),
				"expected spark-submit args to include executor request cores 500m, got: %s", allArgs)
			Expect(allArgs).To(ContainSubstring("spark.kubernetes.executor.limit.cores=1500m"),
				"expected spark-submit args to include executor limit cores 1500m, got: %s", allArgs)
		})
	})

	Context("Precedence: spec.server.coreRequest overrides template CPU request", func() {
		// Tariq asked that the precedence rule be deterministic and explicit.
		// The operator's contract is: if both spec.server.coreRequest and
		// spec.server.template.spec.containers[].resources.requests.cpu are
		// set, spec.server.coreRequest wins for the CPU key. Other resource
		// keys (memory, ephemeral-storage) on the template are preserved.
		ctx := context.Background()

		var conn *v1alpha1.SparkConnect

		BeforeEach(func() {
			image := "apache/spark:4.0.0"
			conn = &v1alpha1.SparkConnect{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "spark-connect-precedence",
					Namespace: "default",
				},
				Spec: v1alpha1.SparkConnectSpec{
					Image:        &image,
					SparkVersion: "4.0.0",
					Server: v1alpha1.ServerSpec{
						SparkPodSpec: v1alpha1.SparkPodSpec{
							CoreRequest: ptr.To("500m"),
							CoreLimit:   ptr.To("1"),
							// Template also specifies CPU and memory.
							Template: &corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name:  "spark-kubernetes-driver",
											Image: image,
											Resources: corev1.ResourceRequirements{
												Requests: corev1.ResourceList{
													corev1.ResourceCPU:    resource.MustParse("1"),
													corev1.ResourceMemory: resource.MustParse("1Gi"),
												},
												Limits: corev1.ResourceList{
													corev1.ResourceCPU:    resource.MustParse("2"),
													corev1.ResourceMemory: resource.MustParse("1Gi"),
												},
											},
										},
									},
								},
							},
						},
					},
					Executor: v1alpha1.ExecutorSpec{
						Instances: ptr.To[int32](1),
					},
				},
			}
		})

		AfterEach(func() {
			key := types.NamespacedName{Namespace: conn.Namespace, Name: conn.Name}
			if err := k8sClient.Get(ctx, key, conn); err == nil {
				Expect(k8sClient.Delete(ctx, conn)).To(Succeed())
			}
		})

		It("overrides template CPU request/limit and preserves template memory", func() {
			By("Creating the SparkConnect")
			Expect(k8sClient.Create(ctx, conn)).To(Succeed())

			serverPodName := sparkconnect.GetServerPodName(conn)

			By("Waiting for the server pod to be created by the operator")
			serverPod := &corev1.Pod{}
			Eventually(func() error {
				return k8sClient.Get(ctx, types.NamespacedName{Namespace: conn.Namespace, Name: serverPodName}, serverPod)
			}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(Succeed())

			By("Asserting spec.server.coreRequest wins for the CPU request")
			cpuReq, ok := serverPod.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU]
			Expect(ok).To(BeTrue())
			Expect(cpuReq.Equal(resource.MustParse("500m"))).To(BeTrue(),
				"spec.server.coreRequest (500m) should win over template CPU request (1), got %s", cpuReq.String())

			By("Asserting spec.server.coreLimit wins for the CPU limit")
			cpuLim, ok := serverPod.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU]
			Expect(ok).To(BeTrue())
			Expect(cpuLim.Equal(resource.MustParse("1"))).To(BeTrue(),
				"spec.server.coreLimit (1) should win over template CPU limit (2), got %s", cpuLim.String())

			By("Asserting the template's memory request is preserved")
			memReq, ok := serverPod.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory]
			Expect(ok).To(BeTrue(), "template memory request should be preserved")
			Expect(memReq.Equal(resource.MustParse("1Gi"))).To(BeTrue(),
				"expected template memory 1Gi to be preserved, got %s", memReq.String())

			By("Asserting the template's memory limit is preserved")
			memLim, ok := serverPod.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory]
			Expect(ok).To(BeTrue(), "template memory limit should be preserved")
			Expect(memLim.Equal(resource.MustParse("1Gi"))).To(BeTrue(),
				"expected template memory 1Gi to be preserved, got %s", memLim.String())
		})
	})
})
