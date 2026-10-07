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
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/internal/controller/sparkconnect"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

var _ = Describe("SparkConnect CPU Resources", func() {
	Context("Apply server CoreRequest/CoreLimit to the server pod", func() {
		ctx := context.Background()

		var conn *v1alpha1.SparkConnect

		BeforeEach(func() {
			image := "docker.io/apache/spark:4.0.4"
			conn = &v1alpha1.SparkConnect{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "spark-connect-resources-",
					Namespace:    "default",
				},
				Spec: v1alpha1.SparkConnectSpec{
					Image:        &image,
					SparkVersion: "4.0.4",
					Server: v1alpha1.ServerSpec{
						SparkPodSpec: v1alpha1.SparkPodSpec{
							Cores:       ptr.To[int32](1),
							CoreRequest: ptr.To(resource.MustParse("500m")),
							CoreLimit:   ptr.To(resource.MustParse("1")),
						},
					},
					Executor: v1alpha1.ExecutorSpec{
						SparkPodSpec: v1alpha1.SparkPodSpec{
							Cores:       ptr.To[int32](1),
							CoreRequest: ptr.To(resource.MustParse("500m")),
							CoreLimit:   ptr.To(resource.MustParse("1500m")),
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

		It("applies server CPU resources to the server pod and emits executor CPU conf", func() {
			By("Creating the SparkConnect")
			Expect(k8sClient.Create(ctx, conn)).To(Succeed())

			By("Waiting for the operator to create the server pod")
			serverPod := waitForServerPod(ctx, conn)

			By("Asserting the server container CPU request matches spec.server.coreRequest")
			serverContainer := containerByName(serverPod, common.SparkDriverContainerName)
			cpuReq, ok := serverContainer.Resources.Requests[corev1.ResourceCPU]
			Expect(ok).To(BeTrue(), "server pod should have a CPU request set")
			Expect(cpuReq.Equal(resource.MustParse("500m"))).To(BeTrue(),
				"expected server CPU request 500m, got %s", cpuReq.String())

			By("Asserting the server container CPU limit matches spec.server.coreLimit")
			cpuLim, ok := serverContainer.Resources.Limits[corev1.ResourceCPU]
			Expect(ok).To(BeTrue(), "server pod should have a CPU limit set")
			Expect(cpuLim.Equal(resource.MustParse("1"))).To(BeTrue(),
				"expected server CPU limit 1, got %s", cpuLim.String())

			By("Asserting the server pod args contain the executor CPU conf keys")
			args := serverContainer.Args
			Expect(args).NotTo(BeEmpty(), "server pod args should be set by the operator")
			allArgs := strings.Join(args, " ")

			Expect(allArgs).To(ContainSubstring("spark.kubernetes.executor.request.cores=500m"),
				"expected spark-submit args to include executor request cores 500m, got: %s", allArgs)
			Expect(allArgs).To(ContainSubstring("spark.kubernetes.executor.limit.cores=1500m"),
				"expected spark-submit args to include executor limit cores 1500m, got: %s", allArgs)
		})
	})

	Context("Precedence: CRD CPU fields override the pod templates", func() {
		ctx := context.Background()

		var conn *v1alpha1.SparkConnect

		BeforeEach(func() {
			image := "docker.io/apache/spark:4.0.4"
			conn = &v1alpha1.SparkConnect{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "spark-connect-precedence-",
					Namespace:    "default",
				},
				Spec: v1alpha1.SparkConnectSpec{
					Image:        &image,
					SparkVersion: "4.0.4",
					Server: v1alpha1.ServerSpec{
						SparkPodSpec: v1alpha1.SparkPodSpec{
							CoreRequest: ptr.To(resource.MustParse("500m")),
							CoreLimit:   ptr.To(resource.MustParse("1")),
							Template: &corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									ServiceAccountName: "spark-operator-spark",
									Containers: []corev1.Container{
										{
											Name:  common.SparkDriverContainerName,
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
						SparkPodSpec: v1alpha1.SparkPodSpec{
							CoreRequest: ptr.To(resource.MustParse("500m")),
							CoreLimit:   ptr.To(resource.MustParse("1500m")),
							Template: &corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name:  common.Spark3DefaultExecutorContainerName,
											Image: image,
											Resources: corev1.ResourceRequirements{
												Requests: corev1.ResourceList{
													corev1.ResourceCPU: resource.MustParse("1"),
												},
												Limits: corev1.ResourceList{
													corev1.ResourceCPU: resource.MustParse("2"),
												},
											},
										},
									},
								},
							},
						},
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

			By("Waiting for the operator to create the server pod")
			serverPod := waitForServerPod(ctx, conn)
			serverContainer := containerByName(serverPod, common.SparkDriverContainerName)

			By("Asserting spec.server.coreRequest wins for the CPU request")
			cpuReq, ok := serverContainer.Resources.Requests[corev1.ResourceCPU]
			Expect(ok).To(BeTrue())
			Expect(cpuReq.Equal(resource.MustParse("500m"))).To(BeTrue(),
				"spec.server.coreRequest (500m) should win over template CPU request (1), got %s", cpuReq.String())

			By("Asserting spec.server.coreLimit wins for the CPU limit")
			cpuLim, ok := serverContainer.Resources.Limits[corev1.ResourceCPU]
			Expect(ok).To(BeTrue())
			Expect(cpuLim.Equal(resource.MustParse("1"))).To(BeTrue(),
				"spec.server.coreLimit (1) should win over template CPU limit (2), got %s", cpuLim.String())

			By("Asserting the template's memory request is preserved")
			memReq, ok := serverContainer.Resources.Requests[corev1.ResourceMemory]
			Expect(ok).To(BeTrue(), "template memory request should be preserved")
			Expect(memReq.Equal(resource.MustParse("1Gi"))).To(BeTrue(),
				"expected template memory 1Gi to be preserved, got %s", memReq.String())

			By("Asserting the template's memory limit is preserved")
			memLim, ok := serverContainer.Resources.Limits[corev1.ResourceMemory]
			Expect(ok).To(BeTrue(), "template memory limit should be preserved")
			Expect(memLim.Equal(resource.MustParse("1Gi"))).To(BeTrue(),
				"expected template memory 1Gi to be preserved, got %s", memLim.String())
		})

		It("overrides template CPU request/limit on the executor pod", func() {
			By("Creating the SparkConnect")
			Expect(k8sClient.Create(ctx, conn)).To(Succeed())

			By("Waiting for Spark to create the executor pod")
			executorPod := waitForExecutorPod(ctx, conn)
			executorContainer := containerByName(executorPod, common.Spark3DefaultExecutorContainerName)

			By("Asserting spec.executor.coreRequest wins for the CPU request")
			cpuReq, ok := executorContainer.Resources.Requests[corev1.ResourceCPU]
			Expect(ok).To(BeTrue(), "executor pod should have a CPU request set")
			Expect(cpuReq.Equal(resource.MustParse("500m"))).To(BeTrue(),
				"spec.executor.coreRequest (500m) should win over template CPU request (1), got %s", cpuReq.String())

			By("Asserting spec.executor.coreLimit wins for the CPU limit")
			cpuLim, ok := executorContainer.Resources.Limits[corev1.ResourceCPU]
			Expect(ok).To(BeTrue(), "executor pod should have a CPU limit set")
			Expect(cpuLim.Equal(resource.MustParse("1500m"))).To(BeTrue(),
				"spec.executor.coreLimit (1500m) should win over template CPU limit (2), got %s", cpuLim.String())
		})

		It("creates a server pod that eventually becomes ready", func() {
			By("Creating the SparkConnect")
			Expect(k8sClient.Create(ctx, conn)).To(Succeed())

			By("Waiting for the operator to create the server pod")
			serverPod := waitForServerPod(ctx, conn)

			By("Waiting for the server pod to become ready")
			Eventually(func() bool {
				key := types.NamespacedName{Namespace: conn.Namespace, Name: serverPod.Name}
				if err := k8sClient.Get(ctx, key, serverPod); err != nil {
					return false
				}
				return util.IsPodReady(serverPod)
			}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue(),
				"operator-created server pod should become ready within %s", WaitTimeout)
		})
	})
})

func waitForServerPod(ctx context.Context, conn *v1alpha1.SparkConnect) *corev1.Pod {
	GinkgoHelper()

	key := types.NamespacedName{
		Namespace: conn.Namespace,
		Name:      sparkconnect.GetServerPodName(conn),
	}
	serverPod := &corev1.Pod{}
	Eventually(func() error {
		return k8sClient.Get(ctx, key, serverPod)
	}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(Succeed())

	Expect(serverPod.Spec.Containers).NotTo(BeEmpty(), "server pod should have at least one container")
	return serverPod
}

func waitForExecutorPod(ctx context.Context, conn *v1alpha1.SparkConnect) *corev1.Pod {
	GinkgoHelper()

	instances := int(ptr.Deref(conn.Spec.Executor.Instances, 0))
	var executorPods *corev1.PodList
	Eventually(func() bool {
		executorPods = &corev1.PodList{}
		if err := k8sClient.List(
			ctx,
			executorPods,
			client.InNamespace(conn.Namespace),
			client.MatchingLabels(sparkconnect.GetExecutorSelectorLabels(conn)),
		); err != nil {
			return false
		}

		if len(executorPods.Items) != instances {
			return false
		}

		for i := range executorPods.Items {
			if !util.IsPodReady(&executorPods.Items[i]) {
				return false
			}
		}
		return true
	}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue(),
		"expected %d ready executor pod(s) for %s within %s", instances, conn.Name, WaitTimeout)

	Expect(executorPods.Items).NotTo(BeEmpty(), "expected at least one executor pod")
	return &executorPods.Items[0]
}

func containerByName(pod *corev1.Pod, name string) *corev1.Container {
	GinkgoHelper()

	container := util.GetContainerByNameOrFirst(pod.Spec.Containers, name)
	Expect(container).NotTo(BeNil(), "pod %s should have at least one container", pod.Name)
	Expect(container.Name).To(Equal(name),
		"pod %s should have a container named %s, got %s", pod.Name, name, container.Name)
	return container
}
