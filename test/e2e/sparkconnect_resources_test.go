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
	// Test the new CoreRequest/CoreLimit fields at the API/runtime level.
	// These tests create a SparkConnect in-memory (rather than loading the
	// example yaml) so that they can assert the specific values they wrote
	// are actually applied to the operator-created server pod and surfaced
	// in the spark-submit args for executor pods.
	//
	// The "Apply server CoreRequest/CoreLimit" Context only waits for the
	// server pod to be created — its assertions only depend on fields the
	// operator writes at pod-creation time, and waiting on readiness would
	// add JVM startup per spec for no extra coverage. The "Precedence"
	// Context adds separate Its that do exercise readiness end-to-end and
	// inspect a real executor pod. The executor assertions belong there
	// because Spark, not the operator, creates executor pods, and those are
	// created by the server pod's service account: the namespace's default
	// service account has no RBAC to create them, so that Context sets
	// spark-operator-spark explicitly.
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
			// These stay alongside the executor-pod assertions in the Precedence Context
			// below: they guard what the operator emits, which is a different failure from
			// Spark not honouring it.
			// The server pod's args string is built from buildStartConnectServerArgs and includes
			// --conf spark.kubernetes.executor.request.cores=... and --conf spark.kubernetes.executor.limit.cores=...
			// generated from executor.coreRequest / executor.coreLimit.
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
							// Template also specifies CPU and memory.
							Template: &corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									// The readiness It waits for the Spark Connect server to start, which
									// requires the pod to launch executor pods. The default service account
									// in the "default" namespace has no RBAC for that, so we explicitly use
									// the spark-operator-spark SA installed by config/spark-rbac/ in the e2e
									// suite. The example YAML does the same.
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
							// The template also specifies CPU, which Spark ignores for the request
							// and only falls back to for the limit once executor.coreLimit is unset.
							// The values deliberately differ from the CRD fields above so the
							// assertions below can tell the two sources apart. Memory is left out
							// on purpose: Spark sets the executor container's memory from
							// spark.executor.memory plus its overhead, so a template memory value
							// only risks the merged pod being rejected for request > limit.
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
			// Spark always sets the executor CPU request from
			// spark.kubernetes.executor.request.cores, so the template's request never
			// applies — the container must end up with the CRD value, not the template's.
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

// waitForServerPod waits until the operator has created the SparkConnect server pod
// and returns it. It deliberately does not wait for the pod to become ready: the
// caller asserts on fields the operator writes when it first creates the pod.
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

// waitForExecutorPod waits until Spark has created the requested number of executor
// pods for the SparkConnect and they are all ready, then returns the first one.
// Executor pods are created by Spark rather than the operator, so they can only be
// found by label — there is no computed pod name to look up.
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

// containerByName returns the container with the given name. It fails the test
// rather than falling back to the first container when the name does not match, so
// that a renamed or reordered container is caught instead of silently asserting
// against an unrelated one.
func containerByName(pod *corev1.Pod, name string) *corev1.Container {
	GinkgoHelper()

	container := util.GetContainerByNameOrFirst(pod.Spec.Containers, name)
	Expect(container).NotTo(BeNil(), "pod %s should have at least one container", pod.Name)
	Expect(container.Name).To(Equal(name),
		"pod %s should have a container named %s, got %s", pod.Name, name, container.Name)
	return container
}
