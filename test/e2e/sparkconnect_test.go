package e2e_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/internal/controller/sparkconnect"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

func loadSparkConnectFromFile(path string) *v1alpha1.SparkConnect {
	file, err := os.Open(path)
	Expect(err).NotTo(HaveOccurred())
	Expect(file).NotTo(BeNil())
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(file)

	decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
	Expect(decoder).NotTo(BeNil())

	conn := &v1alpha1.SparkConnect{}
	Expect(decoder.Decode(conn)).NotTo(HaveOccurred())
	return conn
}

func waitForServerPodReady(ctx context.Context, conn *v1alpha1.SparkConnect) {
	Eventually(func() bool {
		key := types.NamespacedName{
			Namespace: conn.Namespace,
			Name:      sparkconnect.GetServerPodName(conn),
		}
		server := &corev1.Pod{}
		if err := k8sClient.Get(ctx, key, server); err != nil {
			return false
		}
		return util.IsPodReady(server)
	}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())
}

func waitForServerServiceCreated(ctx context.Context, conn *v1alpha1.SparkConnect) {
	Eventually(func() bool {
		key := types.NamespacedName{
			Namespace: conn.Namespace,
			Name:      sparkconnect.GetServerServiceName(conn),
		}
		service := &corev1.Service{}
		if err := k8sClient.Get(ctx, key, service); err != nil {
			return false
		}
		return true
	}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())
}

func waitForExecutorPodsReady(ctx context.Context, conn *v1alpha1.SparkConnect, expectedCount int) {
	Eventually(func() bool {
		executors := &corev1.PodList{}
		Expect(k8sClient.List(
			ctx,
			executors,
			client.InNamespace(conn.Namespace),
			client.MatchingLabels(sparkconnect.GetExecutorSelectorLabels(conn))),
		).NotTo(HaveOccurred())

		if len(executors.Items) != expectedCount {
			return false
		}

		for _, executor := range executors.Items {
			if !util.IsPodReady(&executor) {
				return false
			}
		}
		return true
	}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())
}

func getRunningExecutorCountFromStatus(ctx context.Context, conn *v1alpha1.SparkConnect) int {
	key := types.NamespacedName{
		Namespace: conn.Namespace,
		Name:      conn.Name,
	}
	Expect(k8sClient.Get(ctx, key, conn)).To(Succeed())

	runningCount := 0
	if count, ok := conn.Status.Executors["running"]; ok {
		runningCount = count
	}
	return runningCount
}

var _ = Describe("SparkConnect Controller", func() {
	Context("Reconcile a new SparkConnect", func() {
		ctx := context.Background()
		path := filepath.Join("..", "..", "examples", "sparkconnect", "spark-connect.yaml")

		var conn *v1alpha1.SparkConnect

		BeforeEach(func() {
			By("Parsing SparkConnect from file", func() {
				conn = loadSparkConnectFromFile(path)
			})

			By("Creating SparkConnect", func() {
				Expect(k8sClient.Create(ctx, conn)).To(Succeed())
			})
		})

		AfterEach(func() {
			By("Deleting SparkConnect", func() {
				Expect(k8sClient.Delete(ctx, conn)).To(Succeed())
				conn = nil
			})
		})

		It("Should reconcile SparkConnect correctly", func() {
			By("Waiting for server pod to be ready", func() {
				waitForServerPodReady(ctx, conn)
			})

			By("Waiting for executor pods to be ready", func() {
				instances := int(ptr.Deref(conn.Spec.Executor.Instances, 0))
				waitForExecutorPodsReady(ctx, conn, instances)
			})

			By("Waiting for server service to be created", func() {
				waitForServerServiceCreated(ctx, conn)
			})
		})
	})

	Context("Test dynamic allocation of min executors", func() {
		ctx := context.Background()
		path := filepath.Join("..", "..", "examples", "sparkconnect", "spark-connect.yaml")

		var conn *v1alpha1.SparkConnect

		BeforeEach(func() {
			By("Parsing SparkConnect from file", func() {
				conn = loadSparkConnectFromFile(path)

				conn.Spec.DynamicAllocation = &v1alpha1.DynamicAllocation{
					Enabled:          true,
					InitialExecutors: ptr.To(int32(1)),
					MinExecutors:     ptr.To(int32(2)),
				}
			})

			By("Creating SparkConnect with dynamic allocation", func() {
				Expect(k8sClient.Create(ctx, conn)).To(Succeed())
			})
		})

		AfterEach(func() {
			By("Deleting SparkConnect", func() {
				Expect(k8sClient.Delete(ctx, conn)).To(Succeed())
				conn = nil
			})
		})

		It("Should scale executors to 2", func() {
			By("Waiting for initial executor pod to be ready", func() {
				waitForExecutorPodsReady(ctx, conn, 2)
			})

			By("Verifying status reflects correct executor count", func() {
				Eventually(func() int {
					return getRunningExecutorCountFromStatus(ctx, conn)
				}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(Equal(2))
			})
		})
	})
	Context("Test dynamic allocation with max executors limit", func() {
		ctx := context.Background()
		path := filepath.Join("..", "..", "examples", "sparkconnect", "spark-connect.yaml")

		var conn *v1alpha1.SparkConnect

		BeforeEach(func() {
			By("Parsing SparkConnect from file", func() {
				conn = loadSparkConnectFromFile(path)

				// initial executors cannot exceed max executors
				conn.Spec.DynamicAllocation = &v1alpha1.DynamicAllocation{
					Enabled:          true,
					InitialExecutors: ptr.To(int32(5)),
					MaxExecutors:     ptr.To(int32(3)),
				}
			})

			By("Creating SparkConnect with max executors limit", func() {
				Expect(k8sClient.Create(ctx, conn)).To(Succeed())
			})
		})

		AfterEach(func() {
			By("Deleting SparkConnect", func() {
				Expect(k8sClient.Delete(ctx, conn)).To(Succeed())
				conn = nil
			})
		})
		It("Should fail with containers in CrashLoopBackOff due to misconfigured max executors", func() {
			By("Verifying server pod exists but containers are not ready", func() {
				Eventually(func() bool {
					key := types.NamespacedName{
						Namespace: conn.Namespace,
						Name:      sparkconnect.GetServerPodName(conn),
					}
					server := &corev1.Pod{}
					if err := k8sClient.Get(ctx, key, server); err != nil {
						return false
					}
					return server != nil && !util.IsPodReady(server)
				}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())
			})
			By("Verifying pod status shows container in crash loop", func() {
				Eventually(func() bool {
					key := types.NamespacedName{
						Namespace: conn.Namespace,
						Name:      sparkconnect.GetServerPodName(conn),
					}
					server := &corev1.Pod{}
					if err := k8sClient.Get(ctx, key, server); err != nil {
						return false
					}

					for _, containerStatus := range server.Status.ContainerStatuses {
						if containerStatus.State.Waiting != nil &&
							containerStatus.State.Waiting.Reason == "CrashLoopBackOff" {
							return true
						}
					}
					return false
				}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())
			})

			By("Verifying driver container status in SparkConnect resource", func() {
				Eventually(func() bool {
					key := types.NamespacedName{
						Namespace: conn.Namespace,
						Name:      conn.Name,
					}
					updatedConn := &v1alpha1.SparkConnect{}
					if err := k8sClient.Get(ctx, key, updatedConn); err != nil {
						return false
					}

					return strings.Contains(updatedConn.Status.Server.DriverContainerStatus, "CrashLoopBackOff") || strings.Contains(updatedConn.Status.Server.DriverContainerStatus, "Terminated")
				}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())
			})
		})
	})
})
