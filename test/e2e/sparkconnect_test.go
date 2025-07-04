package e2e_test

import (
	"context"
	"os"
	"path/filepath"

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

var _ = Describe("SparkConnect Controller", func() {
	Context("Reconcile a new SparkConnect", func() {
		ctx := context.Background()
		path := filepath.Join("..", "..", "examples", "sparkconnect", "spark-connect.yaml")

		var conn *v1alpha1.SparkConnect

		BeforeEach(func() {
			By("Parsing SparkConnect from file", func() {
				file, err := os.Open(path)
				Expect(err).NotTo(HaveOccurred())
				Expect(file).NotTo(BeNil())

				decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
				Expect(decoder).NotTo(BeNil())

				conn = &v1alpha1.SparkConnect{}
				Expect(decoder.Decode(conn)).NotTo(HaveOccurred())
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
			})

			By("Waiting for executor pods to be ready", func() {
				Eventually(func() bool {
					executors := &corev1.PodList{}
					Expect(k8sClient.List(
						ctx,
						executors,
						client.InNamespace(conn.Namespace),
						client.MatchingLabels(sparkconnect.GetExecutorSelectorLabels(conn))),
					).NotTo(HaveOccurred())

					instances := int(ptr.Deref(conn.Spec.Executor.Instances, 0))
					if len(executors.Items) != instances {
						return false
					}

					for _, executor := range executors.Items {
						if !util.IsPodReady(&executor) {
							return false
						}
					}
					return true
				}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())
			})

			By("Waiting for server service to be created", func() {
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
			})
		})
	})
})
