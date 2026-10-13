/*
Copyright The Kubeflow Authors.

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

package e2e_test

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/internal/controller/sparkconnect"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// connectQueryScript queries the Spark Connect server from the client pod.
//
//go:embed scripts/connect_query.py
var connectQueryScript string

var _ = Describe("SparkConnect Dependencies", func() {
	Context("Start the server with dependencies", func() {
		ctx := context.Background()
		path := filepath.Join("..", "..", "examples", "sparkconnect", "spark-connect-dependencies.yaml")

		var conn *v1alpha1.SparkConnect

		BeforeEach(func() {
			By("Parsing SparkConnect from file")
			file, err := os.Open(path)
			Expect(err).NotTo(HaveOccurred())
			Expect(file).NotTo(BeNil())
			defer func() { Expect(file.Close()).To(Succeed()) }()

			decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
			Expect(decoder).NotTo(BeNil())

			conn = &v1alpha1.SparkConnect{}
			Expect(decoder.Decode(conn)).NotTo(HaveOccurred())
			conn.Name = "spark-connect-dependencies"

			By("Creating SparkConnect")
			Expect(k8sClient.Create(ctx, conn)).To(Succeed())
		})

		AfterEach(func() {
			key := types.NamespacedName{Namespace: conn.Namespace, Name: conn.Name}
			if err := k8sClient.Get(ctx, key, conn); err == nil {
				By("Deleting SparkConnect")
				Expect(k8sClient.Delete(ctx, conn)).To(Succeed())
			}
		})

		It("Should exercise the declared dependencies in the running server pod", func() {
			serverPodName := sparkconnect.GetServerPodName(conn)
			serviceName := sparkconnect.GetServerServiceName(conn)

			By("Waiting for the server pod to be ready")
			Eventually(func() bool {
				key := types.NamespacedName{Namespace: conn.Namespace, Name: serverPodName}
				server := &corev1.Pod{}
				if err := k8sClient.Get(ctx, key, server); err != nil {
					return false
				}
				return util.IsPodReady(server)
			}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())

			By("Waiting for executor pods to be ready")
			Eventually(func() bool {
				executors := &corev1.PodList{}
				Expect(k8sClient.List(
					ctx,
					executors,
					client.InNamespace(conn.Namespace),
					client.MatchingLabels(sparkconnect.GetExecutorSelectorLabels(conn)),
				)).NotTo(HaveOccurred())

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

			By("Waiting for server service to be created")
			Eventually(func() bool {
				service := &corev1.Service{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: conn.Namespace, Name: serviceName}, service); err != nil {
					return false
				}
				return true
			}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())

			By("Reading the operator controller image")
			operatorDeploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: ReleaseNamespace,
				Name:      "spark-operator-controller",
			}, operatorDeploy)).To(Succeed())
			operatorImage := operatorDeploy.Spec.Template.Spec.Containers[0].Image

			By("Creating Spark Connect client pod")
			clientPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "spark-connect-deps-client-",
					Namespace:    conn.Namespace,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "spark-connect-client",
							Image:           operatorImage,
							ImagePullPolicy: corev1.PullNever,
							Command:         []string{"sleep", "infinity"},
						},
					},
					RestartPolicy: corev1.RestartPolicyNever,
				},
			}
			Expect(controllerutil.SetOwnerReference(conn, clientPod, k8sClient.Scheme())).To(Succeed())
			Expect(k8sClient.Create(ctx, clientPod)).To(Succeed())

			By("Waiting for Spark Connect client pod to be ready")
			Eventually(func() bool {
				pod := &corev1.Pod{}
				if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: conn.Namespace, Name: clientPod.Name}, pod); err != nil {
					return false
				}
				return util.IsPodReady(pod)
			}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())

			By("Running a query on the server session that uses the declared spark-avro package")
			// to_avro is not available in the base image. It only resolves if the
			// declared spark-avro package was fetched and put on the server session's
			// classpath, so a successful query exercises the dependency instead of only
			// inspecting the container arguments. The helper asserts the session is a
			// genuine Spark Connect session, because a classic fallback would evaluate
			// the query on the client JVM and never reach the operator's server at all.
			connectURL := fmt.Sprintf("sc://%s.%s.svc:15002", serviceName, conn.Namespace)
			output, err := runCommandWithInput(
				connectQueryScript,
				"kubectl", "exec", "-i", "-n", conn.Namespace, clientPod.Name, "--",
				"python3", "-", connectURL,
			)
			Expect(err).NotTo(HaveOccurred(), "Spark Connect dependency query failed: %s", output)
			Expect(output).To(ContainSubstring("CONNECT_ROWS=10"), "expected the spark-avro query to return 10 rows, got: %s", output)
		})
	})
})
