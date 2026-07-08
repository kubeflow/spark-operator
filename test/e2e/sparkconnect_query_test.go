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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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

var _ = Describe("SparkConnect Query", func() {
	Context("Execute a query via Spark Connect", func() {
		ctx := context.Background()
		path := filepath.Join("..", "..", "examples", "sparkconnect", "spark-connect.yaml")

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
			conn.Name = "spark-connect-query"

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

		It("Should execute a PySpark query through the Spark Connect endpoint", func() {
			serverPodName := sparkconnect.GetServerPodName(conn)
			serviceName := sparkconnect.GetServerServiceName(conn)

			By("Waiting for server pod to be ready")
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
				key := types.NamespacedName{Namespace: conn.Namespace, Name: serviceName}
				service := &corev1.Service{}
				if err := k8sClient.Get(ctx, key, service); err != nil {
					return false
				}
				return true
			}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())

			By("Verifying the service has the spark-connect-server port")
			svcKey := types.NamespacedName{Namespace: conn.Namespace, Name: serviceName}
			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, svcKey, svc)).To(Succeed())
			hasConnectPort := false
			for _, port := range svc.Spec.Ports {
				if port.Name == "spark-connect-server" && port.Port == 15002 {
					hasConnectPort = true
					break
				}
			}
			Expect(hasConnectPort).To(BeTrue(), "service should expose spark-connect-server port 15002")

			By("Creating PySpark client pod")
			clientPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "pyspark-client-",
					Namespace:    conn.Namespace,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    "pyspark-client",
							Image:   "quay.io/fedora/python-312:latest",
							Command: []string{"sleep", "infinity"},
						},
					},
					RestartPolicy: corev1.RestartPolicyNever,
				},
			}
			Expect(controllerutil.SetOwnerReference(conn, clientPod, k8sClient.Scheme())).To(Succeed())
			Expect(k8sClient.Create(ctx, clientPod)).To(Succeed())

			By("Waiting for PySpark client pod to be ready")
			Eventually(func() bool {
				key := types.NamespacedName{Namespace: conn.Namespace, Name: clientPod.Name}
				pod := &corev1.Pod{}
				if err := k8sClient.Get(ctx, key, pod); err != nil {
					return false
				}
				return util.IsPodReady(pod)
			}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())

			By("Installing PySpark Connect client")
			output, err := runCommand(
				"kubectl", "exec", "-n", conn.Namespace, serverPodName, "--",
				"bash", "-c", "export HOME=/tmp && pip install --quiet --disable-pip-version-check pandas pyarrow grpcio grpcio-status",
			)
			Expect(err).NotTo(HaveOccurred(), "pip install failed: %s", output)

			By("Executing a PySpark query via Spark Connect inside the server pod")
			connectURL := fmt.Sprintf("sc://%s.%s.svc.cluster.local:15002", serviceName, conn.Namespace)
			pysparkScript := fmt.Sprintf(
				`from pyspark.sql import SparkSession; `+
					`spark = SparkSession.builder.remote("%s").getOrCreate(); `+
					`df = spark.range(100).selectExpr("id", "id * 2 as doubled"); `+
					`print("ROW_COUNT=" + str(df.count())); `+
					`row = df.filter("id = 7").collect()[0]; `+
					`print("VALIDATE=" + str(row["doubled"])); `+
					`spark.stop()`,
				connectURL,
			)
			output, err = runCommand(
				"kubectl", "exec", "-n", conn.Namespace, serverPodName, "--",
				"bash", "-c",
				fmt.Sprintf(
					"export HOME=/tmp && PYTHONPATH=${SPARK_HOME}/python:$(ls ${SPARK_HOME}/python/lib/py4j-*.zip):${PYTHONPATH} python3 -c '%s'",
					pysparkScript,
				),
			)
			Expect(err).NotTo(HaveOccurred(), "PySpark query failed: %s", output)
			Expect(output).To(ContainSubstring("ROW_COUNT=100"),
				"expected query to return 100 rows, got: %s", output)
			Expect(output).To(ContainSubstring("VALIDATE=14"),
				"expected doubled value of 14 for id=7, got: %s", output)
		})
	})
})

func runCommand(name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), WaitTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return string(out), fmt.Errorf("command %s timed out after %s", name, WaitTimeout)
	}
	return string(out), err
}
