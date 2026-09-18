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
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/internal/controller/sparkconnect"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

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

		It("Should pass the declared dependencies to the Spark Connect server", func() {
			serverPodName := sparkconnect.GetServerPodName(conn)

			By("Waiting for the server pod to exist")
			var serverArgs []string
			Eventually(func() bool {
				pod := &corev1.Pod{}
				key := types.NamespacedName{Namespace: conn.Namespace, Name: serverPodName}
				if err := k8sClient.Get(ctx, key, pod); err != nil {
					return false
				}
				for _, container := range pod.Spec.Containers {
					if container.Name == common.SparkDriverContainerName && len(container.Args) > 0 {
						serverArgs = container.Args
						return true
					}
				}
				return false
			}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())

			By("Checking that the dependency flags are passed as individual container arguments")
			Expect(serverArgs).To(ContainElement("--jars"))
			Expect(serverArgs).To(ContainElement(ContainSubstring("commons-lang3-3.17.0.jar")))
			Expect(serverArgs).To(ContainElement("--packages"))
			Expect(serverArgs).To(ContainElement(ContainSubstring("org.apache.commons:commons-text:1.12.0")))
			Expect(serverArgs).To(ContainElement("--exclude-packages"))
			Expect(serverArgs).To(ContainElement(ContainSubstring("org.apache.commons:commons-lang3")))
			Expect(serverArgs).To(ContainElement("--repositories"))
			Expect(serverArgs).To(ContainElement(ContainSubstring("https://repo1.maven.org/maven2")))
		})
	})
})
