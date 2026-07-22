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
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

var _ = Describe("Spark UI", func() {
	Context("Verify Spark UI is accessible while application is running", func() {
		ctx := context.Background()
		path := filepath.Join("..", "..", "examples", "spark-pi.yaml")

		var app *v1beta2.SparkApplication

		BeforeEach(func() {
			By("Parsing SparkApplication from file")
			file, err := os.Open(path)
			Expect(err).NotTo(HaveOccurred())
			Expect(file).NotTo(BeNil())
			defer func() { Expect(file.Close()).To(Succeed()) }()
			decoder := yaml.NewYAMLOrJSONDecoder(file, 4096)
			Expect(decoder).NotTo(BeNil())

			app = &v1beta2.SparkApplication{}
			Expect(decoder.Decode(app)).NotTo(HaveOccurred())

			// Use a unique name and a partition count high enough that SparkPi keeps
			// running for the few seconds it takes the test to query the UI.
			app.Name = fmt.Sprintf("spark-pi-ui-test-%d", GinkgoRandomSeed())
			app.Spec.Arguments = []string{"50000"}

			By("Creating SparkApplication")
			Expect(k8sClient.Create(ctx, app)).To(Succeed())
		})

		AfterEach(func() {
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			if err := k8sClient.Get(ctx, key, app); err == nil {
				By("Deleting SparkApplication")
				Expect(k8sClient.Delete(ctx, app)).To(Succeed())
			}
		})

		It("Should create a UI service and serve the Spark web UI", func() {
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			driverPodName := util.GetDriverPodName(app)

			By("Waiting for SparkApplication to reach Running state with UI service populated")
			Eventually(func() bool {
				if err := k8sClient.Get(ctx, key, app); err != nil {
					return false
				}
				return app.Status.AppState.State == v1beta2.ApplicationStateRunning &&
					app.Status.DriverInfo.WebUIServiceName != ""
			}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())

			By("Verifying the WebUI status fields are populated")
			Expect(app.Status.DriverInfo.WebUIPort).To(Equal(int32(4040)))
			Expect(app.Status.DriverInfo.WebUIAddress).NotTo(BeEmpty())

			By("Verifying the UI service exists with port 4040")
			uiServiceName := app.Status.DriverInfo.WebUIServiceName
			svcKey := types.NamespacedName{Namespace: app.Namespace, Name: uiServiceName}
			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, svcKey, svc)).To(Succeed())
			hasUIPort := false
			for _, port := range svc.Spec.Ports {
				if port.Port == 4040 {
					hasUIPort = true
					break
				}
			}
			Expect(hasUIPort).To(BeTrue(), "UI service should expose port 4040")

			By("Verifying the Spark UI is responding on port 4040")
			Eventually(func(g Gomega) {
				transport, upgrader, err := spdy.RoundTripperFor(cfg)
				g.Expect(err).NotTo(HaveOccurred())

				reqURL := clientset.CoreV1().RESTClient().Post().
					Resource("pods").
					Namespace(app.Namespace).
					Name(driverPodName).
					SubResource("portforward").
					URL()

				dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, reqURL)

				stopChan := make(chan struct{})
				defer close(stopChan)
				readyChan := make(chan struct{})

				fw, err := portforward.New(dialer, []string{"0:4040"}, stopChan, readyChan, io.Discard, io.Discard)
				g.Expect(err).NotTo(HaveOccurred())

				errChan := make(chan error, 1)
				go func() { errChan <- fw.ForwardPorts() }()

				select {
				case <-readyChan:
				case err := <-errChan:
					g.Expect(err).NotTo(HaveOccurred(), "port forward failed to start")
					return
				case <-time.After(15 * time.Second):
					g.Expect(errors.New("timed out waiting for port-forward to become ready")).NotTo(HaveOccurred())
					return
				}

				ports, err := fw.GetPorts()
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(ports).NotTo(BeEmpty())

				httpClient := &http.Client{Timeout: 15 * time.Second}
				resp, err := httpClient.Get(fmt.Sprintf("http://localhost:%d", ports[0].Local))
				g.Expect(err).NotTo(HaveOccurred())
				defer func() { _ = resp.Body.Close() }()

				g.Expect(resp.StatusCode).To(Equal(http.StatusOK))

				body, err := io.ReadAll(resp.Body)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(string(body)).To(ContainSubstring("Spark Jobs"))
			}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(Succeed())
		})
	})
})
