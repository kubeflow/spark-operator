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
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

// sumMetricValues parses Prometheus text-exposition output and returns the
// sum of all sample values for the given metric name, across all label combinations.
func sumMetricValues(metricsText, name string) float64 {
	pattern := regexp.MustCompile(`^` + regexp.QuoteMeta(name) + `(\{[^}]*\})?\s+(\S+)$`)

	var total float64
	scanner := bufio.NewScanner(strings.NewReader(metricsText))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		matches := pattern.FindStringSubmatch(line)
		if matches == nil {
			continue
		}
		if value, err := strconv.ParseFloat(matches[2], 64); err == nil {
			total += value
		}
	}
	return total
}

var _ = Describe("Prometheus Metrics", func() {
	Context("Controller metrics endpoint", func() {
		ctx := context.Background()
		path := filepath.Join("..", "..", "examples", "spark-pi.yaml")

		var app *v1beta2.SparkApplication

		BeforeEach(func() {
			By("Parsing SparkApplication from file")
			file, err := os.Open(path)
			Expect(err).NotTo(HaveOccurred())
			Expect(file).NotTo(BeNil())
			defer func() { Expect(file.Close()).To(Succeed()) }()

			decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
			Expect(decoder).NotTo(BeNil())

			app = &v1beta2.SparkApplication{}
			Expect(decoder.Decode(app)).NotTo(HaveOccurred())
			app.Name = fmt.Sprintf("spark-pi-metrics-test-%d", GinkgoRandomSeed())
		})

		AfterEach(func() {
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			if err := k8sClient.Get(ctx, key, app); err == nil {
				By("Deleting SparkApplication")
				Expect(k8sClient.Delete(ctx, app)).To(Succeed())
			}
		})

		It("Should serve Prometheus metrics including Spark application metrics", func() {
			By("Finding the controller pod")
			pods := &corev1.PodList{}
			Expect(k8sClient.List(ctx, pods,
				client.InNamespace(ReleaseNamespace),
				client.MatchingLabels{
					"app.kubernetes.io/name":      "spark-operator",
					"app.kubernetes.io/component": "controller",
				},
			)).To(Succeed())
			Expect(pods.Items).NotTo(BeEmpty(), "controller pod not found")

			By("Identifying the leader controller pod via Lease")
			controllerPod := pods.Items[0]
			lease := &coordinationv1.Lease{}
			if err := k8sClient.Get(ctx, types.NamespacedName{
				Namespace: ReleaseNamespace,
				Name:      "spark-operator-controller-lock",
			}, lease); err == nil && lease.Spec.HolderIdentity != nil {
				// controller-runtime sets the lease holder identity to "<pod-name>_<uuid>".
				matched := false
				for _, pod := range pods.Items {
					if strings.HasPrefix(*lease.Spec.HolderIdentity, pod.Name+"_") {
						controllerPod = pod
						matched = true
						break
					}
				}
				Expect(matched).To(BeTrue(), "no controller pod matches lease holder identity %q", *lease.Spec.HolderIdentity)
			}

			By("Detecting metrics scheme and port from controller deployment")
			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: ReleaseNamespace,
				Name:      "spark-operator-controller",
			}, deploy)).To(Succeed())

			metricsScheme := "http"
			metricsPort := "8080"
			for _, arg := range deploy.Spec.Template.Spec.Containers[0].Args {
				if arg == "--secure-metrics" || arg == "--secure-metrics=true" {
					metricsScheme = "https"
				}
				if strings.HasPrefix(arg, "--metrics-bind-address=") {
					addr := strings.TrimPrefix(arg, "--metrics-bind-address=")
					if _, port, err := net.SplitHostPort(addr); err == nil && port != "" {
						metricsPort = port
					}
				}
			}

			By("Verifying the metrics endpoint serves Prometheus-formatted data")
			data, err := clientset.CoreV1().Pods(ReleaseNamespace).
				ProxyGet(metricsScheme, controllerPod.Name, metricsPort, "metrics", nil).
				DoRaw(ctx)
			Expect(err).NotTo(HaveOccurred(), "failed to proxy GET /metrics from controller pod")

			metricsOutput := string(data)
			Expect(metricsOutput).To(ContainSubstring("# HELP"))
			Expect(metricsOutput).To(ContainSubstring("# TYPE"))
			Expect(metricsOutput).To(ContainSubstring("go_goroutines"))
			Expect(metricsOutput).To(ContainSubstring("process_cpu_seconds_total"))

			// Other specs may run concurrently and share these process-global counters,
			// so capture a baseline and assert on the delta rather than mere presence.
			countBefore := sumMetricValues(metricsOutput, "spark_application_count")
			submitCountBefore := sumMetricValues(metricsOutput, "spark_application_submit_count")
			successCountBefore := sumMetricValues(metricsOutput, "spark_application_success_count")

			By("Creating SparkApplication to exercise the metrics pipeline")
			Expect(k8sClient.Create(ctx, app)).To(Succeed())

			By("Waiting for SparkApplication to complete")
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			Expect(waitForSparkApplicationCompleted(ctx, key)).NotTo(HaveOccurred())

			By("Verifying Spark application metrics incremented after app completion")
			Eventually(func() (bool, error) {
				data, err := clientset.CoreV1().Pods(ReleaseNamespace).
					ProxyGet(metricsScheme, controllerPod.Name, metricsPort, "metrics", nil).
					DoRaw(ctx)
				if err != nil {
					return false, err
				}
				metricsOutput := string(data)
				return sumMetricValues(metricsOutput, "spark_application_count") > countBefore &&
					sumMetricValues(metricsOutput, "spark_application_submit_count") > submitCountBefore &&
					sumMetricValues(metricsOutput, "spark_application_success_count") > successCountBefore, nil
			}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())
		})
	})
})
