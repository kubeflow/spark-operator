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
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

// sleepyPiScript computes a trivial result and then sleeps for a fixed,
// deterministic duration so the driver stays up long enough for the test to
// query the Spark UI, regardless of cluster/node speed.
const sleepyPiScript = `
import time
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkUIKeepAlive").getOrCreate()
print(spark.sparkContext.parallelize(range(2), 2).sum())
time.sleep(60)
spark.stop()
`

var _ = Describe("Spark UI", func() {
	Context("Verify Spark UI is accessible while application is running", func() {
		ctx := context.Background()

		var app *v1beta2.SparkApplication
		var scriptConfigMap *corev1.ConfigMap

		BeforeEach(func() {
			seed := GinkgoRandomSeed()
			app = loadSparkPiPython(fmt.Sprintf("spark-pi-ui-test-%d", seed))

			By("Creating a ConfigMap with a script that keeps the driver alive")
			scriptConfigMap = &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("spark-ui-test-script-%d", seed),
					Namespace: app.Namespace,
				},
				Data: map[string]string{
					"sleepy_pi.py": sleepyPiScript,
				},
			}
			Expect(k8sClient.Create(ctx, scriptConfigMap)).To(Succeed())

			app.Spec.MainApplicationFile = ptr.To("local:///opt/spark/scripts/sleepy_pi.py")
			app.Spec.Volumes = append(app.Spec.Volumes, corev1.Volume{
				Name: "ui-test-script",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: scriptConfigMap.Name},
					},
				},
			})
			app.Spec.Driver.VolumeMounts = append(app.Spec.Driver.VolumeMounts, corev1.VolumeMount{
				Name:      "ui-test-script",
				MountPath: "/opt/spark/scripts",
			})

			By("Creating SparkApplication")
			Expect(k8sClient.Create(ctx, app)).To(Succeed())
		})

		AfterEach(func() {
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			if err := k8sClient.Get(ctx, key, app); err == nil {
				By("Deleting SparkApplication")
				Expect(k8sClient.Delete(ctx, app)).To(Succeed())
			}
			By("Deleting the script ConfigMap")
			_ = k8sClient.Delete(ctx, scriptConfigMap)
		})

		It("Should create a UI service and serve the Spark web UI", func() {
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}

			By("Waiting for SparkApplication to reach Running state with UI service populated")
			Eventually(func() bool {
				if err := k8sClient.Get(ctx, key, app); err != nil {
					return false
				}
				return app.Status.AppState.State == v1beta2.ApplicationStateRunning &&
					app.Status.DriverInfo.WebUIServiceName != ""
			}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(BeTrue())

			By("Verifying the WebUI status fields are populated")
			Expect(app.Status.DriverInfo.WebUIPort).To(Equal(common.DefaultSparkWebUIPort))
			_, port, err := net.SplitHostPort(app.Status.DriverInfo.WebUIAddress)
			Expect(err).NotTo(HaveOccurred(), "WebUIAddress should be a valid host:port, got %q", app.Status.DriverInfo.WebUIAddress)
			Expect(port).To(Equal(strconv.Itoa(int(app.Status.DriverInfo.WebUIPort))))

			By(fmt.Sprintf("Verifying the UI service exists with port %d", common.DefaultSparkWebUIPort))
			uiServiceName := app.Status.DriverInfo.WebUIServiceName
			svcKey := types.NamespacedName{Namespace: app.Namespace, Name: uiServiceName}
			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, svcKey, svc)).To(Succeed())
			hasUIPort := false
			for _, port := range svc.Spec.Ports {
				if port.Port == common.DefaultSparkWebUIPort {
					hasUIPort = true
					break
				}
			}
			Expect(hasUIPort).To(BeTrue(), "UI service should expose port %d", common.DefaultSparkWebUIPort)

			By("Verifying the Spark UI REST API reports the running application via the Service proxy")
			Eventually(func(g Gomega) {
				reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
				defer cancel()

				data, err := clientset.CoreV1().Services(app.Namespace).
					ProxyGet("http", uiServiceName, common.DefaultSparkWebUIPortName, "api/v1/applications", nil).
					DoRaw(reqCtx)
				g.Expect(err).NotTo(HaveOccurred(), "failed to proxy GET /api/v1/applications from UI service")

				var runningApps []map[string]any
				g.Expect(json.Unmarshal(data, &runningApps)).To(Succeed(), "expected valid JSON from Spark UI REST API, got: %s", data)
				g.Expect(runningApps).NotTo(BeEmpty(), "expected the Spark UI REST API to report at least one application")
			}).WithPolling(PollInterval).WithTimeout(WaitTimeout).Should(Succeed())
		})
	})
})
