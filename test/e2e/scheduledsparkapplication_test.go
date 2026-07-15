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
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/robfig/cron/v3"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

var _ = Describe("Example ScheduledSparkApplication", func() {
	Context("spark-pi-scheduled", func() {
		ctx := context.Background()
		path := filepath.Join("..", "..", "examples", "spark-pi-scheduled.yaml")
		var scheduledApp *v1beta2.ScheduledSparkApplication

		BeforeEach(func() {
			By("Parsing ScheduledSparkApplication from file")
			scheduledApp = &v1beta2.ScheduledSparkApplication{}
			file, err := os.Open(path)
			Expect(err).NotTo(HaveOccurred())
			Expect(file).NotTo(BeNil())
			defer func() { Expect(file.Close()).To(Succeed()) }()

			decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
			Expect(decoder).NotTo(BeNil())
			Expect(decoder.Decode(scheduledApp)).NotTo(HaveOccurred())

			// Use GenerateName so each spec gets a unique CR name,
			// preventing collisions if a prior AfterEach fails to delete.
			scheduledApp.GenerateName = scheduledApp.Name + "-"
			scheduledApp.Name = ""

			// Override schedule to fire quickly for testing.
			scheduledApp.Spec.Schedule = "@every 1m"

			By("Creating ScheduledSparkApplication")
			Expect(k8sClient.Create(ctx, scheduledApp)).To(Succeed())
		})

		AfterEach(func() {
			key := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: scheduledApp.Name}
			if err := k8sClient.Get(ctx, key, scheduledApp); err == nil {
				By("Deleting ScheduledSparkApplication")
				Expect(k8sClient.Delete(ctx, scheduledApp)).To(Succeed())
			}

			By("Verifying child SparkApplications are cascade-deleted via owner references")
			Eventually(func(g Gomega) {
				appList := &v1beta2.SparkApplicationList{}
				g.Expect(k8sClient.List(ctx, appList,
					client.InNamespace(scheduledApp.Namespace),
					client.MatchingLabels{common.LabelScheduledSparkAppName: scheduledApp.Name},
				)).To(Succeed())
				g.Expect(appList.Items).To(BeEmpty())
			}).WithTimeout(WaitTimeout).WithPolling(PollInterval).Should(Succeed())
		})

		It("Should reach Scheduled state and populate NextRun", func() {
			key := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: scheduledApp.Name}

			By("Waiting for ScheduledSparkApplication to reach Scheduled state")
			Eventually(func(g Gomega) {
				app := &v1beta2.ScheduledSparkApplication{}
				g.Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
				g.Expect(app.Status.ScheduleState).To(Equal(v1beta2.ScheduleStateScheduled))
				g.Expect(app.Status.NextRun.IsZero()).To(BeFalse())
			}).WithTimeout(WaitTimeout).WithPolling(PollInterval).Should(Succeed())
		})

		It("Should spawn a child SparkApplication that completes successfully", func() {
			key := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: scheduledApp.Name}

			By("Waiting for a child SparkApplication to be created")
			var childName string
			Eventually(func(g Gomega) {
				app := &v1beta2.ScheduledSparkApplication{}
				g.Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
				g.Expect(app.Status.LastRunName).NotTo(BeEmpty())
				childName = app.Status.LastRunName
			}).WithTimeout(WaitTimeout).WithPolling(PollInterval).Should(Succeed())

			By("Verifying the child SparkApplication resource exists")
			childKey := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: childName}
			childApp := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, childKey, childApp)).To(Succeed())

			By("Verifying the child has the scheduled app label")
			Expect(childApp.Labels).To(HaveKeyWithValue(common.LabelScheduledSparkAppName, scheduledApp.Name))

			By("Verifying the child has an owner reference to the scheduled app")
			Expect(childApp.OwnerReferences).NotTo(BeEmpty())
			found := false
			for _, ref := range childApp.OwnerReferences {
				if ref.Name == scheduledApp.Name && ref.Kind == "ScheduledSparkApplication" {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue())

			By("Waiting for the child SparkApplication to complete")
			Expect(waitForSparkApplicationCompleted(ctx, childKey)).NotTo(HaveOccurred())

			By("Verifying LastRun is set")
			app := &v1beta2.ScheduledSparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
			Expect(app.Status.LastRun.IsZero()).To(BeFalse())

			By("Checking driver logs of the child SparkApplication")
			driverPodName := util.GetDriverPodName(childApp)
			bytes, err := clientset.CoreV1().Pods(scheduledApp.Namespace).GetLogs(driverPodName, &corev1.PodLogOptions{}).Do(ctx).Raw()
			Expect(err).NotTo(HaveOccurred())
			Expect(bytes).NotTo(BeEmpty())
			Expect(strings.Contains(string(bytes), "Pi is roughly 3")).To(BeTrue())

			By("Waiting for a second scheduled run to fire at the expected interval")
			schedule, err := cron.ParseStandard(scheduledApp.Spec.Schedule)
			Expect(err).NotTo(HaveOccurred())
			expectedInterval := schedule.Next(childApp.CreationTimestamp.Time).Sub(childApp.CreationTimestamp.Time)

			var secondChildName string
			Eventually(func(g Gomega) {
				app := &v1beta2.ScheduledSparkApplication{}
				g.Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
				g.Expect(app.Status.LastRunName).NotTo(Equal(childName))
				secondChildName = app.Status.LastRunName
			}).WithTimeout(WaitTimeout).WithPolling(PollInterval).Should(Succeed())

			By("Verifying the second run starts at correct interval after the first run")
			secondChildKey := types.NamespacedName{Namespace: scheduledApp.Namespace, Name: secondChildName}
			secondChildApp := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, secondChildKey, secondChildApp)).To(Succeed())
			actualInterval := secondChildApp.CreationTimestamp.Sub(childApp.CreationTimestamp.Time)
			Expect(actualInterval).To(BeNumerically(">=", expectedInterval-2*time.Second),
				"second run started before the schedule interval elapsed")

			By("Waiting for the second child SparkApplication to complete")
			Expect(waitForSparkApplicationCompleted(ctx, secondChildKey)).NotTo(HaveOccurred())
		})
	})

})
