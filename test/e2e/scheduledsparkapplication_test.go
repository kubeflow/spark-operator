package e2e_test

import (
	"bufio"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

var _ = Describe("Example ScheduledSparkApplication", func() {
	Context("spark-pi-scheduled", func() {
		ctx := context.Background()
		path := os.Getenv("SCHEDULED_APP_YAML")
		if path == "" {
			path = filepath.Join("..", "..", "examples", "spark-pi-scheduled.yaml")
		}
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
			// avoiding AlreadyExists when CLEANUP=false skips deletion.
			scheduledApp.GenerateName = scheduledApp.Name + "-"
			scheduledApp.Name = ""

			// Override schedule to fire quickly for testing.
			scheduledApp.Spec.Schedule = "@every 1m"

			By("Creating ScheduledSparkApplication")
			Expect(k8sClient.Create(ctx, scheduledApp)).To(Succeed())
		})

		AfterEach(func() {
			if strings.EqualFold(os.Getenv("CLEANUP"), "false") && CurrentSpecReport().Failed() {
				return
			}

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
			}).WithTimeout(2 * time.Minute).WithPolling(PollInterval).Should(Succeed())

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
			logStream, err := clientset.CoreV1().Pods(scheduledApp.Namespace).GetLogs(driverPodName, &corev1.PodLogOptions{
				LimitBytes: ptr.To(int64(1 << 20)),
			}).Stream(ctx)
			Expect(err).NotTo(HaveOccurred())
			defer func() { Expect(logStream.Close()).To(Succeed()) }()
			scanner := bufio.NewScanner(io.LimitReader(logStream, 1<<20))
			foundPi := false
			for scanner.Scan() {
				if strings.Contains(scanner.Text(), "Pi is roughly 3") {
					foundPi = true
					break
				}
			}
			Expect(scanner.Err()).NotTo(HaveOccurred())
			Expect(foundPi).To(BeTrue(), "expected driver logs to contain 'Pi is roughly 3'")
		})
	})

})
