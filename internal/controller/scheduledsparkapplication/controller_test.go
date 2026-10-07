/*
Copyright 2024 The Kubeflow Authors.

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

package scheduledsparkapplication

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	testingclock "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

var _ = Describe("ScheduledSparkApplication Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		scheduledsparkapplication := &v1beta2.ScheduledSparkApplication{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind ScheduledSparkApplication")
			err := k8sClient.Get(ctx, typeNamespacedName, scheduledsparkapplication)
			if err != nil && errors.IsNotFound(err) {
				resource := &v1beta2.ScheduledSparkApplication{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: v1beta2.ScheduledSparkApplicationSpec{
						Schedule:          "@every 1m",
						ConcurrencyPolicy: v1beta2.ConcurrencyAllow,
						Template: v1beta2.SparkApplicationSpec{
							Type: v1beta2.SparkApplicationTypeScala,
							Mode: v1beta2.DeployModeCluster,
							RestartPolicy: v1beta2.RestartPolicy{
								Type: v1beta2.RestartPolicyNever,
							},
							MainApplicationFile: ptr.To("local:///dummy.jar"),
						},
					},
					// TODO(user): Specify other spec details if needed.
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &v1beta2.ScheduledSparkApplication{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance ScheduledSparkApplication")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})

		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			reconciler := NewReconciler(k8sClient.Scheme(), k8sClient, nil, clock.RealClock{}, Options{Namespaces: []string{"default"}, TimestampPrecision: "nanos"})
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
			// Example: If you expect a certain status condition after reconciliation, verify it here.
		})

		It("should transition to FailedValidation on invalid schedule and recover to Scheduled when corrected", func() {
			t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			fakeClock := testingclock.NewFakeClock(t0)
			reconciler := NewReconciler(k8sClient.Scheme(), k8sClient, nil, fakeClock, Options{Namespaces: []string{"default"}, TimestampPrecision: "nanos"})

			By("Initially reconciling to Scheduled state with a valid schedule")
			app := &v1beta2.ScheduledSparkApplication{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, app)).To(Succeed())
			app.Spec.Schedule = "@every 1h"
			Expect(k8sClient.Update(ctx, app)).To(Succeed())

			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(1 * time.Hour))

			Expect(k8sClient.Get(ctx, typeNamespacedName, app)).To(Succeed())
			Expect(app.Status.ScheduleState).To(Equal(v1beta2.ScheduleStateScheduled))
			initialNextRun := app.Status.NextRun
			Expect(initialNextRun.Time).To(BeTemporally("==", t0.Add(1*time.Hour)))

			By("Updating the schedule to an invalid value")
			app.Spec.Schedule = "invalid-cron-schedule"
			Expect(k8sClient.Update(ctx, app)).To(Succeed())

			By("Reconciling with invalid schedule")
			result, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())

			By("Verifying the status reflects FailedValidation while preserving stale NextRun")
			Expect(k8sClient.Get(ctx, typeNamespacedName, app)).To(Succeed())
			Expect(app.Status.ScheduleState).To(Equal(v1beta2.ScheduleStateFailedValidation))
			Expect(app.Status.Reason).To(ContainSubstring("expected exactly 5 fields"))
			Expect(app.Status.NextRun.Time).To(BeTemporally("==", initialNextRun.Time))

			By("Correcting the schedule to a new valid schedule with a later next run")
			// @every 2h ensures the new next run (t0 + 2h) is after the stale next run (t0 + 1h),
			// directly verifying that recovery updates NextRun even when nextRunTime is not before oldNextRunTime.
			app.Spec.Schedule = "@every 2h"
			Expect(k8sClient.Update(ctx, app)).To(Succeed())

			By("Reconciling after correcting the schedule")
			result, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(2 * time.Hour))

			By("Verifying the status recovered to Scheduled with recalculated NextRun")
			Expect(k8sClient.Get(ctx, typeNamespacedName, app)).To(Succeed())
			Expect(app.Status.ScheduleState).To(Equal(v1beta2.ScheduleStateScheduled))
			Expect(app.Status.Reason).To(BeEmpty())
			Expect(app.Status.NextRun.Time).To(BeTemporally("==", t0.Add(2*time.Hour)))
			Expect(app.Status.NextRun.Time).NotTo(BeTemporally("==", initialNextRun.Time))
		})

		It("should transition to FailedValidation on invalid timezone and recover to Scheduled when corrected", func() {
			t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
			fakeClock := testingclock.NewFakeClock(t0)
			reconciler := NewReconciler(k8sClient.Scheme(), k8sClient, nil, fakeClock, Options{Namespaces: []string{"default"}, TimestampPrecision: "nanos"})

			By("Initially reconciling to Scheduled state with a valid timezone")
			app := &v1beta2.ScheduledSparkApplication{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, app)).To(Succeed())
			app.Spec.Schedule = "0 14 * * *"
			app.Spec.TimeZone = "UTC"
			Expect(k8sClient.Update(ctx, app)).To(Succeed())

			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(2 * time.Hour))

			Expect(k8sClient.Get(ctx, typeNamespacedName, app)).To(Succeed())
			Expect(app.Status.ScheduleState).To(Equal(v1beta2.ScheduleStateScheduled))
			initialNextRun := app.Status.NextRun
			Expect(initialNextRun.Time).To(BeTemporally("==", t0.Add(2*time.Hour)))

			By("Updating the timezone to an invalid value")
			app.Spec.TimeZone = "Invalid/Timezone"
			Expect(k8sClient.Update(ctx, app)).To(Succeed())

			By("Reconciling with invalid timezone")
			result, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())

			By("Verifying the status reflects FailedValidation while preserving stale NextRun")
			Expect(k8sClient.Get(ctx, typeNamespacedName, app)).To(Succeed())
			Expect(app.Status.ScheduleState).To(Equal(v1beta2.ScheduleStateFailedValidation))
			Expect(app.Status.Reason).To(ContainSubstring("unknown time zone"))
			Expect(app.Status.NextRun.Time).To(BeTemporally("==", initialNextRun.Time))

			By("Correcting the timezone to a timezone that produces a later next run")
			// 14:00 in America/New_York (EST, UTC-5) corresponds to 19:00 UTC (t0 + 7h).
			// This tests that recovery overwrites stale NextRun (t0 + 2h) with the recalculated next run (t0 + 7h).
			app.Spec.TimeZone = "America/New_York"
			Expect(k8sClient.Update(ctx, app)).To(Succeed())

			By("Reconciling after correcting the timezone")
			result, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(7 * time.Hour))

			By("Verifying the status recovered to Scheduled with recalculated NextRun")
			Expect(k8sClient.Get(ctx, typeNamespacedName, app)).To(Succeed())
			Expect(app.Status.ScheduleState).To(Equal(v1beta2.ScheduleStateScheduled))
			Expect(app.Status.Reason).To(BeEmpty())
			Expect(app.Status.NextRun.Time).To(BeTemporally("==", t0.Add(7*time.Hour)))
			Expect(app.Status.NextRun.Time).NotTo(BeTemporally("==", initialNextRun.Time))
		})
	})
})

var _ = Describe("formatTimestamp", func() {
	var testTime time.Time

	BeforeEach(func() {
		// Use a fixed timestamp for consistent test results
		testTime = time.Unix(1234567890, 123456789)
	})

	DescribeTable("should format timestamp with correct precision",
		func(precision string, expectedLen int, checkFunc func(result string)) {
			result := formatTimestamp(testTime, precision)
			Expect(len(result)).To(BeNumerically("<=", expectedLen))
			checkFunc(result)
		},
		Entry("nanos precision", "nanos", 20, func(result string) {
			Expect(result).To(Equal("1234567890123456789"))
		}),
		Entry("micros precision", "micros", 17, func(result string) {
			Expect(result).To(Equal("1234567890123456"))
		}),
		Entry("millis precision", "millis", 14, func(result string) {
			Expect(result).To(Equal("1234567890123"))
		}),
		Entry("seconds precision", "seconds", 11, func(result string) {
			Expect(result).To(Equal("1234567890"))
		}),
		Entry("minutes precision", "minutes", 9, func(result string) {
			Expect(result).To(Equal("20576131"))
		}),
	)

	It("should use nanos as default for unknown precision", func() {
		result := formatTimestamp(testTime, "invalid")
		Expect(result).To(Equal("1234567890123456789"))
	})

	It("should use nanos for empty precision", func() {
		result := formatTimestamp(testTime, "")
		Expect(result).To(Equal("1234567890123456789"))
	})
})
