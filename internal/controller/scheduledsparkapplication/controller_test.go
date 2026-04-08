/*
Copyright 2024 The Kubeflow authors.

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
	})
})

var _ = Describe("ScheduledSparkApplication spec change detection", func() {
	Context("when spec.schedule changes in ScheduleStateScheduled", func() {
		const resourceName = "repro-schedule-change"
		ctx := context.Background()
		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}

		BeforeEach(func() {
			resource := &v1beta2.ScheduledSparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: v1beta2.ScheduledSparkApplicationSpec{
					Schedule:          "10 * * * *",
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
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
		})

		AfterEach(func() {
			resource := &v1beta2.ScheduledSparkApplication{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			}
		})

		It("should recalculate nextRun when spec.schedule changes", func() {
			reconciler := NewReconciler(k8sClient.Scheme(), k8sClient, nil, clock.RealClock{}, Options{Namespaces: []string{"default"}, TimestampPrecision: "nanos"})

			By("Reconciling to reach ScheduleStateScheduled")
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			// Verify it's in Scheduled state
			app := &v1beta2.ScheduledSparkApplication{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, app)).To(Succeed())
			Expect(app.Status.ScheduleState).To(Equal(v1beta2.ScheduleStateScheduled))
			oldNextRun := app.Status.NextRun

			By("Changing spec.schedule from '10 * * * *' to '50 * * * *'")
			app.Spec.Schedule = "50 * * * *"
			Expect(k8sClient.Update(ctx, app)).To(Succeed())

			By("Reconciling again after schedule change")
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			updatedApp := &v1beta2.ScheduledSparkApplication{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, updatedApp)).To(Succeed())
			Expect(updatedApp.Status.NextRun.Time).NotTo(Equal(oldNextRun.Time),
				"nextRun should be recalculated after spec.schedule change")
		})
	})

	Context("when spec is fixed after FailedValidation", func() {
		const resourceName = "repro-failed-validation"
		ctx := context.Background()
		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}

		BeforeEach(func() {
			resource := &v1beta2.ScheduledSparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: v1beta2.ScheduledSparkApplicationSpec{
					Schedule:          "invalid-cron",
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
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
		})

		AfterEach(func() {
			resource := &v1beta2.ScheduledSparkApplication{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			}
		})

		It("should recover from FailedValidation when spec is fixed", func() {
			reconciler := NewReconciler(k8sClient.Scheme(), k8sClient, nil, clock.RealClock{}, Options{Namespaces: []string{"default"}, TimestampPrecision: "nanos"})

			By("Reconciling to reach ScheduleStateFailedValidation")
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			app := &v1beta2.ScheduledSparkApplication{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, app)).To(Succeed())
			Expect(app.Status.ScheduleState).To(Equal(v1beta2.ScheduleStateFailedValidation))

			By("Fixing spec.schedule to a valid cron expression")
			app.Spec.Schedule = "10 * * * *"
			Expect(k8sClient.Update(ctx, app)).To(Succeed())

			By("Reconciling again after fixing the schedule")
			_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			updatedApp := &v1beta2.ScheduledSparkApplication{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, updatedApp)).To(Succeed())
			Expect(updatedApp.Status.ScheduleState).To(Equal(v1beta2.ScheduleStateScheduled),
				"should recover from FailedValidation to Scheduled after spec is fixed")
			Expect(updatedApp.Status.NextRun.IsZero()).To(BeFalse(),
				"nextRun should be set after recovering from FailedValidation")
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
