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

package sparkapplication_test

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/internal/controller/sparkapplication"
	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
)

var _ = Describe("SparkApplication Controller", func() {
	Context("When reconciling a new SparkApplication", func() {
		ctx := context.Background()
		appName := "test"
		appNamespace := "default"
		key := types.NamespacedName{
			Name:      appName,
			Namespace: appNamespace,
		}

		BeforeEach(func() {
			By("Creating a test SparkApplication")
			app := &v1beta2.SparkApplication{}
			if err := k8sClient.Get(ctx, key, app); err != nil && errors.IsNotFound(err) {
				app = &v1beta2.SparkApplication{
					ObjectMeta: metav1.ObjectMeta{
						Name:      appName,
						Namespace: appNamespace,
					},
				}
				v1beta2.SetSparkApplicationDefaults(app)
				Expect(k8sClient.Create(ctx, app)).To(Succeed())

				app.Status.AppState.State = v1beta2.ApplicationStateCompleted
				Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
			}
		})

		AfterEach(func() {
			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

			By("Deleting the created test SparkApplication")
			Expect(k8sClient.Delete(ctx, app)).To(Succeed())
		})
	})

	Context("When reconciling a submitted SparkApplication with no driver pod", func() {
		ctx := context.Background()
		appName := "test"
		appNamespace := "default"
		key := types.NamespacedName{
			Name:      appName,
			Namespace: appNamespace,
		}

		BeforeEach(func() {
			By("Creating a test SparkApplication")
			app := &v1beta2.SparkApplication{}
			if err := k8sClient.Get(ctx, key, app); err != nil && errors.IsNotFound(err) {
				app = &v1beta2.SparkApplication{
					ObjectMeta: metav1.ObjectMeta{
						Name:      appName,
						Namespace: appNamespace,
					},
					Spec: v1beta2.SparkApplicationSpec{
						MainApplicationFile: util.StringPtr("local:///dummy.jar"),
					},
				}
				v1beta2.SetSparkApplicationDefaults(app)
				Expect(k8sClient.Create(ctx, app)).To(Succeed())

				app.Status.AppState.State = v1beta2.ApplicationStateSubmitted
				app.Status.DriverInfo.PodName = "non-existent-driver"
				app.Status.LastSubmissionAttemptTime = metav1.NewTime(time.Now())
				Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
			}
		})

		AfterEach(func() {
			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

			By("Deleting the created test SparkApplication")
			Expect(k8sClient.Delete(ctx, app)).To(Succeed())
		})

		It("Should requeue submitted SparkApplication when driver pod not found inside the grace period", func() {
			By("Reconciling the created test SparkApplication")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				nil,
				nil,
				sparkapplication.Options{Namespaces: []string{appNamespace}, DriverPodCreationGracePeriod: 10 * time.Second},
			)
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).To(MatchError(ContainSubstring("driver pod not found, while inside the grace period. Grace period of")))
			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
			Expect(app.Status.AppState.State).To(Equal(v1beta2.ApplicationStateSubmitted))
		})

		It("Should fail a SparkApplication when driver pod not found outside the grace period", func() {
			By("Reconciling the created test SparkApplication")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				nil,
				nil,
				sparkapplication.Options{Namespaces: []string{appNamespace}, DriverPodCreationGracePeriod: 0 * time.Second},
			)
			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
			Expect(app.Status.AppState.State).To(Equal(v1beta2.ApplicationStateFailing))
		})
	})

	Context("When reconciling a SparkApplication with driver pod", func() {
		ctx := context.Background()
		appName := "test"
		appNamespace := "default"
		key := types.NamespacedName{
			Name:      appName,
			Namespace: appNamespace,
		}

		BeforeEach(func() {
			By("Creating a test SparkApplication")
			app := &v1beta2.SparkApplication{}
			if err := k8sClient.Get(ctx, key, app); err != nil && errors.IsNotFound(err) {
				app = &v1beta2.SparkApplication{
					ObjectMeta: metav1.ObjectMeta{
						Name:      appName,
						Namespace: appNamespace,
					},
					Spec: v1beta2.SparkApplicationSpec{
						MainApplicationFile: util.StringPtr("local:///dummy.jar"),
					},
				}
				v1beta2.SetSparkApplicationDefaults(app)
				Expect(k8sClient.Create(ctx, app)).To(Succeed())

				app.Status.AppState.State = v1beta2.ApplicationStateSubmitted
				driverPod := createDriverPod(appName, appNamespace)
				Expect(k8sClient.Create(ctx, driverPod)).To(Succeed())
				app.Status.DriverInfo.PodName = driverPod.Name
				Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
			}
		})

		AfterEach(func() {
			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

			By("Deleting the created test SparkApplication")
			Expect(k8sClient.Delete(ctx, app)).To(Succeed())

			By("Deleting the driver pod")
			driverPod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, getDriverNamespacedName(appName, appNamespace), driverPod)).To(Succeed())
			Expect(k8sClient.Delete(ctx, driverPod)).To(Succeed())
		})

		It("When reconciling a submitted SparkApplication when driver pod exists", func() {
			By("Reconciling the created test SparkApplication")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				nil,
				nil,
				sparkapplication.Options{Namespaces: []string{appNamespace}, DriverPodCreationGracePeriod: 0 * time.Second},
			)
			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
			Expect(app.Status.AppState.State).To(Equal(v1beta2.ApplicationStateSubmitted))
		})
	})

	Context("When reconciling a completed SparkApplication", func() {
		ctx := context.Background()
		appName := "test"
		appNamespace := "default"
		key := types.NamespacedName{
			Name:      appName,
			Namespace: appNamespace,
		}

		BeforeEach(func() {
			By("Creating a test SparkApplication")
			app := &v1beta2.SparkApplication{}
			if err := k8sClient.Get(ctx, key, app); err != nil && errors.IsNotFound(err) {
				app = &v1beta2.SparkApplication{
					ObjectMeta: metav1.ObjectMeta{
						Name:      appName,
						Namespace: appNamespace,
						Labels: map[string]string{
							common.LabelSparkAppName: app.Name,
						},
					},
					Spec: v1beta2.SparkApplicationSpec{
						MainApplicationFile: util.StringPtr("local:///dummy.jar"),
					},
				}
				v1beta2.SetSparkApplicationDefaults(app)
				Expect(k8sClient.Create(ctx, app)).To(Succeed())

				app.Status.AppState.State = v1beta2.ApplicationStateCompleted
				Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
			}
		})

		AfterEach(func() {
			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

			By("Deleting the created test SparkApplication")
			Expect(k8sClient.Delete(ctx, app)).To(Succeed())
		})

		It("Should successfully reconcile a completed SparkApplication", func() {
			By("Reconciling the created test SparkApplication")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				nil,
				nil,
				sparkapplication.Options{Namespaces: []string{appNamespace}},
			)
			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())
		})
	})

	Context("When reconciling a completed expired SparkApplication", func() {
		ctx := context.Background()
		appName := "test"
		appNamespace := "default"
		key := types.NamespacedName{
			Name:      appName,
			Namespace: appNamespace,
		}

		BeforeEach(func() {
			By("Creating a test SparkApplication")
			app := &v1beta2.SparkApplication{}
			if err := k8sClient.Get(ctx, key, app); err != nil && errors.IsNotFound(err) {
				app = &v1beta2.SparkApplication{
					ObjectMeta: metav1.ObjectMeta{
						Name:      appName,
						Namespace: appNamespace,
					},
					Spec: v1beta2.SparkApplicationSpec{
						MainApplicationFile: util.StringPtr("local:///dummy.jar"),
					},
				}
				v1beta2.SetSparkApplicationDefaults(app)
				Expect(k8sClient.Create(ctx, app)).To(Succeed())

				app.Status.AppState.State = v1beta2.ApplicationStateCompleted
				Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
			}
		})

		AfterEach(func() {
			app := &v1beta2.SparkApplication{}
			Expect(errors.IsNotFound(k8sClient.Get(ctx, key, app))).To(BeTrue())
		})

		It("Should delete expired SparkApplication", func() {
			By("Set TimeToLiveSeconds and make the SparkApplication expired")
			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
			app.Spec.TimeToLiveSeconds = util.Int64Ptr(60)
			Expect(k8sClient.Update(ctx, app)).To(Succeed())
			app.Status.TerminationTime = metav1.NewTime(time.Now().Add(-2 * time.Minute))
			Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())

			By("Reconciling the expired SparkApplication")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				nil,
				nil,
				sparkapplication.Options{Namespaces: []string{appNamespace}},
			)
			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())
		})
	})

	Context("When reconciling a failed SparkApplication", func() {
		ctx := context.Background()
		appName := "test"
		appNamespace := "default"
		key := types.NamespacedName{
			Name:      appName,
			Namespace: appNamespace,
		}

		BeforeEach(func() {
			By("Creating a test SparkApplication")
			app := &v1beta2.SparkApplication{}
			if err := k8sClient.Get(ctx, key, app); err != nil && errors.IsNotFound(err) {
				app = &v1beta2.SparkApplication{
					ObjectMeta: metav1.ObjectMeta{
						Name:      appName,
						Namespace: appNamespace,
					},
					Spec: v1beta2.SparkApplicationSpec{
						MainApplicationFile: util.StringPtr("local:///dummy.jar"),
					},
				}
				v1beta2.SetSparkApplicationDefaults(app)
				Expect(k8sClient.Create(ctx, app)).To(Succeed())

				app.Status.AppState.State = v1beta2.ApplicationStateFailed
				Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
			}
		})

		AfterEach(func() {
			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

			By("Deleting the created test SparkApplication")
			Expect(k8sClient.Delete(ctx, app)).To(Succeed())
		})

		It("Should successfully reconcile a failed SparkApplication", func() {
			By("Reconciling the created test SparkApplication")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				nil,
				nil,
				sparkapplication.Options{Namespaces: []string{appNamespace}},
			)
			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())
		})
	})

	Context("When reconciling a failed expired SparkApplication", func() {
		ctx := context.Background()
		appName := "test"
		appNamespace := "default"
		key := types.NamespacedName{
			Name:      appName,
			Namespace: appNamespace,
		}

		BeforeEach(func() {
			By("Creating a test SparkApplication")
			app := &v1beta2.SparkApplication{}
			if err := k8sClient.Get(ctx, key, app); err != nil && errors.IsNotFound(err) {
				app = &v1beta2.SparkApplication{
					ObjectMeta: metav1.ObjectMeta{
						Name:      appName,
						Namespace: appNamespace,
					},
					Spec: v1beta2.SparkApplicationSpec{
						MainApplicationFile: util.StringPtr("local:///dummy.jar"),
					},
				}
				v1beta2.SetSparkApplicationDefaults(app)
				Expect(k8sClient.Create(ctx, app)).To(Succeed())

				app.Status.AppState.State = v1beta2.ApplicationStateFailed
				Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
			}
		})

		AfterEach(func() {
			app := &v1beta2.SparkApplication{}
			Expect(errors.IsNotFound(k8sClient.Get(ctx, key, app))).To(BeTrue())
		})

		It("Should delete expired SparkApplication", func() {
			By("Set TimeToLiveSeconds and make the SparkApplication expired")
			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
			app.Spec.TimeToLiveSeconds = util.Int64Ptr(60)
			Expect(k8sClient.Update(ctx, app)).To(Succeed())
			app.Status.TerminationTime = metav1.NewTime(time.Now().Add(-2 * time.Minute))
			Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())

			By("Reconciling the expired SparkApplication")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				nil,
				nil,
				sparkapplication.Options{Namespaces: []string{appNamespace}},
			)
			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())
		})
	})

	Context("When reconciling a running SparkApplication", func() {
		ctx := context.Background()
		appName := "test"
		appNamespace := "default"
		key := types.NamespacedName{
			Name:      appName,
			Namespace: appNamespace,
		}

		BeforeEach(func() {
			By("Creating a test SparkApplication")
			app := &v1beta2.SparkApplication{}
			if err := k8sClient.Get(ctx, key, app); err != nil && errors.IsNotFound(err) {
				app = &v1beta2.SparkApplication{
					ObjectMeta: metav1.ObjectMeta{
						Name:      appName,
						Namespace: appNamespace,
					},
					Spec: v1beta2.SparkApplicationSpec{
						MainApplicationFile: util.StringPtr("local:///dummy.jar"),
					},
				}
				v1beta2.SetSparkApplicationDefaults(app)
				Expect(k8sClient.Create(ctx, app)).To(Succeed())
			}
			driverPod := createDriverPod(appName, appNamespace)
			Expect(k8sClient.Create(ctx, driverPod)).To(Succeed())
			driverPod.Status.Phase = corev1.PodRunning
			Expect(k8sClient.Status().Update(ctx, driverPod)).To(Succeed())

			app.Status.DriverInfo.PodName = driverPod.Name
			app.Status.AppState.State = v1beta2.ApplicationStateRunning
			Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())

			executorPod1 := createExecutorPod(appName, appNamespace, 1)
			Expect(k8sClient.Create(ctx, executorPod1)).To(Succeed())
			executorPod1.Status.Phase = corev1.PodRunning
			Expect(k8sClient.Status().Update(ctx, executorPod1)).To(Succeed())

			executorPod2 := createExecutorPod(appName, appNamespace, 2)
			Expect(k8sClient.Create(ctx, executorPod2)).To(Succeed())
			executorPod2.Status.Phase = corev1.PodRunning
			Expect(k8sClient.Status().Update(ctx, executorPod2)).To(Succeed())
		})

		AfterEach(func() {
			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

			By("Deleting the created test SparkApplication")
			Expect(k8sClient.Delete(ctx, app)).To(Succeed())

			By("Deleting the driver pod")
			driverPod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, getDriverNamespacedName(appName, appNamespace), driverPod)).To(Succeed())
			Expect(k8sClient.Delete(ctx, driverPod)).To(Succeed())

			By("Deleting the executor pods")
			executorPod1 := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, getExecutorNamespacedName(appName, appNamespace, 1), executorPod1)).To(Succeed())
			Expect(k8sClient.Delete(ctx, executorPod1)).To(Succeed())
			executorPod2 := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, getExecutorNamespacedName(appName, appNamespace, 2), executorPod2)).To(Succeed())
			Expect(k8sClient.Delete(ctx, executorPod2)).To(Succeed())
		})

		It("Should add the executors to the SparkApplication", func() {
			By("Reconciling the running SparkApplication")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				record.NewFakeRecorder(3),
				nil,
				sparkapplication.Options{Namespaces: []string{appNamespace}, MaxTrackedExecutorPerApp: 10},
			)
			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).NotTo(HaveOccurred())
			Expect(app.Status.ExecutorState).To(HaveLen(2))
		})

		It("Should only add 1 executor to the SparkApplication", func() {
			By("Reconciling the running SparkApplication")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				record.NewFakeRecorder(3),
				nil,
				sparkapplication.Options{Namespaces: []string{appNamespace}, MaxTrackedExecutorPerApp: 1},
			)
			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).NotTo(HaveOccurred())
			Expect(app.Status.ExecutorState).To(HaveLen(1))
		})
	})

	Context("When reconciling a pending rerun SparkApplication with running driver pod", func() {
		ctx := context.Background()
		appName := "test-pending-rerun"
		appNamespace := "default"
		key := types.NamespacedName{
			Name:      appName,
			Namespace: appNamespace,
		}

		BeforeEach(func() {
			By("Creating a test SparkApplication in pending rerun state")
			app := &v1beta2.SparkApplication{}
			if err := k8sClient.Get(ctx, key, app); err != nil && errors.IsNotFound(err) {
				app = &v1beta2.SparkApplication{
					ObjectMeta: metav1.ObjectMeta{
						Name:      appName,
						Namespace: appNamespace,
					},
					Spec: v1beta2.SparkApplicationSpec{
						MainApplicationFile: util.StringPtr("local:///dummy.jar"),
					},
				}
				v1beta2.SetSparkApplicationDefaults(app)
				Expect(k8sClient.Create(ctx, app)).To(Succeed())

				// Create a running driver pod (simulating operator restart scenario)
				driverPod := createDriverPod(appName, appNamespace)
				Expect(k8sClient.Create(ctx, driverPod)).To(Succeed())
				driverPod.Status.Phase = corev1.PodRunning
				Expect(k8sClient.Status().Update(ctx, driverPod)).To(Succeed())

				// Set app to pending rerun with no start time or attempts
				app.Status.AppState.State = v1beta2.ApplicationStatePendingRerun
				app.Status.DriverInfo.PodName = driverPod.Name
				app.Status.LastSubmissionAttemptTime = metav1.Time{} // Empty time
				app.Status.ExecutionAttempts = 0
				Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
			}
		})

		AfterEach(func() {
			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

			By("Deleting the created test SparkApplication")
			Expect(k8sClient.Delete(ctx, app)).To(Succeed())

			By("Deleting the driver pod")
			driverPod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, getDriverNamespacedName(appName, appNamespace), driverPod)).To(Succeed())
			Expect(k8sClient.Delete(ctx, driverPod)).To(Succeed())
		})

		It("Should preserve start time and execution attempts from running driver pod", func() {
			By("Reconciling the pending rerun SparkApplication")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				record.NewFakeRecorder(3),
				nil,
				sparkapplication.Options{Namespaces: []string{appNamespace}},
			)
			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

			// Verify that start time was set from pod creation time
			Expect(app.Status.LastSubmissionAttemptTime.IsZero()).To(BeFalse())

			// Verify that execution attempts was set to 1
			Expect(app.Status.ExecutionAttempts).To(Equal(int32(1)))

			// Verify state transitioned to running
			Expect(app.Status.AppState.State).To(Equal(v1beta2.ApplicationStateRunning))
		})
	})

	Context("When reconciling a pending rerun SparkApplication with pending driver pod", func() {
		ctx := context.Background()
		appName := "test-pending-rerun-pending-pod"
		appNamespace := "default"
		key := types.NamespacedName{
			Name:      appName,
			Namespace: appNamespace,
		}

		BeforeEach(func() {
			By("Creating a test SparkApplication in pending rerun state")
			app := &v1beta2.SparkApplication{}
			if err := k8sClient.Get(ctx, key, app); err != nil && errors.IsNotFound(err) {
				app = &v1beta2.SparkApplication{
					ObjectMeta: metav1.ObjectMeta{
						Name:      appName,
						Namespace: appNamespace,
					},
					Spec: v1beta2.SparkApplicationSpec{
						MainApplicationFile: util.StringPtr("local:///dummy.jar"),
					},
				}
				v1beta2.SetSparkApplicationDefaults(app)
				Expect(k8sClient.Create(ctx, app)).To(Succeed())

				// Create a pending driver pod
				driverPod := createDriverPod(appName, appNamespace)
				Expect(k8sClient.Create(ctx, driverPod)).To(Succeed())
				driverPod.Status.Phase = corev1.PodPending
				Expect(k8sClient.Status().Update(ctx, driverPod)).To(Succeed())

				// Set app to pending rerun with no start time or attempts
				app.Status.AppState.State = v1beta2.ApplicationStatePendingRerun
				app.Status.DriverInfo.PodName = driverPod.Name
				app.Status.LastSubmissionAttemptTime = metav1.Time{} // Empty time
				app.Status.ExecutionAttempts = 0
				Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
			}
		})

		AfterEach(func() {
			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

			By("Deleting the created test SparkApplication")
			Expect(k8sClient.Delete(ctx, app)).To(Succeed())

			By("Deleting the driver pod")
			driverPod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, getDriverNamespacedName(appName, appNamespace), driverPod)).To(Succeed())
			Expect(k8sClient.Delete(ctx, driverPod)).To(Succeed())
		})

		It("Should preserve start time and execution attempts from pending driver pod", func() {
			By("Reconciling the pending rerun SparkApplication")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				record.NewFakeRecorder(3),
				nil,
				sparkapplication.Options{Namespaces: []string{appNamespace}},
			)
			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

			// Verify that start time was set from pod creation time
			Expect(app.Status.LastSubmissionAttemptTime.IsZero()).To(BeFalse())

			// Verify that execution attempts was set to 1
			Expect(app.Status.ExecutionAttempts).To(Equal(int32(1)))
		})
	})

	Context("When reconciling a pending rerun SparkApplication with failed driver pod", func() {
		ctx := context.Background()
		appName := "test-pending-rerun-failed-pod"
		appNamespace := "default"
		key := types.NamespacedName{
			Name:      appName,
			Namespace: appNamespace,
		}

		BeforeEach(func() {
			By("Creating a test SparkApplication in pending rerun state")
			app := &v1beta2.SparkApplication{}
			if err := k8sClient.Get(ctx, key, app); err != nil && errors.IsNotFound(err) {
				app = &v1beta2.SparkApplication{
					ObjectMeta: metav1.ObjectMeta{
						Name:      appName,
						Namespace: appNamespace,
					},
					Spec: v1beta2.SparkApplicationSpec{
						MainApplicationFile: util.StringPtr("local:///dummy.jar"),
					},
				}
				v1beta2.SetSparkApplicationDefaults(app)
				Expect(k8sClient.Create(ctx, app)).To(Succeed())

				// Create a failed driver pod (terminal state)
				driverPod := createDriverPod(appName, appNamespace)
				Expect(k8sClient.Create(ctx, driverPod)).To(Succeed())
				driverPod.Status.Phase = corev1.PodFailed
				Expect(k8sClient.Status().Update(ctx, driverPod)).To(Succeed())

				// Set app to pending rerun with no start time or attempts
				app.Status.AppState.State = v1beta2.ApplicationStatePendingRerun
				app.Status.DriverInfo.PodName = driverPod.Name
				app.Status.LastSubmissionAttemptTime = metav1.Time{} // Empty time
				app.Status.ExecutionAttempts = 0
				Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
			}
		})

		AfterEach(func() {
			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

			By("Deleting the created test SparkApplication")
			Expect(k8sClient.Delete(ctx, app)).To(Succeed())

			By("Deleting the driver pod if it exists")
			driverPod := &corev1.Pod{}
			if err := k8sClient.Get(ctx, getDriverNamespacedName(appName, appNamespace), driverPod); err == nil {
				Expect(k8sClient.Delete(ctx, driverPod)).To(Succeed())
			}
		})

		It("Should not preserve start time when driver pod is in failed state", func() {
			By("Reconciling the pending rerun SparkApplication")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				record.NewFakeRecorder(3),
				nil,
				sparkapplication.Options{Namespaces: []string{appNamespace}},
			)
			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

			// When driver pod is in failed state, the normal pending rerun flow should proceed
			// This means the app should remain in pending rerun state (waiting for pod deletion)
			// and should NOT have preserved the start time or execution attempts from the failed pod
			Expect(app.Status.AppState.State).To(Equal(v1beta2.ApplicationStatePendingRerun))

			// Start time and execution attempts should remain empty/zero since the pod is failed
			// and needs to be cleaned up before resubmission
			Expect(app.Status.LastSubmissionAttemptTime.IsZero()).To(BeTrue())
			Expect(app.Status.ExecutionAttempts).To(Equal(int32(0)))
		})
	})

	Context("When reconciling a pending rerun SparkApplication with mismatched driver pod", func() {
		ctx := context.Background()
		appName := "test-pending-rerun-mismatch"
		appNamespace := "default"
		key := types.NamespacedName{
			Name:      appName,
			Namespace: appNamespace,
		}

		BeforeEach(func() {
			By("Creating a test SparkApplication in pending rerun state")
			app := &v1beta2.SparkApplication{}
			if err := k8sClient.Get(ctx, key, app); err != nil && errors.IsNotFound(err) {
				app = &v1beta2.SparkApplication{
					ObjectMeta: metav1.ObjectMeta{
						Name:      appName,
						Namespace: appNamespace,
					},
					Spec: v1beta2.SparkApplicationSpec{
						MainApplicationFile: util.StringPtr("local:///dummy.jar"),
					},
				}
				v1beta2.SetSparkApplicationDefaults(app)
				Expect(k8sClient.Create(ctx, app)).To(Succeed())

				// Create a driver pod with WRONG app name label (simulating name collision)
				driverPod := createDriverPod(appName, appNamespace)
				// Override the app name label to simulate a different application
				driverPod.Labels[common.LabelSparkAppName] = "different-app"
				Expect(k8sClient.Create(ctx, driverPod)).To(Succeed())
				driverPod.Status.Phase = corev1.PodRunning
				Expect(k8sClient.Status().Update(ctx, driverPod)).To(Succeed())

				// Set app to pending rerun with no start time or attempts
				app.Status.AppState.State = v1beta2.ApplicationStatePendingRerun
				app.Status.DriverInfo.PodName = driverPod.Name
				app.Status.LastSubmissionAttemptTime = metav1.Time{} // Empty time
				app.Status.ExecutionAttempts = 0
				Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
			}
		})

		AfterEach(func() {
			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

			By("Deleting the created test SparkApplication")
			Expect(k8sClient.Delete(ctx, app)).To(Succeed())

			By("Deleting the driver pod")
			driverPod := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, getDriverNamespacedName(appName, appNamespace), driverPod)).To(Succeed())
			Expect(k8sClient.Delete(ctx, driverPod)).To(Succeed())
		})

		It("Should not sync state from pod with mismatched app name label", func() {
			By("Reconciling the pending rerun SparkApplication")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				record.NewFakeRecorder(3),
				nil,
				sparkapplication.Options{Namespaces: []string{appNamespace}},
			)
			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

			// Should NOT have synced state from the mismatched pod
			// Start time and execution attempts should remain empty/zero
			Expect(app.Status.LastSubmissionAttemptTime.IsZero()).To(BeTrue())
			Expect(app.Status.ExecutionAttempts).To(Equal(int32(0)))

			// Should remain in pending rerun state
			Expect(app.Status.AppState.State).To(Equal(v1beta2.ApplicationStatePendingRerun))
		})
	})

	Context("When reconciling a pending rerun SparkApplication with pod being deleted", func() {
		ctx := context.Background()
		appName := "test-pending-rerun-deleting"
		appNamespace := "default"
		key := types.NamespacedName{
			Name:      appName,
			Namespace: appNamespace,
		}

		BeforeEach(func() {
			By("Creating a test SparkApplication in pending rerun state")
			app := &v1beta2.SparkApplication{}
			if err := k8sClient.Get(ctx, key, app); err != nil && errors.IsNotFound(err) {
				app = &v1beta2.SparkApplication{
					ObjectMeta: metav1.ObjectMeta{
						Name:      appName,
						Namespace: appNamespace,
					},
					Spec: v1beta2.SparkApplicationSpec{
						MainApplicationFile: util.StringPtr("local:///dummy.jar"),
					},
				}
				v1beta2.SetSparkApplicationDefaults(app)
				Expect(k8sClient.Create(ctx, app)).To(Succeed())

				// Create a driver pod with a finalizer so it doesn't immediately disappear
				driverPod := createDriverPod(appName, appNamespace)
				driverPod.Finalizers = []string{"sparkoperator.k8s.io/test-finalizer"}
				Expect(k8sClient.Create(ctx, driverPod)).To(Succeed())
				driverPod.Status.Phase = corev1.PodRunning
				Expect(k8sClient.Status().Update(ctx, driverPod)).To(Succeed())

				// Delete the pod - it will have DeletionTimestamp set but won't be fully deleted due to finalizer
				Expect(k8sClient.Delete(ctx, driverPod)).To(Succeed())

				// Set app to pending rerun with no start time or attempts
				app.Status.AppState.State = v1beta2.ApplicationStatePendingRerun
				app.Status.DriverInfo.PodName = driverPod.Name
				app.Status.LastSubmissionAttemptTime = metav1.Time{} // Empty time
				app.Status.ExecutionAttempts = 0
				app.Status.SubmissionAttempts = 0
				Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
			}
		})

		AfterEach(func() {
			app := &v1beta2.SparkApplication{}
			if err := k8sClient.Get(ctx, key, app); err == nil {
				By("Deleting the created test SparkApplication")
				Expect(k8sClient.Delete(ctx, app)).To(Succeed())
			}

			By("Removing finalizer and deleting the driver pod if it exists")
			driverPod := &corev1.Pod{}
			if err := k8sClient.Get(ctx, getDriverNamespacedName(appName, appNamespace), driverPod); err == nil {
				// Remove finalizer so pod can be deleted
				driverPod.Finalizers = []string{}
				_ = k8sClient.Update(ctx, driverPod)
				_ = k8sClient.Delete(ctx, driverPod)
			}
		})

		It("Should not sync state from pod with deletion timestamp set", func() {
			By("Reconciling the pending rerun SparkApplication")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				record.NewFakeRecorder(3),
				nil,
				sparkapplication.Options{Namespaces: []string{appNamespace}},
			)
			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			By("Verifying that start time and attempts were NOT preserved from the deleting pod")
			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

			// Should NOT have preserved metadata from a pod being deleted
			Expect(app.Status.LastSubmissionAttemptTime.IsZero()).To(BeTrue(), "LastSubmissionAttemptTime should remain empty when pod is being deleted")
			Expect(app.Status.ExecutionAttempts).To(Equal(int32(0)), "ExecutionAttempts should remain 0 when pod is being deleted")
			Expect(app.Status.SubmissionAttempts).To(Equal(int32(0)), "SubmissionAttempts should remain 0 when pod is being deleted")
		})
	})
})

func getDriverNamespacedName(appName string, appNamespace string) types.NamespacedName {
	return types.NamespacedName{
		Name:      fmt.Sprintf("%s-driver", appName),
		Namespace: appNamespace,
	}
}

func createDriverPod(appName string, appNamespace string) *corev1.Pod {
	namespacedName := getDriverNamespacedName(appName, appNamespace)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      namespacedName.Name,
			Namespace: namespacedName.Namespace,
			Labels: map[string]string{
				common.LabelSparkAppName:            appName,
				common.LabelLaunchedBySparkOperator: "true",
				common.LabelSparkRole:               common.SparkRoleDriver,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  common.SparkDriverContainerName,
					Image: "spark-executor:latest",
				},
			},
		},
	}
	return pod
}

func getExecutorNamespacedName(appName string, appNamespace string, id int) types.NamespacedName {
	return types.NamespacedName{
		Name:      fmt.Sprintf("%s-exec%d", appName, id),
		Namespace: appNamespace,
	}
}

func createExecutorPod(appName string, appNamespace string, id int) *corev1.Pod {
	namespacedName := getExecutorNamespacedName(appName, appNamespace, id)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      namespacedName.Name,
			Namespace: namespacedName.Namespace,
			Labels: map[string]string{
				common.LabelSparkAppName:            appName,
				common.LabelLaunchedBySparkOperator: "true",
				common.LabelSparkRole:               common.SparkRoleExecutor,
				common.LabelSparkExecutorID:         fmt.Sprintf("%d", id),
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  common.SparkExecutorContainerName,
					Image: "spark-executor:latest",
				},
			},
		},
	}
	return pod
}

var _ = Describe("Orphaned Resource Cleanup", func() {
	Context("When reconciling a NEW SparkApplication with orphaned driver pod", func() {
		ctx := context.Background()
		appName := "test-orphaned-pod"
		appNamespace := "default"
		key := types.NamespacedName{
			Name:      appName,
			Namespace: appNamespace,
		}

		BeforeEach(func() {
			By("Creating an orphaned driver pod")
			driverPod := createDriverPod(appName, appNamespace)
			driverPod.Status.Phase = corev1.PodPending
			Expect(k8sClient.Create(ctx, driverPod)).To(Succeed())

			By("Creating a NEW SparkApplication (simulating controller restart)")
			app := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      appName,
					Namespace: appNamespace,
				},
				Spec: v1beta2.SparkApplicationSpec{
					MainApplicationFile: util.StringPtr("local:///dummy.jar"),
				},
			}
			v1beta2.SetSparkApplicationDefaults(app)
			Expect(k8sClient.Create(ctx, app)).To(Succeed())

			// Application state should be NEW (empty string)
			app.Status.AppState.State = v1beta2.ApplicationStateNew
			Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
		})

		AfterEach(func() {
			By("Cleaning up the SparkApplication")
			app := &v1beta2.SparkApplication{}
			if err := k8sClient.Get(ctx, key, app); err == nil {
				Expect(k8sClient.Delete(ctx, app)).To(Succeed())
			}

			By("Cleaning up the driver pod if it still exists")
			driverPod := &corev1.Pod{}
			if err := k8sClient.Get(ctx, getDriverNamespacedName(appName, appNamespace), driverPod); err == nil {
				Expect(k8sClient.Delete(ctx, driverPod)).To(Succeed())
			}
		})

		It("Should detect and cleanup orphaned driver pod, then requeue", func() {
			By("Reconciling the NEW SparkApplication with orphaned pod")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				&record.FakeRecorder{},
				nil,
				sparkapplication.Options{Namespaces: []string{appNamespace}},
			)

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})

			By("Expecting error indicating orphaned resources were deleted")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("orphaned resources"))
			Expect(err.Error()).To(ContainSubstring("were deleted, requeuing for submission"))

			By("Verifying driver pod was deleted")
			Eventually(func() bool {
				driverPod := &corev1.Pod{}
				err := k8sClient.Get(ctx, getDriverNamespacedName(appName, appNamespace), driverPod)
				return errors.IsNotFound(err)
			}, 5*time.Second, 500*time.Millisecond).Should(BeTrue())

			By("Verifying application state remains NEW")
			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
			Expect(app.Status.AppState.State).To(Equal(v1beta2.ApplicationStateNew))
		})
	})

	Context("When reconciling a NEW SparkApplication with no orphaned resources", func() {
		ctx := context.Background()
		appName := "test-no-orphaned"
		appNamespace := "default"
		key := types.NamespacedName{
			Name:      appName,
			Namespace: appNamespace,
		}

		BeforeEach(func() {
			By("Creating a NEW SparkApplication without any orphaned resources")
			app := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      appName,
					Namespace: appNamespace,
				},
				Spec: v1beta2.SparkApplicationSpec{
					MainApplicationFile: util.StringPtr("local:///dummy.jar"),
				},
			}
			v1beta2.SetSparkApplicationDefaults(app)
			Expect(k8sClient.Create(ctx, app)).To(Succeed())

			app.Status.AppState.State = v1beta2.ApplicationStateNew
			Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
		})

		AfterEach(func() {
			app := &v1beta2.SparkApplication{}
			if err := k8sClient.Get(ctx, key, app); err == nil {
				Expect(k8sClient.Delete(ctx, app)).To(Succeed())
			}
		})

		It("Should proceed with normal submission without cleanup", func() {
			By("Reconciling the NEW SparkApplication")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				&record.FakeRecorder{},
				nil,
				sparkapplication.Options{Namespaces: []string{appNamespace}},
			)

			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})

			By("Expecting no error from resource validation")
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			By("Verifying application state transitioned from NEW")
			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
			// State should no longer be NEW after submission attempt
			Expect(app.Status.AppState.State).NotTo(Equal(v1beta2.ApplicationStateNew))
		})
	})

	Context("When reconciling a NEW SparkApplication with orphaned service", func() {
		ctx := context.Background()
		appName := "test-orphaned-service"
		appNamespace := "default"
		key := types.NamespacedName{
			Name:      appName,
			Namespace: appNamespace,
		}

		BeforeEach(func() {
			By("Creating an orphaned web UI service")
			service := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-ui-svc", appName),
					Namespace: appNamespace,
					Labels: map[string]string{
						common.LabelSparkAppName: appName,
					},
				},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{
						{
							Port: 4040,
						},
					},
					Selector: map[string]string{
						common.LabelSparkRole: common.SparkRoleDriver,
					},
				},
			}
			Expect(k8sClient.Create(ctx, service)).To(Succeed())

			By("Creating a NEW SparkApplication")
			app := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      appName,
					Namespace: appNamespace,
				},
				Spec: v1beta2.SparkApplicationSpec{
					MainApplicationFile: util.StringPtr("local:///dummy.jar"),
				},
			}
			v1beta2.SetSparkApplicationDefaults(app)
			Expect(k8sClient.Create(ctx, app)).To(Succeed())

			// Set status with service name to trigger validation
			app.Status.AppState.State = v1beta2.ApplicationStateNew
			app.Status.DriverInfo.WebUIServiceName = service.Name
			Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
		})

		AfterEach(func() {
			app := &v1beta2.SparkApplication{}
			if err := k8sClient.Get(ctx, key, app); err == nil {
				Expect(k8sClient.Delete(ctx, app)).To(Succeed())
			}

			service := &corev1.Service{}
			serviceKey := types.NamespacedName{
				Name:      fmt.Sprintf("%s-ui-svc", appName),
				Namespace: appNamespace,
			}
			if err := k8sClient.Get(ctx, serviceKey, service); err == nil {
				Expect(k8sClient.Delete(ctx, service)).To(Succeed())
			}
		})

		It("Should detect and cleanup orphaned service", func() {
			By("Reconciling the NEW SparkApplication with orphaned service")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				&record.FakeRecorder{},
				nil,
				sparkapplication.Options{Namespaces: []string{appNamespace}},
			)

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})

			By("Expecting error indicating orphaned resources were deleted")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("orphaned resources"))

			By("Verifying service was deleted")
			Eventually(func() bool {
				service := &corev1.Service{}
				serviceKey := types.NamespacedName{
					Name:      fmt.Sprintf("%s-ui-svc", appName),
					Namespace: appNamespace,
				}
				err := k8sClient.Get(ctx, serviceKey, service)
				return errors.IsNotFound(err)
			}, 5*time.Second, 500*time.Millisecond).Should(BeTrue())
		})
	})

	Context("When reconciling a NEW SparkApplication with multiple orphaned resources", func() {
		ctx := context.Background()
		appName := "test-multiple-orphaned"
		appNamespace := "default"
		key := types.NamespacedName{
			Name:      appName,
			Namespace: appNamespace,
		}

		BeforeEach(func() {
			By("Creating orphaned driver pod")
			driverPod := createDriverPod(appName, appNamespace)
			driverPod.Status.Phase = corev1.PodRunning
			Expect(k8sClient.Create(ctx, driverPod)).To(Succeed())

			By("Creating orphaned web UI service")
			service := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-ui-svc", appName),
					Namespace: appNamespace,
					Labels: map[string]string{
						common.LabelSparkAppName: appName,
					},
				},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{
						{
							Port: 4040,
						},
					},
					Selector: map[string]string{
						common.LabelSparkRole: common.SparkRoleDriver,
					},
				},
			}
			Expect(k8sClient.Create(ctx, service)).To(Succeed())

			By("Creating a NEW SparkApplication")
			app := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      appName,
					Namespace: appNamespace,
				},
				Spec: v1beta2.SparkApplicationSpec{
					MainApplicationFile: util.StringPtr("local:///dummy.jar"),
				},
			}
			v1beta2.SetSparkApplicationDefaults(app)
			Expect(k8sClient.Create(ctx, app)).To(Succeed())

			app.Status.AppState.State = v1beta2.ApplicationStateNew
			app.Status.DriverInfo.WebUIServiceName = service.Name
			Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
		})

		AfterEach(func() {
			app := &v1beta2.SparkApplication{}
			if err := k8sClient.Get(ctx, key, app); err == nil {
				Expect(k8sClient.Delete(ctx, app)).To(Succeed())
			}

			driverPod := &corev1.Pod{}
			if err := k8sClient.Get(ctx, getDriverNamespacedName(appName, appNamespace), driverPod); err == nil {
				Expect(k8sClient.Delete(ctx, driverPod)).To(Succeed())
			}

			service := &corev1.Service{}
			serviceKey := types.NamespacedName{
				Name:      fmt.Sprintf("%s-ui-svc", appName),
				Namespace: appNamespace,
			}
			if err := k8sClient.Get(ctx, serviceKey, service); err == nil {
				Expect(k8sClient.Delete(ctx, service)).To(Succeed())
			}
		})

		It("Should detect and cleanup all orphaned resources", func() {
			By("Reconciling the NEW SparkApplication with multiple orphaned resources")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				&record.FakeRecorder{},
				nil,
				sparkapplication.Options{Namespaces: []string{appNamespace}},
			)

			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})

			By("Expecting error indicating cleanup occurred")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("orphaned resources"))

			By("Verifying all resources were deleted")
			Eventually(func() bool {
				driverPod := &corev1.Pod{}
				podErr := k8sClient.Get(ctx, getDriverNamespacedName(appName, appNamespace), driverPod)

				service := &corev1.Service{}
				serviceKey := types.NamespacedName{
					Name:      fmt.Sprintf("%s-ui-svc", appName),
					Namespace: appNamespace,
				}
				serviceErr := k8sClient.Get(ctx, serviceKey, service)

				return errors.IsNotFound(podErr) && errors.IsNotFound(serviceErr)
			}, 5*time.Second, 500*time.Millisecond).Should(BeTrue())
		})
	})
})
