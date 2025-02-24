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
