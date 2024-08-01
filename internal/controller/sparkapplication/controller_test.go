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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
})
