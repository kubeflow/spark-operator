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
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/internal/controller/sparkapplication"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

var _ = Describe("SparkApplication Controller", func() {
	Context("When reconciling a submitted SparkApplication", func() {
		ctx := context.Background()
		appName := "test"
		appNamespace := "default"
		key := types.NamespacedName{
			Name:      appName,
			Namespace: appNamespace,
		}
		appConfig := &v1beta2.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      appName,
				Namespace: appNamespace,
			},
			Spec: v1beta2.SparkApplicationSpec{
				MainApplicationFile: ptr.To("local:///dummy.jar"),
			},
		}
		ingressKey := types.NamespacedName{
			Name:      util.GetDefaultUIIngressName(appConfig),
			Namespace: appNamespace,
		}

		BeforeEach(func() {
			By("Creating a test SparkApplication")
			app := appConfig.DeepCopy()
			if err := k8sClient.Get(ctx, key, app); err != nil && errors.IsNotFound(err) {
				util.IngressCapabilities = util.Capabilities{"networking.k8s.io/v1": true}
				v1beta2.SetSparkApplicationDefaults(app)
				Expect(k8sClient.Create(ctx, app)).To(Succeed())

				driverPod := createDriverPod(appName, appNamespace)
				Expect(k8sClient.Create(ctx, driverPod)).To(Succeed())

				app.Status.DriverInfo.PodName = driverPod.Name
				app.Status.AppState.State = v1beta2.ApplicationStateSubmitted
				Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
			}
		})

		AfterEach(func() {
			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
			By("Deleting the created test SparkApplication")
			Expect(k8sClient.Delete(ctx, app)).To(Succeed())

			ingress := &networkingv1.Ingress{}
			Expect(k8sClient.Get(ctx, ingressKey, ingress)).To(Succeed())
			By("Deleting the created test ingress")
			Expect(k8sClient.Delete(ctx, ingress)).To(Succeed())

			driverPod := &corev1.Pod{}
			By("Deleting the driver pod")
			Expect(k8sClient.Get(ctx, getDriverNamespacedName(appName, appNamespace), driverPod)).To(Succeed())
			Expect(k8sClient.Delete(ctx, driverPod)).To(Succeed())
		})

		It("Should create an ingress for the Spark UI with no TLS or annotations", func() {
			By("Reconciling the new test SparkApplication")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				record.NewFakeRecorder(3),
				nil,
				&sparkapplication.SparkSubmitter{},
				sparkapplication.Options{EnableUIService: true, IngressURLFormat: "{{$appName}}.spark.test.com", Namespaces: []string{appNamespace}},
			)
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())

			ingress := &networkingv1.Ingress{}
			Expect(k8sClient.Get(ctx, ingressKey, ingress)).To(Succeed())
			Expect(ingress.Spec.TLS).To(HaveLen(0))
			Expect(ingress.ObjectMeta.Annotations).To(HaveLen(0))
		})

		It("Should create an ingress for the Spark UI with default TLS and annotations", func() {
			By("Reconciling the new test SparkApplication")
			defaultIngressTLS := []networkingv1.IngressTLS{
				{
					Hosts:      []string{"*.test.com", "*.test2.com"},
					SecretName: "example-tls-secret",
				},
				{
					Hosts:      []string{"another-test.com"},
					SecretName: "test-tls-secret",
				},
			}
			defaultIngressAnnotations := map[string]string{
				"cert-manager.io/cluster-issuer": "letsencrypt",
				"kubernetes.io/ingress.class":    "nginx",
			}
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				record.NewFakeRecorder(3),
				nil,
				&sparkapplication.SparkSubmitter{},
				sparkapplication.Options{EnableUIService: true, IngressURLFormat: "{{$appName}}.spark.test.com", IngressTLS: defaultIngressTLS, IngressAnnotations: defaultIngressAnnotations, Namespaces: []string{appNamespace}},
			)
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())

			ingress := &networkingv1.Ingress{}
			Expect(k8sClient.Get(ctx, ingressKey, ingress)).To(Succeed())
			Expect(ingress.Spec.TLS).To(Equal(defaultIngressTLS))
			Expect(ingress.ObjectMeta.Annotations).To(Equal(defaultIngressAnnotations))
		})
	})

	Context("When reconciling a submitted SparkApplication with spark UI ingress TLS and annotations", func() {
		ctx := context.Background()
		appName := "test"
		appNamespace := "default"
		key := types.NamespacedName{
			Name:      appName,
			Namespace: appNamespace,
		}
		ingressTLS := []networkingv1.IngressTLS{
			{
				Hosts:      []string{"*.test.com"},
				SecretName: "test-tls-secret",
			},
		}
		ingressAnnotations := map[string]string{"cert-manager.io/cluster-issuer": "letsencrypt"}
		appConfig := &v1beta2.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      appName,
				Namespace: appNamespace,
			},
			Spec: v1beta2.SparkApplicationSpec{
				MainApplicationFile: ptr.To("local:///dummy.jar"),
				SparkUIOptions: &v1beta2.SparkUIConfiguration{
					IngressTLS:         ingressTLS,
					IngressAnnotations: ingressAnnotations,
				},
			},
		}
		ingressKey := types.NamespacedName{
			Name:      util.GetDefaultUIIngressName(appConfig),
			Namespace: appNamespace,
		}

		BeforeEach(func() {
			By("Creating a test SparkApplication")
			app := appConfig.DeepCopy()
			if err := k8sClient.Get(ctx, key, app); err != nil && errors.IsNotFound(err) {
				util.IngressCapabilities = util.Capabilities{"networking.k8s.io/v1": true}

				v1beta2.SetSparkApplicationDefaults(app)
				Expect(k8sClient.Create(ctx, app)).To(Succeed())

				driverPod := createDriverPod(appName, appNamespace)
				Expect(k8sClient.Create(ctx, driverPod)).To(Succeed())

				app.Status.DriverInfo.PodName = driverPod.Name
				app.Status.AppState.State = v1beta2.ApplicationStateSubmitted
				Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
			}
		})

		AfterEach(func() {
			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).To(Succeed())
			By("Deleting the created test SparkApplication")
			Expect(k8sClient.Delete(ctx, app)).To(Succeed())

			ingress := &networkingv1.Ingress{}
			Expect(k8sClient.Get(ctx, ingressKey, ingress)).To(Succeed())
			By("Deleting the created test ingress")
			Expect(k8sClient.Delete(ctx, ingress)).To(Succeed())

			driverPod := &corev1.Pod{}
			By("Deleting the driver pod")
			Expect(k8sClient.Get(ctx, getDriverNamespacedName(appName, appNamespace), driverPod)).To(Succeed())
			Expect(k8sClient.Delete(ctx, driverPod)).To(Succeed())
		})

		It("Should create an ingress for the Spark UI with TLS and annotations", func() {
			By("Reconciling the new test SparkApplication")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				record.NewFakeRecorder(3),
				nil,
				&sparkapplication.SparkSubmitter{},
				sparkapplication.Options{EnableUIService: true, IngressURLFormat: "{{$appName}}.spark.test.com", Namespaces: []string{appNamespace}},
			)
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())

			ingress := &networkingv1.Ingress{}
			Expect(k8sClient.Get(ctx, ingressKey, ingress)).To(Succeed())
			Expect(ingress.Spec.TLS).To(Equal(ingressTLS))
			Expect(ingress.ObjectMeta.Annotations).To(Equal(ingressAnnotations))
		})

		It("Should create an ingress for the Spark UI with TLS and annotations overriding the defaults", func() {
			By("Reconciling the new test SparkApplication")
			defaultIngressTLS := []networkingv1.IngressTLS{
				{
					Hosts:      []string{"*.default.com"},
					SecretName: "default-tls-secret",
				},
			}
			defaultIngressAnnotations := map[string]string{
				"default-annotation-key": "default-annotation-value",
			}
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				record.NewFakeRecorder(3),
				nil,
				&sparkapplication.SparkSubmitter{},
				sparkapplication.Options{EnableUIService: true, IngressURLFormat: "{{$appName}}.spark.test.com", IngressTLS: defaultIngressTLS, IngressAnnotations: defaultIngressAnnotations, Namespaces: []string{appNamespace}},
			)
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())

			ingress := &networkingv1.Ingress{}
			Expect(k8sClient.Get(ctx, ingressKey, ingress)).To(Succeed())
			Expect(ingress.Spec.TLS).To(Equal(ingressTLS))
			Expect(ingress.ObjectMeta.Annotations).To(Equal(ingressAnnotations))
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
						MainApplicationFile: ptr.To("local:///dummy.jar"),
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
				&sparkapplication.SparkSubmitter{},
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
				&sparkapplication.SparkSubmitter{},
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
						MainApplicationFile: ptr.To("local:///dummy.jar"),
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
				&sparkapplication.SparkSubmitter{},
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
						MainApplicationFile: ptr.To("local:///dummy.jar"),
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
				&sparkapplication.SparkSubmitter{},
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
						MainApplicationFile: ptr.To("local:///dummy.jar"),
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
			app.Spec.TimeToLiveSeconds = ptr.To[int64](60)
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
				&sparkapplication.SparkSubmitter{},
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
						MainApplicationFile: ptr.To("local:///dummy.jar"),
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
				&sparkapplication.SparkSubmitter{},
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
						MainApplicationFile: ptr.To("local:///dummy.jar"),
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
			app.Spec.TimeToLiveSeconds = ptr.To[int64](60)
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
				&sparkapplication.SparkSubmitter{},
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
						MainApplicationFile: ptr.To("local:///dummy.jar"),
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
				&sparkapplication.SparkSubmitter{},
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
				&sparkapplication.SparkSubmitter{},
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

	Context("When reconciling a succeeding SparkApplication with Always retry policy", func() {
		ctx := context.Background()
		appName := "test"
		appNamespace := "default"
		key := types.NamespacedName{
			Name:      appName,
			Namespace: appNamespace,
		}

		BeforeEach(func() {
			By("Creating a SparkApplication with Always restart policy")
			app := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      appName,
					Namespace: appNamespace,
				},
				Spec: v1beta2.SparkApplicationSpec{
					MainApplicationFile: ptr.To("local:///dummy.jar"),
					RestartPolicy: v1beta2.RestartPolicy{
						Type: v1beta2.RestartPolicyAlways,
					},
				},
			}
			v1beta2.SetSparkApplicationDefaults(app)
			Expect(k8sClient.Create(ctx, app)).Should(Succeed())

			By("Creating a driver pod with Succeeded phase")
			driver := createDriverPod(appName, appNamespace)
			Expect(k8sClient.Create(ctx, driver)).Should(Succeed())
			driver.Status.Phase = corev1.PodSucceeded
			Expect(k8sClient.Status().Update(ctx, driver)).Should(Succeed())

			By("Creating a executor pod with Succeeded phase")
			executor := createExecutorPod(appName, appNamespace, 1)
			Expect(k8sClient.Create(ctx, executor)).To(Succeed())
			executor.Status.Phase = corev1.PodSucceeded
			Expect(k8sClient.Status().Update(ctx, executor)).To(Succeed())

			By("Updating the SparkApplication state to Succeeding")
			now := time.Now()
			app.Status.SparkApplicationID = "test-app-id"
			app.Status.SubmissionAttempts = 1
			app.Status.LastSubmissionAttemptTime = metav1.NewTime(now.Add(-5 * time.Minute))
			app.Status.ExecutionAttempts = 1
			app.Status.TerminationTime = metav1.NewTime(now.Add(-30 * time.Second))
			app.Status.AppState.State = v1beta2.ApplicationStateSucceeding
			app.Status.DriverInfo.PodName = driver.Name
			app.Status.ExecutorState = map[string]v1beta2.ExecutorState{
				executor.Name: util.GetExecutorState(executor),
			}
			Expect(k8sClient.Status().Update(ctx, app)).Should(Succeed())
		})

		AfterEach(func() {
			By("Deleting the test SparkApplication")
			app := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      appName,
					Namespace: appNamespace,
				},
			}
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, app))).Should(Succeed())

			By("Deleting the driver pod")
			driverKey := getDriverNamespacedName(appName, appNamespace)
			driver := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      driverKey.Name,
					Namespace: driverKey.Namespace,
				},
			}
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, driver))).Should(Succeed())

			By("Deleting the executor pods")
			executorKey := getExecutorNamespacedName(appName, appNamespace, 1)
			executor := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      executorKey.Name,
					Namespace: executorKey.Namespace,
				},
			}
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, executor))).Should(Succeed())
		})

		It("Should reset SparkApplication status when transitioning to PendingRerun", func() {
			By("Reconciling the failing SparkApplication")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				record.NewFakeRecorder(3),
				nil,
				&sparkapplication.SparkSubmitter{},
				sparkapplication.Options{Namespaces: []string{appNamespace}},
			)
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())

			By("Checking whether the SparkApplication status has been reset")
			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).NotTo(HaveOccurred())
			Expect(app.Status.SparkApplicationID).To(BeEmpty())
			Expect(app.Status.TerminationTime).To(BeZero())
			Expect(app.Status.AppState.State).To(Equal(v1beta2.ApplicationStatePendingRerun))
			Expect(app.Status.AppState.ErrorMessage).To(BeEmpty())
			Expect(app.Status.DriverInfo).To(BeZero())
			Expect(app.Status.ExecutorState).To(BeEmpty())
		})

		It("Should delete existing driver and executor pods when transitioning to PendingRerun", func() {
			By("Reconciling the failing SparkApplication")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				record.NewFakeRecorder(3),
				nil,
				&sparkapplication.SparkSubmitter{},
				sparkapplication.Options{Namespaces: []string{appNamespace}},
			)
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())

			By("Checking whether the driver pod has been deleted")
			driverKey := getDriverNamespacedName(appName, appNamespace)
			driverPod := &corev1.Pod{}
			Expect(errors.IsNotFound(k8sClient.Get(ctx, driverKey, driverPod))).To(BeTrue())

			By("Checking whether the executor pods has been deleted")
			executorKey := getDriverNamespacedName(appName, appNamespace)
			executorPod := &corev1.Pod{}
			Expect(errors.IsNotFound(k8sClient.Get(ctx, executorKey, executorPod))).To(BeTrue())
		})
	})

	Context("When reconciling a failing SparkApplication which should retry", func() {
		ctx := context.Background()
		appName := "test"
		appNamespace := "default"
		key := types.NamespacedName{
			Name:      appName,
			Namespace: appNamespace,
		}

		BeforeEach(func() {
			By("Creating a SparkApplication with OnFailure restart policy")
			app := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      appName,
					Namespace: appNamespace,
				},
				Spec: v1beta2.SparkApplicationSpec{
					MainApplicationFile: ptr.To("local:///dummy.jar"),
					RestartPolicy: v1beta2.RestartPolicy{
						Type:                   v1beta2.RestartPolicyOnFailure,
						OnFailureRetries:       ptr.To[int32](3),
						OnFailureRetryInterval: ptr.To[int64](10),
					},
				},
			}
			v1beta2.SetSparkApplicationDefaults(app)
			Expect(k8sClient.Create(ctx, app)).Should(Succeed())

			By("Creating a driver pod with Failed phase")
			driver := createDriverPod(appName, appNamespace)
			Expect(k8sClient.Create(ctx, driver)).Should(Succeed())
			driver.Status.Phase = corev1.PodFailed
			Expect(k8sClient.Status().Update(ctx, driver)).Should(Succeed())

			By("Creating a executor pod with Failed phase")
			executor := createExecutorPod(appName, appNamespace, 1)
			Expect(k8sClient.Create(ctx, executor)).To(Succeed())
			executor.Status.Phase = corev1.PodFailed
			Expect(k8sClient.Status().Update(ctx, executor)).To(Succeed())

			By("Updating the SparkApplication state to Failing")
			now := time.Now()
			app.Status.SparkApplicationID = "test-app-id"
			app.Status.SubmissionAttempts = 1
			app.Status.LastSubmissionAttemptTime = metav1.NewTime(now.Add(-5 * time.Minute))
			app.Status.ExecutionAttempts = 1
			app.Status.TerminationTime = metav1.NewTime(now.Add(-30 * time.Second))
			app.Status.AppState.State = v1beta2.ApplicationStateFailing
			app.Status.AppState.ErrorMessage = "Driver pod failed"
			app.Status.DriverInfo.PodName = driver.Name
			app.Status.ExecutorState = map[string]v1beta2.ExecutorState{
				executor.Name: util.GetExecutorState(executor),
			}
			Expect(k8sClient.Status().Update(ctx, app)).Should(Succeed())
		})

		AfterEach(func() {
			By("Deleting the test SparkApplication")
			app := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      appName,
					Namespace: appNamespace,
				},
			}
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, app))).Should(Succeed())

			By("Deleting the driver pod")
			driverKey := getDriverNamespacedName(appName, appNamespace)
			driver := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      driverKey.Name,
					Namespace: driverKey.Namespace,
				},
			}
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, driver))).Should(Succeed())

			By("Deleting the executor pods")
			executorKey := getExecutorNamespacedName(appName, appNamespace, 1)
			executor := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      executorKey.Name,
					Namespace: executorKey.Namespace,
				},
			}
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, executor))).Should(Succeed())
		})

		It("Should reset SparkApplication status when transitioning to PendingRerun", func() {
			By("Reconciling the failing SparkApplication")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				record.NewFakeRecorder(3),
				nil,
				&sparkapplication.SparkSubmitter{},
				sparkapplication.Options{Namespaces: []string{appNamespace}},
			)
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())

			By("Checking whether the SparkApplication status has been reset")
			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).NotTo(HaveOccurred())
			Expect(app.Status.SparkApplicationID).To(BeEmpty())
			Expect(app.Status.TerminationTime).To(BeZero())
			Expect(app.Status.AppState.State).To(Equal(v1beta2.ApplicationStatePendingRerun))
			Expect(app.Status.AppState.ErrorMessage).To(BeEmpty())
			Expect(app.Status.DriverInfo).To(BeZero())
			Expect(app.Status.ExecutorState).To(BeEmpty())
		})

		It("Should delete existing driver and executor pods when transitioning to PendingRerun", func() {
			By("Reconciling the failing SparkApplication")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				record.NewFakeRecorder(3),
				nil,
				&sparkapplication.SparkSubmitter{},
				sparkapplication.Options{Namespaces: []string{appNamespace}},
			)
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())

			By("Checking whether the driver pod has been deleted")
			driverKey := getDriverNamespacedName(appName, appNamespace)
			driverPod := &corev1.Pod{}
			Expect(errors.IsNotFound(k8sClient.Get(ctx, driverKey, driverPod))).To(BeTrue())

			By("Checking whether the executor pods has been deleted")
			executorKey := getDriverNamespacedName(appName, appNamespace)
			executorPod := &corev1.Pod{}
			Expect(errors.IsNotFound(k8sClient.Get(ctx, executorKey, executorPod))).To(BeTrue())
		})
	})

	Context("Suspend and Resume", func() {
		ctx := context.Background()
		appName := "test"
		appNamespace := "default"
		key := types.NamespacedName{
			Name:      appName,
			Namespace: appNamespace,
		}

		var app *v1beta2.SparkApplication
		BeforeEach(func() {
			app = &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      appName,
					Namespace: appNamespace,
				},
				Spec: v1beta2.SparkApplicationSpec{
					MainApplicationFile: ptr.To("local:///dummy.jar"),
				},
			}
			v1beta2.SetSparkApplicationDefaults(app)
		})

		Context("Suspend", func() {
			When("reconciling a new SparkApplication with Suspend=True", func() {
				BeforeEach(func() {
					app.Spec.Suspend = ptr.To(true)
					Expect(k8sClient.Create(ctx, app)).To(Succeed())
				})
				AfterEach(func() {
					app := &v1beta2.SparkApplication{}
					Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

					By("Deleting the created test SparkApplication")
					Expect(k8sClient.Delete(ctx, app)).To(Succeed())
				})
				It("should transition to Suspending state", func() {
					By("Reconciling the new SparkApplication with Suspend=true")
					reconciler := sparkapplication.NewReconciler(
						nil,
						k8sClient.Scheme(),
						k8sClient,
						record.NewFakeRecorder(3),
						nil,
						&sparkapplication.SparkSubmitter{},
						sparkapplication.Options{Namespaces: []string{appNamespace}, MaxTrackedExecutorPerApp: 10},
					)
					result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
					Expect(err).NotTo(HaveOccurred())
					Expect(result.Requeue).To(BeFalse())

					app := &v1beta2.SparkApplication{}
					Expect(k8sClient.Get(ctx, key, app)).NotTo(HaveOccurred())
					Expect(app.Status.AppState).To(BeEquivalentTo(v1beta2.ApplicationState{State: v1beta2.ApplicationStateSuspending}))
				})
			})
			When("reconciling a Non-Terminated(e.g. Running) SparkApplication with Suspend=true", func() {
				BeforeEach(func() {
					Expect(k8sClient.Create(ctx, app)).To(Succeed())

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

					app.Spec.Suspend = ptr.To(true)
					Expect(k8sClient.Update(ctx, app)).To(Succeed())
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
				It("should transition to Suspending state", func() {
					By("Reconciling the Running SparkApplication with Suspend=true")
					reconciler := sparkapplication.NewReconciler(
						nil,
						k8sClient.Scheme(),
						k8sClient,
						record.NewFakeRecorder(3),
						nil,
						&sparkapplication.SparkSubmitter{},
						sparkapplication.Options{Namespaces: []string{appNamespace}, MaxTrackedExecutorPerApp: 10},
					)
					result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
					Expect(err).NotTo(HaveOccurred())
					Expect(result.Requeue).To(BeFalse())

					app := &v1beta2.SparkApplication{}
					Expect(k8sClient.Get(ctx, key, app)).NotTo(HaveOccurred())
					Expect(app.Status.AppState).To(BeEquivalentTo(v1beta2.ApplicationState{State: v1beta2.ApplicationStateSuspending}))
				})
			})
			When("reconciling a Terminated(Failed or Completed) SparkApplication with Suspend=true", func() {
				BeforeEach(func() {
					app.Spec.Suspend = ptr.To(true)
					Expect(k8sClient.Create(ctx, app)).To(Succeed())
					app.Status.AppState.State = v1beta2.ApplicationStateFailed
					Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
				})
				AfterEach(func() {
					app := &v1beta2.SparkApplication{}
					Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

					By("Deleting the created test SparkApplication")
					Expect(k8sClient.Delete(ctx, app)).To(Succeed())
				})
				It("should not change its application state", func() {
					By("Reconciling the terminated SparkApplication with Suspend=true")
					reconciler := sparkapplication.NewReconciler(
						nil,
						k8sClient.Scheme(),
						k8sClient,
						record.NewFakeRecorder(3),
						nil,
						&sparkapplication.SparkSubmitter{},
						sparkapplication.Options{Namespaces: []string{appNamespace}, MaxTrackedExecutorPerApp: 10},
					)
					result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
					Expect(err).NotTo(HaveOccurred())
					Expect(result.Requeue).To(BeFalse())

					app := &v1beta2.SparkApplication{}
					Expect(k8sClient.Get(ctx, key, app)).NotTo(HaveOccurred())
					Expect(app.Status.AppState).To(BeEquivalentTo(v1beta2.ApplicationState{State: v1beta2.ApplicationStateFailed}))
				})
			})
			When("reconciling Resuming SparkApplication with Suspend=true", func() {
				BeforeEach(func() {
					app.Spec.Suspend = ptr.To(true)
					Expect(k8sClient.Create(ctx, app)).To(Succeed())
					app.Status.AppState.State = v1beta2.ApplicationStateResuming
					Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
				})
				AfterEach(func() {
					app := &v1beta2.SparkApplication{}
					Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

					By("Deleting the created test SparkApplication")
					Expect(k8sClient.Delete(ctx, app)).To(Succeed())
				})
				It("should transition to Suspending state", func() {
					By("Reconciling the Resuming SparkApplication with Suspend=true")
					reconciler := sparkapplication.NewReconciler(
						nil,
						k8sClient.Scheme(),
						k8sClient,
						record.NewFakeRecorder(3),
						nil,
						&sparkapplication.SparkSubmitter{},
						sparkapplication.Options{Namespaces: []string{appNamespace}, MaxTrackedExecutorPerApp: 10},
					)
					result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
					Expect(err).NotTo(HaveOccurred())
					Expect(result.Requeue).To(BeFalse())

					app := &v1beta2.SparkApplication{}
					Expect(k8sClient.Get(ctx, key, app)).NotTo(HaveOccurred())
					Expect(app.Status.AppState).To(BeEquivalentTo(v1beta2.ApplicationState{State: v1beta2.ApplicationStateSuspending}))
				})
			})
			When("reconciling a Suspending SparkApplication with Suspend=true", func() {
				var driverPod *corev1.Pod
				BeforeEach(func() {
					app.Spec.Suspend = ptr.To(true)
					Expect(k8sClient.Create(ctx, app)).To(Succeed())

					driverPod = createDriverPod(appName, appNamespace)
					Expect(k8sClient.Create(ctx, driverPod)).To(Succeed())
					driverPod.Status.Phase = corev1.PodRunning
					Expect(k8sClient.Status().Update(ctx, driverPod)).To(Succeed())

					app.Status.DriverInfo.PodName = driverPod.Name
					Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())

					app.Status.AppState.State = v1beta2.ApplicationStateSuspending
					Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
				})
				AfterEach(func() {
					app := &v1beta2.SparkApplication{}
					Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

					By("Deleting the created test SparkApplication")
					Expect(k8sClient.Delete(ctx, app)).To(Succeed())
				})
				It("should delete dependent spark resources and transition to Suspended app state", func() {
					By("Reconciling the Suspending SparkApplication with Suspend=true")
					reconciler := sparkapplication.NewReconciler(
						nil,
						k8sClient.Scheme(),
						k8sClient,
						record.NewFakeRecorder(3),
						nil,
						&sparkapplication.SparkSubmitter{},
						sparkapplication.Options{Namespaces: []string{appNamespace}, MaxTrackedExecutorPerApp: 10},
					)
					result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
					Expect(err).NotTo(HaveOccurred())
					Expect(result.Requeue).To(BeFalse())

					Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(driverPod), &corev1.Pod{})).To(Satisfy(errors.IsNotFound))
					app := &v1beta2.SparkApplication{}
					Expect(k8sClient.Get(ctx, key, app)).NotTo(HaveOccurred())
					Expect(app.Status.AppState).To(BeEquivalentTo(v1beta2.ApplicationState{State: v1beta2.ApplicationStateSuspended}))
				})
			})
		})
		Context("Resume", func() {
			When("reconciling Suspended SparkApplication with Suspend=false(resuming)", func() {
				BeforeEach(func() {
					app.Spec.Suspend = ptr.To(false)
					Expect(k8sClient.Create(ctx, app)).To(Succeed())
					app.Status.AppState.State = v1beta2.ApplicationStateSuspended
					Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
				})
				AfterEach(func() {
					app := &v1beta2.SparkApplication{}
					Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

					By("Deleting the created test SparkApplication")
					Expect(k8sClient.Delete(ctx, app)).To(Succeed())
				})
				It("should transition to Resuming state", func() {
					By("Reconciling the Suspended SparkApplication with Suspend=false")
					reconciler := sparkapplication.NewReconciler(
						nil,
						k8sClient.Scheme(),
						k8sClient,
						record.NewFakeRecorder(3),
						nil,
						&sparkapplication.SparkSubmitter{},
						sparkapplication.Options{Namespaces: []string{appNamespace}, MaxTrackedExecutorPerApp: 10},
					)
					result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
					Expect(err).NotTo(HaveOccurred())
					Expect(result.Requeue).To(BeFalse())

					app := &v1beta2.SparkApplication{}
					Expect(k8sClient.Get(ctx, key, app)).NotTo(HaveOccurred())
					Expect(app.Status.AppState).To(BeEquivalentTo(v1beta2.ApplicationState{State: v1beta2.ApplicationStateResuming}))
				})
			})
			When("reconciling Suspending SparkApplication with Suspend=false(resuming)", func() {
				var driverPod *corev1.Pod
				BeforeEach(func() {
					app.Spec.Suspend = ptr.To(false)
					Expect(k8sClient.Create(ctx, app)).To(Succeed())

					driverPod = createDriverPod(appName, appNamespace)
					Expect(k8sClient.Create(ctx, driverPod)).To(Succeed())
					driverPod.Status.Phase = corev1.PodRunning
					Expect(k8sClient.Status().Update(ctx, driverPod)).To(Succeed())

					app.Status.DriverInfo.PodName = driverPod.Name
					Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())

					app.Status.AppState.State = v1beta2.ApplicationStateSuspending
					Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
				})
				AfterEach(func() {
					app := &v1beta2.SparkApplication{}
					Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

					By("Deleting the created test SparkApplication")
					Expect(k8sClient.Delete(ctx, app)).To(Succeed())
				})
				It("should transition to Suspended -> Resuming state", func() {
					By("Reconciling the Suspending SparkApplication with Suspend=false")
					reconciler := sparkapplication.NewReconciler(
						nil,
						k8sClient.Scheme(),
						k8sClient,
						record.NewFakeRecorder(3),
						nil,
						&sparkapplication.SparkSubmitter{},
						sparkapplication.Options{Namespaces: []string{appNamespace}, MaxTrackedExecutorPerApp: 10},
					)
					result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
					Expect(err).NotTo(HaveOccurred())
					Expect(result.Requeue).To(BeFalse())

					Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(driverPod), &corev1.Pod{})).To(Satisfy(errors.IsNotFound))
					app := &v1beta2.SparkApplication{}
					Expect(k8sClient.Get(ctx, key, app)).NotTo(HaveOccurred())
					Expect(app.Status.AppState).To(BeEquivalentTo(v1beta2.ApplicationState{State: v1beta2.ApplicationStateSuspended}))

					By("Reconciling the Suspended SparkApplication with Suspend=false")
					result, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
					Expect(err).NotTo(HaveOccurred())
					Expect(result.Requeue).To(BeFalse())
					app = &v1beta2.SparkApplication{}
					Expect(k8sClient.Get(ctx, key, app)).NotTo(HaveOccurred())
					Expect(app.Status.AppState).To(BeEquivalentTo(v1beta2.ApplicationState{State: v1beta2.ApplicationStateResuming}))
				})
			})
			When("reconciling Resuming SparkApplication with Suspend=false(resuming)", func() {
				BeforeEach(func() {
					app.Spec.Suspend = ptr.To(false)
					Expect(k8sClient.Create(ctx, app)).To(Succeed())
					app.Status.AppState.State = v1beta2.ApplicationStateResuming
					Expect(k8sClient.Status().Update(ctx, app)).To(Succeed())
				})
				AfterEach(func() {
					app := &v1beta2.SparkApplication{}
					Expect(k8sClient.Get(ctx, key, app)).To(Succeed())

					By("Deleting the created test SparkApplication")
					Expect(k8sClient.Delete(ctx, app)).To(Succeed())
				})
				It("should submit pods and transition to Submitted", func() {
					Skip("This transition can not test because there is no spark-submit")
				})
			})
		})
	})

	Context("When reconciling a new SparkApplication with a pre-existing driver pod", func() {
		ctx := context.Background()
		appName := "test-idempotent"
		appNamespace := "default"
		key := types.NamespacedName{
			Name:      appName,
			Namespace: appNamespace,
		}
		existingSubmissionID := "previous-submission-id-12345"

		BeforeEach(func() {
			By("Creating a SparkApplication in New state")
			app := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      appName,
					Namespace: appNamespace,
				},
				Spec: v1beta2.SparkApplicationSpec{
					MainApplicationFile: ptr.To("local:///dummy.jar"),
				},
			}
			v1beta2.SetSparkApplicationDefaults(app)
			Expect(k8sClient.Create(ctx, app)).To(Succeed())

			By("Pre-creating a driver pod with submission labels")
			driverPod := createDriverPod(appName, appNamespace)
			driverPod.Labels[common.LabelSubmissionID] = existingSubmissionID
			Expect(k8sClient.Create(ctx, driverPod)).To(Succeed())
		})

		AfterEach(func() {
			By("Deleting the test SparkApplication")
			app := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      appName,
					Namespace: appNamespace,
				},
			}
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, app))).To(Succeed())

			By("Deleting the driver pod")
			driverKey := getDriverNamespacedName(appName, appNamespace)
			driver := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      driverKey.Name,
					Namespace: driverKey.Namespace,
				},
			}
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, driver))).To(Succeed())
		})

		It("Should recover submission state from existing driver pod instead of re-submitting", func() {
			By("Reconciling the new SparkApplication")
			reconciler := sparkapplication.NewReconciler(
				nil,
				k8sClient.Scheme(),
				k8sClient,
				record.NewFakeRecorder(3),
				nil,
				&sparkapplication.SparkSubmitter{},
				sparkapplication.Options{Namespaces: []string{appNamespace}},
			)
			result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			By("Checking that the app transitioned to Submitted with the recovered SubmissionID")
			app := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, app)).NotTo(HaveOccurred())
			Expect(app.Status.AppState.State).To(Equal(v1beta2.ApplicationStateSubmitted))
			Expect(app.Status.SubmissionID).To(Equal(existingSubmissionID))
			Expect(app.Status.DriverInfo.PodName).To(Equal(fmt.Sprintf("%s-driver", appName)))
			Expect(app.Status.SubmissionAttempts).To(Equal(int32(1)))
			Expect(app.Status.ExecutionAttempts).To(Equal(int32(1)))
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
