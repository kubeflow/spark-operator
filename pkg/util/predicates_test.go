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

package util_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

var _ = Describe("NamespacePredicate", func() {
	var fakeClient client.Client

	BeforeEach(func() {
		scheme := runtime.NewScheme()
		_ = corev1.AddToScheme(scheme)
		_ = v1beta2.AddToScheme(scheme)
		fakeClient = fake.NewClientBuilder().WithScheme(scheme).Build()
	})

	Context("NewNamespacePredicate", func() {
		It("should create predicate with explicit namespaces", func() {
			predicate, err := util.NewNamespacePredicate(fakeClient, []string{"default", "prod"}, "")
			Expect(err).NotTo(HaveOccurred())
			Expect(predicate).NotTo(BeNil())
		})

		It("should create predicate with selector", func() {
			predicate, err := util.NewNamespacePredicate(fakeClient, []string{}, "spark=enabled")
			Expect(err).NotTo(HaveOccurred())
			Expect(predicate).NotTo(BeNil())
		})

		It("should create predicate with both explicit namespaces and selector", func() {
			predicate, err := util.NewNamespacePredicate(fakeClient, []string{"default"}, "spark=enabled")
			Expect(err).NotTo(HaveOccurred())
			Expect(predicate).NotTo(BeNil())
		})

		It("should reject invalid selector", func() {
			_, err := util.NewNamespacePredicate(fakeClient, []string{}, "=value")
			Expect(err).To(HaveOccurred())
		})
	})

	Context("Create event filtering", func() {
		It("should filter Create events based on explicit namespaces", func() {
			predicate, _ := util.NewNamespacePredicate(fakeClient, []string{"default", "prod"}, "")

			// Create event in watched namespace
			obj := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: "default",
				},
			}
			e := event.CreateEvent{Object: obj}
			Expect(predicate.Create(e)).To(BeTrue())

			// Create event in unwatched namespace
			obj.Namespace = "other"
			e = event.CreateEvent{Object: obj}
			Expect(predicate.Create(e)).To(BeFalse())
		})

		It("should filter Create events based on namespace selector", func() {
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "team-a",
					Labels: map[string]string{"spark": "enabled"},
				},
			}
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = v1beta2.AddToScheme(scheme)
			fakeClient = fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns).Build()

			predicate, _ := util.NewNamespacePredicate(fakeClient, []string{}, "spark=enabled")

			// Create event in namespace matching selector
			obj := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: "team-a",
				},
			}
			e := event.CreateEvent{Object: obj}
			Expect(predicate.Create(e)).To(BeTrue())

			// Create event in namespace not matching selector
			ns2 := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "team-b",
					Labels: map[string]string{"env": "dev"},
				},
			}
			fakeClient = fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns2).Build()
			predicate, _ = util.NewNamespacePredicate(fakeClient, []string{}, "spark=enabled")

			obj.Namespace = "team-b"
			e = event.CreateEvent{Object: obj}
			Expect(predicate.Create(e)).To(BeFalse())
		})

		It("should return false on Create event when namespace not found", func() {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = v1beta2.AddToScheme(scheme)
			fakeClient = fake.NewClientBuilder().WithScheme(scheme).Build()

			predicate, _ := util.NewNamespacePredicate(fakeClient, []string{}, "spark=enabled")

			obj := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: "non-existent",
				},
			}
			e := event.CreateEvent{Object: obj}
			Expect(predicate.Create(e)).To(BeFalse())
		})
	})

	Context("Update event filtering", func() {
		It("should filter Update events based on explicit namespaces", func() {
			predicate, _ := util.NewNamespacePredicate(fakeClient, []string{"default"}, "")

			oldObj := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: "default",
				},
			}
			newObj := oldObj.DeepCopy()

			// Update event in watched namespace
			e := event.UpdateEvent{ObjectOld: oldObj, ObjectNew: newObj}
			Expect(predicate.Update(e)).To(BeTrue())

			// Update event in unwatched namespace
			newObj.Namespace = "other"
			e = event.UpdateEvent{ObjectOld: oldObj, ObjectNew: newObj}
			Expect(predicate.Update(e)).To(BeFalse())
		})

		It("should filter Update events based on namespace selector", func() {
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "team-a",
					Labels: map[string]string{"spark": "enabled"},
				},
			}
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = v1beta2.AddToScheme(scheme)
			fakeClient = fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns).Build()

			predicate, _ := util.NewNamespacePredicate(fakeClient, []string{}, "spark=enabled")

			oldObj := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: "team-a",
				},
			}
			newObj := oldObj.DeepCopy()

			e := event.UpdateEvent{ObjectOld: oldObj, ObjectNew: newObj}
			Expect(predicate.Update(e)).To(BeTrue())
		})

		It("should return false on Update event when namespace not found", func() {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = v1beta2.AddToScheme(scheme)
			fakeClient = fake.NewClientBuilder().WithScheme(scheme).Build()

			predicate, _ := util.NewNamespacePredicate(fakeClient, []string{}, "spark=enabled")

			oldObj := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: "non-existent",
				},
			}
			newObj := oldObj.DeepCopy()

			e := event.UpdateEvent{ObjectOld: oldObj, ObjectNew: newObj}
			Expect(predicate.Update(e)).To(BeFalse())
		})
	})

	Context("Delete event filtering", func() {
		It("should filter Delete events based on explicit namespaces", func() {
			predicate, _ := util.NewNamespacePredicate(fakeClient, []string{"default"}, "")

			obj := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: "default",
				},
			}

			// Delete event in watched namespace
			e := event.DeleteEvent{Object: obj}
			Expect(predicate.Delete(e)).To(BeTrue())

			// Delete event in unwatched namespace
			obj.Namespace = "other"
			e = event.DeleteEvent{Object: obj}
			Expect(predicate.Delete(e)).To(BeFalse())
		})

		It("should filter Delete events based on namespace selector", func() {
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "team-a",
					Labels: map[string]string{"spark": "enabled"},
				},
			}
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = v1beta2.AddToScheme(scheme)
			fakeClient = fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns).Build()

			predicate, _ := util.NewNamespacePredicate(fakeClient, []string{}, "spark=enabled")

			obj := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: "team-a",
				},
			}

			e := event.DeleteEvent{Object: obj}
			Expect(predicate.Delete(e)).To(BeTrue())
		})

		It("should return false on Delete event when namespace not found", func() {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = v1beta2.AddToScheme(scheme)
			fakeClient = fake.NewClientBuilder().WithScheme(scheme).Build()

			predicate, _ := util.NewNamespacePredicate(fakeClient, []string{}, "spark=enabled")

			obj := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: "non-existent",
				},
			}

			e := event.DeleteEvent{Object: obj}
			Expect(predicate.Delete(e)).To(BeFalse())
		})
	})

	Context("Generic event filtering", func() {
		It("should filter Generic events based on explicit namespaces", func() {
			predicate, _ := util.NewNamespacePredicate(fakeClient, []string{"default"}, "")

			obj := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: "default",
				},
			}

			// Generic event in watched namespace
			e := event.GenericEvent{Object: obj}
			Expect(predicate.Generic(e)).To(BeTrue())

			// Generic event in unwatched namespace
			obj.Namespace = "other"
			e = event.GenericEvent{Object: obj}
			Expect(predicate.Generic(e)).To(BeFalse())
		})

		It("should filter Generic events based on namespace selector", func() {
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "team-a",
					Labels: map[string]string{"spark": "enabled"},
				},
			}
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = v1beta2.AddToScheme(scheme)
			fakeClient = fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns).Build()

			predicate, _ := util.NewNamespacePredicate(fakeClient, []string{}, "spark=enabled")

			obj := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: "team-a",
				},
			}

			e := event.GenericEvent{Object: obj}
			Expect(predicate.Generic(e)).To(BeTrue())
		})

		It("should return false on Generic event when namespace not found", func() {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = v1beta2.AddToScheme(scheme)
			fakeClient = fake.NewClientBuilder().WithScheme(scheme).Build()

			predicate, _ := util.NewNamespacePredicate(fakeClient, []string{}, "spark=enabled")

			obj := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: "non-existent",
				},
			}

			e := event.GenericEvent{Object: obj}
			Expect(predicate.Generic(e)).To(BeFalse())
		})
	})

	Context("Combined explicit namespaces and selector", func() {
		It("should match namespace in explicit list OR selector", func() {
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "team-a",
					Labels: map[string]string{"spark": "enabled"},
				},
			}
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = v1beta2.AddToScheme(scheme)
			fakeClient = fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns).Build()

			predicate, _ := util.NewNamespacePredicate(fakeClient, []string{"default"}, "spark=enabled")

			// Should match explicit namespace
			obj := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-app",
					Namespace: "default",
				},
			}
			e := event.CreateEvent{Object: obj}
			Expect(predicate.Create(e)).To(BeTrue())

			// Should match namespace via selector
			obj.Namespace = "team-a"
			e = event.CreateEvent{Object: obj}
			Expect(predicate.Create(e)).To(BeTrue())

			// Should NOT match neither
			ns2 := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "other",
					Labels: map[string]string{},
				},
			}
			fakeClient = fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns2).Build()
			predicate, _ = util.NewNamespacePredicate(fakeClient, []string{"default"}, "spark=enabled")

			obj.Namespace = "other"
			e = event.CreateEvent{Object: obj}
			Expect(predicate.Create(e)).To(BeFalse())
		})
	})
})
