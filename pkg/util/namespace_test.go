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
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

var _ = Describe("NamespaceMatcher", func() {
	Context("NewNamespaceMatcher", func() {
		It("should create matcher with explicit namespaces", func() {
			matcher, err := util.NewNamespaceMatcher(
				[]string{"default", "prod"},
				"",
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(matcher).NotTo(BeNil())
		})

		It("should create matcher with empty namespaces (NamespaceAll)", func() {
			matcher, err := util.NewNamespaceMatcher([]string{}, "")
			Expect(err).NotTo(HaveOccurred())
			Expect(matcher).NotTo(BeNil())
		})

		It("should create matcher with selector only", func() {
			matcher, err := util.NewNamespaceMatcher([]string{}, "spark=enabled")
			Expect(err).NotTo(HaveOccurred())
			Expect(matcher).NotTo(BeNil())
		})

		It("should create matcher with both explicit namespaces and selector", func() {
			matcher, err := util.NewNamespaceMatcher(
				[]string{"default", "prod"},
				"spark=enabled",
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(matcher).NotTo(BeNil())
		})

		It("should reject invalid selector syntax", func() {
			_, err := util.NewNamespaceMatcher(
				[]string{},
				"=value",
			)
			Expect(err).To(HaveOccurred())
		})

		It("should reject invalid selector with unbalanced parentheses", func() {
			_, err := util.NewNamespaceMatcher(
				[]string{},
				"env in (prod,staging",
			)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("Matches", func() {
		It("should match explicit namespace", func() {
			matcher, _ := util.NewNamespaceMatcher(
				[]string{"default"},
				"",
			)

			Expect(matcher.Matches("default", nil)).To(BeTrue())
			Expect(matcher.Matches("other", nil)).To(BeFalse())
		})

		It("should match all namespaces when namespaces list is empty", func() {
			matcher, _ := util.NewNamespaceMatcher([]string{}, "")
			Expect(matcher.Matches("any-namespace", nil)).To(BeTrue())
			Expect(matcher.Matches("another-namespace", nil)).To(BeTrue())
			Expect(matcher.Matches("", nil)).To(BeTrue())
		})

		It("should match namespace with single label selector", func() {
			matcher, _ := util.NewNamespaceMatcher(
				[]string{},
				"spark=enabled",
			)

			labels := map[string]string{"spark": "enabled"}
			Expect(matcher.Matches("team-a", labels)).To(BeTrue())

			labels = map[string]string{"env": "prod"}
			Expect(matcher.Matches("team-b", labels)).To(BeFalse())

			labels = map[string]string{}
			Expect(matcher.Matches("team-c", labels)).To(BeFalse())
		})

		It("should match namespace with multiple label selectors (AND)", func() {
			matcher, _ := util.NewNamespaceMatcher(
				[]string{},
				"spark=enabled,env=prod",
			)

			labels := map[string]string{"spark": "enabled", "env": "prod"}
			Expect(matcher.Matches("prod-ns", labels)).To(BeTrue())

			labels = map[string]string{"spark": "enabled"}
			Expect(matcher.Matches("dev-ns", labels)).To(BeFalse())

			labels = map[string]string{"env": "prod"}
			Expect(matcher.Matches("prod-ns-2", labels)).To(BeFalse())
		})

		It("should match namespace with 'in' operator", func() {
			matcher, _ := util.NewNamespaceMatcher(
				[]string{},
				"env in (prod,staging)",
			)

			labels := map[string]string{"env": "prod"}
			Expect(matcher.Matches("ns1", labels)).To(BeTrue())

			labels = map[string]string{"env": "staging"}
			Expect(matcher.Matches("ns2", labels)).To(BeTrue())

			labels = map[string]string{"env": "dev"}
			Expect(matcher.Matches("ns3", labels)).To(BeFalse())
		})

		It("should match namespace with 'notin' operator", func() {
			matcher, _ := util.NewNamespaceMatcher(
				[]string{},
				"env notin (dev,test)",
			)

			labels := map[string]string{"env": "prod"}
			Expect(matcher.Matches("ns1", labels)).To(BeTrue())

			labels = map[string]string{"env": "dev"}
			Expect(matcher.Matches("ns2", labels)).To(BeFalse())

			labels = map[string]string{"env": "test"}
			Expect(matcher.Matches("ns3", labels)).To(BeFalse())
		})

		It("should match namespace with combined explicit namespaces and selector", func() {
			matcher, _ := util.NewNamespaceMatcher(
				[]string{"default", "prod"},
				"spark=enabled",
			)

			// Should match explicit namespace
			Expect(matcher.Matches("default", nil)).To(BeTrue())
			Expect(matcher.Matches("prod", nil)).To(BeTrue())

			// Should match namespace via selector
			labels := map[string]string{"spark": "enabled"}
			Expect(matcher.Matches("team-a", labels)).To(BeTrue())

			// Should NOT match neither explicit nor selector
			labels = map[string]string{"env": "dev"}
			Expect(matcher.Matches("other", labels)).To(BeFalse())
		})

		It("should prioritize explicit namespace over selector when both match", func() {
			matcher, _ := util.NewNamespaceMatcher(
				[]string{"default"},
				"spark=enabled",
			)

			// Explicit namespace should match even without labels
			Expect(matcher.Matches("default", nil)).To(BeTrue())

			// Explicit namespace should match even with wrong labels
			labels := map[string]string{"env": "dev"}
			Expect(matcher.Matches("default", labels)).To(BeTrue())
		})

		It("should handle nil labels map", func() {
			matcher, _ := util.NewNamespaceMatcher(
				[]string{},
				"spark=enabled",
			)

			Expect(matcher.Matches("namespace", nil)).To(BeFalse())
		})

		It("should handle empty namespace name", func() {
			matcher, _ := util.NewNamespaceMatcher(
				[]string{"default"},
				"",
			)

			Expect(matcher.Matches("", nil)).To(BeFalse())
		})

		It("should handle empty namespace name with NamespaceAll", func() {
			matcher, _ := util.NewNamespaceMatcher([]string{}, "")
			Expect(matcher.Matches("", nil)).To(BeTrue())
		})

		It("should match namespace with existence operator (label exists)", func() {
			matcher, _ := util.NewNamespaceMatcher(
				[]string{},
				"spark",
			)

			// Namespace with spark label (any value) should match
			labels := map[string]string{"spark": "enabled"}
			Expect(matcher.Matches("ns1", labels)).To(BeTrue())

			labels = map[string]string{"spark": ""}
			Expect(matcher.Matches("ns2", labels)).To(BeTrue())

			labels = map[string]string{"spark": "any-value"}
			Expect(matcher.Matches("ns3", labels)).To(BeTrue())

			// Namespace without spark label should NOT match
			labels = map[string]string{"env": "prod"}
			Expect(matcher.Matches("ns4", labels)).To(BeFalse())

			// Namespace with no labels should NOT match
			Expect(matcher.Matches("ns5", nil)).To(BeFalse())
		})

		It("should match namespace with DoesNotExist operator (!label)", func() {
			matcher, _ := util.NewNamespaceMatcher(
				[]string{},
				"!legacy",
			)

			// Namespace without legacy label should match
			labels := map[string]string{"spark": "enabled"}
			Expect(matcher.Matches("ns1", labels)).To(BeTrue())

			// Namespace with empty labels should match
			labels = map[string]string{}
			Expect(matcher.Matches("ns2", labels)).To(BeTrue())

			// Namespace with nil labels should match
			Expect(matcher.Matches("ns3", nil)).To(BeTrue())

			// Namespace with legacy label should NOT match
			labels = map[string]string{"legacy": "true"}
			Expect(matcher.Matches("ns4", labels)).To(BeFalse())
		})

		It("should match namespace with complex combined selectors", func() {
			// Combination of equality and set-based selectors
			matcher, _ := util.NewNamespaceMatcher(
				[]string{},
				"spark=enabled,env in (prod,staging),!legacy",
			)

			// All conditions met
			labels := map[string]string{"spark": "enabled", "env": "prod"}
			Expect(matcher.Matches("ns1", labels)).To(BeTrue())

			labels = map[string]string{"spark": "enabled", "env": "staging", "team": "data"}
			Expect(matcher.Matches("ns2", labels)).To(BeTrue())

			// Missing spark label
			labels = map[string]string{"env": "prod"}
			Expect(matcher.Matches("ns3", labels)).To(BeFalse())

			// Wrong spark value
			labels = map[string]string{"spark": "disabled", "env": "prod"}
			Expect(matcher.Matches("ns4", labels)).To(BeFalse())

			// Wrong env value
			labels = map[string]string{"spark": "enabled", "env": "dev"}
			Expect(matcher.Matches("ns5", labels)).To(BeFalse())

			// Has legacy label (should NOT match)
			labels = map[string]string{"spark": "enabled", "env": "prod", "legacy": "true"}
			Expect(matcher.Matches("ns6", labels)).To(BeFalse())
		})

		It("should match namespace with empty label value", func() {
			matcher, _ := util.NewNamespaceMatcher(
				[]string{},
				"spark=",
			)

			// Namespace with empty spark value should match
			labels := map[string]string{"spark": ""}
			Expect(matcher.Matches("ns1", labels)).To(BeTrue())

			// Namespace with non-empty spark value should NOT match
			labels = map[string]string{"spark": "enabled"}
			Expect(matcher.Matches("ns2", labels)).To(BeFalse())

			// Namespace without spark label should NOT match
			labels = map[string]string{"env": "prod"}
			Expect(matcher.Matches("ns3", labels)).To(BeFalse())
		})

		It("should match namespace with existence and equality combined", func() {
			matcher, _ := util.NewNamespaceMatcher(
				[]string{},
				"spark,env=prod",
			)

			// Both conditions met
			labels := map[string]string{"spark": "enabled", "env": "prod"}
			Expect(matcher.Matches("ns1", labels)).To(BeTrue())

			labels = map[string]string{"spark": "", "env": "prod"}
			Expect(matcher.Matches("ns2", labels)).To(BeTrue())

			// Only spark exists
			labels = map[string]string{"spark": "enabled", "env": "dev"}
			Expect(matcher.Matches("ns3", labels)).To(BeFalse())

			// Only env=prod
			labels = map[string]string{"env": "prod"}
			Expect(matcher.Matches("ns4", labels)).To(BeFalse())
		})

		It("should match namespace with NotEquals operator (key!=value)", func() {
			matcher, _ := util.NewNamespaceMatcher(
				[]string{},
				"env!=dev",
			)

			// Namespace with env=prod should match
			labels := map[string]string{"env": "prod"}
			Expect(matcher.Matches("ns1", labels)).To(BeTrue())

			// Namespace without env label should match
			labels = map[string]string{"spark": "enabled"}
			Expect(matcher.Matches("ns2", labels)).To(BeTrue())

			// Namespace with env=dev should NOT match
			labels = map[string]string{"env": "dev"}
			Expect(matcher.Matches("ns3", labels)).To(BeFalse())
		})
	})

	Context("MatchesNamespace", func() {
		It("should match namespace object with explicit namespace", func() {
			matcher, _ := util.NewNamespaceMatcher(
				[]string{"default"},
				"",
			)

			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "default",
					Labels: map[string]string{"env": "prod"},
				},
			}
			Expect(matcher.MatchesNamespace(ns)).To(BeTrue())

			ns = &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "other",
					Labels: map[string]string{"env": "prod"},
				},
			}
			Expect(matcher.MatchesNamespace(ns)).To(BeFalse())
		})

		It("should match namespace object with selector", func() {
			matcher, _ := util.NewNamespaceMatcher(
				[]string{},
				"spark=enabled",
			)

			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "team-a",
					Labels: map[string]string{"spark": "enabled"},
				},
			}
			Expect(matcher.MatchesNamespace(ns)).To(BeTrue())

			ns = &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "team-b",
					Labels: map[string]string{"env": "prod"},
				},
			}
			Expect(matcher.MatchesNamespace(ns)).To(BeFalse())
		})

		It("should match namespace object with combined explicit and selector", func() {
			matcher, _ := util.NewNamespaceMatcher(
				[]string{"default"},
				"spark=enabled",
			)

			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "default",
					Labels: map[string]string{},
				},
			}
			Expect(matcher.MatchesNamespace(ns)).To(BeTrue())

			ns = &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "team-a",
					Labels: map[string]string{"spark": "enabled"},
				},
			}
			Expect(matcher.MatchesNamespace(ns)).To(BeTrue())

			ns = &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "team-b",
					Labels: map[string]string{},
				},
			}
			Expect(matcher.MatchesNamespace(ns)).To(BeFalse())
		})
	})

	Context("MatchesWithClient", func() {
		var fakeClient client.Client
		var ctx context.Context

		BeforeEach(func() {
			ctx = context.Background()
		})

		It("should use fast-path when no selector is configured", func() {
			matcher, _ := util.NewNamespaceMatcher(
				[]string{"default"},
				"",
			)

			fakeClient = fake.NewClientBuilder().Build()

			matched, err := matcher.MatchesWithClient(ctx, fakeClient, "default")
			Expect(err).NotTo(HaveOccurred())
			Expect(matched).To(BeTrue())

			matched, err = matcher.MatchesWithClient(ctx, fakeClient, "other")
			Expect(err).NotTo(HaveOccurred())
			Expect(matched).To(BeFalse())
		})

		It("should retrieve namespace from client when selector is configured", func() {
			matcher, _ := util.NewNamespaceMatcher(
				[]string{},
				"spark=enabled",
			)

			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "team-a",
					Labels: map[string]string{"spark": "enabled"},
				},
			}
			fakeClient = fake.NewClientBuilder().WithObjects(ns).Build()

			matched, err := matcher.MatchesWithClient(ctx, fakeClient, "team-a")
			Expect(err).NotTo(HaveOccurred())
			Expect(matched).To(BeTrue())

			ns2 := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "team-b",
					Labels: map[string]string{"env": "prod"},
				},
			}
			fakeClient = fake.NewClientBuilder().WithObjects(ns2).Build()

			matched, err = matcher.MatchesWithClient(ctx, fakeClient, "team-b")
			Expect(err).NotTo(HaveOccurred())
			Expect(matched).To(BeFalse())
		})

		It("should return error when namespace not found", func() {
			matcher, _ := util.NewNamespaceMatcher(
				[]string{},
				"spark=enabled",
			)

			fakeClient = fake.NewClientBuilder().Build()

			matched, err := matcher.MatchesWithClient(ctx, fakeClient, "non-existent")
			Expect(err).To(HaveOccurred())
			Expect(matched).To(BeFalse())
		})

		It("should handle combined explicit namespaces and selector", func() {
			matcher, _ := util.NewNamespaceMatcher(
				[]string{"default"},
				"spark=enabled",
			)

			// Fast-path should work for explicit namespace
			fakeClient = fake.NewClientBuilder().Build()
			matched, err := matcher.MatchesWithClient(ctx, fakeClient, "default")
			Expect(err).NotTo(HaveOccurred())
			Expect(matched).To(BeTrue())

			// Should retrieve namespace for selector check
			ns := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "team-a",
					Labels: map[string]string{"spark": "enabled"},
				},
			}
			fakeClient = fake.NewClientBuilder().WithObjects(ns).Build()

			matched, err = matcher.MatchesWithClient(ctx, fakeClient, "team-a")
			Expect(err).NotTo(HaveOccurred())
			Expect(matched).To(BeTrue())
		})
	})
})
