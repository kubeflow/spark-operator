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
	"fmt"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

// createTestNamespaceWithSA creates a namespace with service account and required RBAC
func createTestNamespaceWithSA(ctx context.Context, name string, nsLabels map[string]string) (*corev1.Namespace, error) {
	// Create namespace
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: nsLabels,
		},
	}
	if err := k8sClient.Create(ctx, ns); err != nil {
		return nil, err
	}

	// Create spark service account
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-operator-spark",
			Namespace: name,
		},
	}
	if err := k8sClient.Create(ctx, sa); err != nil {
		return nil, err
	}

	// Create Role with required permissions for Spark jobs
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-operator-spark",
			Namespace: name,
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"pods", "services", "configmaps", "persistentvolumeclaims"},
				Verbs:     []string{"create", "get", "list", "watch", "delete", "update", "patch"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"secrets"},
				Verbs:     []string{"get"},
			},
		},
	}
	if err := k8sClient.Create(ctx, role); err != nil {
		return nil, err
	}

	// Create RoleBinding
	roleBinding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spark-operator-spark",
			Namespace: name,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      "spark-operator-spark",
				Namespace: name,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     "spark-operator-spark",
		},
	}
	if err := k8sClient.Create(ctx, roleBinding); err != nil {
		return nil, err
	}

	return ns, nil
}

// loadSparkPiExample loads the spark-pi example with custom namespace and name
func loadSparkPiExample(namespace, name string) (*v1beta2.SparkApplication, error) {
	path := filepath.Join("..", "..", "examples", "spark-pi.yaml")
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = file.Close()
	}()

	app := &v1beta2.SparkApplication{}
	decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
	if err := decoder.Decode(app); err != nil {
		return nil, err
	}

	app.Namespace = namespace
	app.Name = name
	return app, nil
}

var _ = Describe("Namespace Filtering", func() {
	// NOTE: These tests require the operator to be configured with
	// namespace selector. Update ci/ci-values.yaml:
	//   controller:
	//     namespaceSelector: "spark=enabled"

	Context("With namespace selector", func() {
		ctx := context.Background()

		var nsLabeled *corev1.Namespace
		var nsUnlabeled *corev1.Namespace
		var testID string

		BeforeEach(func() {
			// Generate TRULY unique suffix using current time
			testID = fmt.Sprintf("%d", time.Now().UnixNano())

			var err error

			// Create namespace WITH label, service account, and RBAC
			nsLabeled, err = createTestNamespaceWithSA(
				ctx,
				fmt.Sprintf("test-labeled-%s", testID),
				map[string]string{"spark": "enabled"},
			)
			Expect(err).NotTo(HaveOccurred())

			// Create namespace WITHOUT label but with service account and RBAC
			nsUnlabeled, err = createTestNamespaceWithSA(
				ctx,
				fmt.Sprintf("test-unlabeled-%s", testID),
				nil,
			)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			if nsLabeled != nil {
				_ = k8sClient.Delete(ctx, nsLabeled)
			}
			if nsUnlabeled != nil {
				_ = k8sClient.Delete(ctx, nsUnlabeled)
			}
		})

		It("should process SparkApp in labeled namespace", func() {
			app, err := loadSparkPiExample(
				nsLabeled.Name,
				fmt.Sprintf("test-labeled-app-%s", testID),
			)
			Expect(err).NotTo(HaveOccurred())

			By("Creating SparkApplication")
			Expect(k8sClient.Create(ctx, app)).To(Succeed())

			By("Waiting for completion")
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			Expect(waitForSparkApplicationCompleted(ctx, key)).NotTo(HaveOccurred())

			By("Cleaning up")
			Expect(k8sClient.Delete(ctx, app)).To(Succeed())
		})

		It("should NOT process SparkApp in unlabeled namespace", func() {
			app, err := loadSparkPiExample(
				nsUnlabeled.Name,
				fmt.Sprintf("test-unlabeled-app-%s", testID),
			)
			Expect(err).NotTo(HaveOccurred())

			By("Creating SparkApplication")
			Expect(k8sClient.Create(ctx, app)).To(Succeed())

			By("Verifying app stays in New state")
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}

			Consistently(func() v1beta2.ApplicationStateType {
				currentApp := &v1beta2.SparkApplication{}
				if err := k8sClient.Get(ctx, key, currentApp); err != nil {
					return ""
				}
				return currentApp.Status.AppState.State
			}).WithTimeout(30 * time.Second).WithPolling(2 * time.Second).Should(Equal(v1beta2.ApplicationStateNew))

			By("Cleaning up")
			Expect(k8sClient.Delete(ctx, app)).To(Succeed())
		})

		It("should NOT automatically process existing apps when namespace label is added", func() {
			// Create namespace without label
			nsDynamic, err := createTestNamespaceWithSA(
				ctx,
				fmt.Sprintf("test-dynamic-%s", testID),
				nil,
			)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = k8sClient.Delete(ctx, nsDynamic) }()

			app, err := loadSparkPiExample(nsDynamic.Name, fmt.Sprintf("test-dynamic-app-%s", testID))
			Expect(err).NotTo(HaveOccurred())

			By("Creating SparkApplication in unlabeled namespace")
			Expect(k8sClient.Create(ctx, app)).To(Succeed())

			By("Verifying app stays in New state")
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			time.Sleep(5 * time.Second)

			currentApp := &v1beta2.SparkApplication{}
			Expect(k8sClient.Get(ctx, key, currentApp)).To(Succeed())
			Expect(currentApp.Status.AppState.State).To(Equal(v1beta2.ApplicationStateNew))

			By("Adding label to namespace")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nsDynamic.Name}, nsDynamic)).To(Succeed())
			if nsDynamic.Labels == nil {
				nsDynamic.Labels = make(map[string]string)
			}
			nsDynamic.Labels["spark"] = "enabled"
			Expect(k8sClient.Update(ctx, nsDynamic)).To(Succeed())

			By("Verifying namespace label was added")
			Eventually(func() map[string]string {
				ns := &corev1.Namespace{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nsDynamic.Name}, ns)).To(Succeed())
				return ns.Labels
			}).WithTimeout(10 * time.Second).Should(HaveKeyWithValue("spark", "enabled"))

			By("Verifying existing app is NOT automatically reconciled (known limitation)")
			Consistently(func() v1beta2.ApplicationStateType {
				app := &v1beta2.SparkApplication{}
				if err := k8sClient.Get(ctx, key, app); err != nil {
					return ""
				}
				return app.Status.AppState.State
			}).WithTimeout(30*time.Second).WithPolling(2*time.Second).
				Should(Equal(v1beta2.ApplicationStateNew),
					"Existing apps should stay in New state until manually triggered")

			By("Cleaning up")
			Expect(k8sClient.Delete(ctx, app)).To(Succeed())
		})

		It("should process new apps created after namespace is labeled", func() {
			// Create namespace without label
			nsDynamic, err := createTestNamespaceWithSA(
				ctx,
				fmt.Sprintf("test-dynamic-%s", testID),
				nil,
			)
			Expect(err).NotTo(HaveOccurred())
			defer func() { _ = k8sClient.Delete(ctx, nsDynamic) }()

			By("Adding label to namespace FIRST")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nsDynamic.Name}, nsDynamic)).To(Succeed())
			if nsDynamic.Labels == nil {
				nsDynamic.Labels = make(map[string]string)
			}
			nsDynamic.Labels["spark"] = "enabled"
			Expect(k8sClient.Update(ctx, nsDynamic)).To(Succeed())

			By("Verifying namespace label was added")
			Eventually(func() map[string]string {
				ns := &corev1.Namespace{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: nsDynamic.Name}, ns)).To(Succeed())
				return ns.Labels
			}).WithTimeout(10 * time.Second).Should(HaveKeyWithValue("spark", "enabled"))

			By("Creating SparkApplication AFTER namespace is labeled")
			app, err := loadSparkPiExample(nsDynamic.Name, fmt.Sprintf("test-dynamic-app-%s", testID))
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Create(ctx, app)).To(Succeed())

			By("Verifying app gets reconciled immediately")
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			Expect(waitForSparkApplicationCompleted(ctx, key)).NotTo(HaveOccurred())

			By("Cleaning up")
			Expect(k8sClient.Delete(ctx, app)).To(Succeed())
		})
	})

	Context("Selector validation", func() {
		It("should reject invalid selector syntax", func() {
			By("Verifying invalid selectors are rejected")
			invalidSelectors := []struct {
				selector string
				reason   string
			}{
				{"=value", "missing key before ="},
				{"env in (prod,staging", "unbalanced parentheses - missing closing"},
				{"env in prod,staging)", "missing opening parenthesis"},
				{"key!value", "invalid operator (should be != or !key)"},
				{"env in (prod staging)", "missing comma between values"},
			}

			for _, tc := range invalidSelectors {
				_, err := labels.Parse(tc.selector)
				Expect(err).To(HaveOccurred(), "Selector %q should be invalid: %s", tc.selector, tc.reason)
			}

			By("Verifying valid selectors are accepted")
			validSelectors := []string{
				"spark=enabled",
				"spark",
				"!legacy",
				"env in (prod,staging)",
				"env notin (dev,test)",
				"spark=enabled,env=prod",
				"spark,env=prod",
				"env!=dev",
				"key=",       // Empty value is valid
				"key==value", // Valid! Matches key="=value"
			}

			for _, selector := range validSelectors {
				_, err := labels.Parse(selector)
				Expect(err).NotTo(HaveOccurred(), "Selector %q should be valid", selector)
			}
		})
	})
})
