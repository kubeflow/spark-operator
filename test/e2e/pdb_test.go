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
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// loadSparkPi parses the canonical spark-pi example and gives the caller a
// fresh copy. We rename it per-test so two tests can run in the same
// namespace without colliding.
func loadSparkPi(name string) *v1beta2.SparkApplication {
	app := &v1beta2.SparkApplication{}
	path := filepath.Join("..", "..", "examples", "spark-pi.yaml")
	file, err := os.Open(path)
	Expect(err).NotTo(HaveOccurred())
	defer func() {
		_ = file.Close()
	}()
	Expect(yaml.NewYAMLOrJSONDecoder(file, 100).Decode(app)).NotTo(HaveOccurred())
	app.Name = name
	app.ResourceVersion = ""
	app.UID = ""
	return app
}

func driverPDBKey(app *v1beta2.SparkApplication) types.NamespacedName {
	return types.NamespacedName{Name: util.GetDriverPodName(app), Namespace: app.Namespace}
}

var _ = Describe("Driver PodDisruptionBudget", func() {
	ctx := context.Background()

	Context("when the SparkApplication opts in", func() {
		var app *v1beta2.SparkApplication

		BeforeEach(func() {
			app = loadSparkPi("e2e-pdb-on")
			app.Spec.DriverPodDisruptionBudget = ptr.To(true)
			Expect(k8sClient.Create(ctx, app)).To(Succeed())
		})

		AfterEach(func() {
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			if err := k8sClient.Get(ctx, key, app); err == nil {
				_ = k8sClient.Delete(ctx, app)
			}
		})

		It("creates a PDB while the driver is running, and the app completes", func() {
			By("Waiting for the driver PDB to appear")
			pdb := &policyv1.PodDisruptionBudget{}
			Eventually(func() error {
				return k8sClient.Get(ctx, driverPDBKey(app), pdb)
			}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

			By("Verifying the PDB shape")
			Expect(pdb.Spec.MinAvailable).ToNot(BeNil())
			Expect(pdb.Spec.MinAvailable.IntValue()).To(Equal(1))
			Expect(pdb.Spec.Selector.MatchLabels[common.LabelSparkAppName]).To(Equal(app.Name))
			Expect(pdb.Spec.Selector.MatchLabels[common.LabelSparkRole]).To(Equal(common.SparkRoleDriver))
			Expect(pdb.OwnerReferences).ToNot(BeEmpty())
			Expect(pdb.OwnerReferences[0].Name).To(Equal(app.Name))
			Expect(pdb.OwnerReferences[0].Kind).To(Equal("SparkApplication"))

			By("Waiting for SparkApplication to complete")
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			Expect(waitForSparkApplicationCompleted(ctx, key)).NotTo(HaveOccurred())
		})

		It("garbage-collects the PDB when the SparkApplication is deleted", func() {
			By("Waiting for the driver PDB to appear")
			Eventually(func() error {
				pdb := &policyv1.PodDisruptionBudget{}
				return k8sClient.Get(ctx, driverPDBKey(app), pdb)
			}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

			By("Deleting the SparkApplication")
			Expect(k8sClient.Delete(ctx, app)).To(Succeed())

			By("Waiting for the PDB to be garbage-collected")
			Eventually(func() bool {
				pdb := &policyv1.PodDisruptionBudget{}
				err := k8sClient.Get(ctx, driverPDBKey(app), pdb)
				return apierrors.IsNotFound(err)
			}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(BeTrue())
		})
	})

	Context("when the SparkApplication does not opt in", func() {
		var app *v1beta2.SparkApplication

		AfterEach(func() {
			if app == nil {
				return
			}
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			if err := k8sClient.Get(ctx, key, app); err == nil {
				_ = k8sClient.Delete(ctx, app)
			}
		})

		It("does not create a PDB when the field is omitted, and the app completes", func() {
			app = loadSparkPi("e2e-pdb-omitted")
			Expect(k8sClient.Create(ctx, app)).To(Succeed())

			By("Waiting for SparkApplication to complete (proves no PDB was needed for normal lifecycle)")
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}
			Expect(waitForSparkApplicationCompleted(ctx, key)).NotTo(HaveOccurred())

			By("Confirming no PDB was ever created")
			pdb := &policyv1.PodDisruptionBudget{}
			err := k8sClient.Get(ctx, driverPDBKey(app), pdb)
			Expect(apierrors.IsNotFound(err)).To(BeTrue(), "expected NotFound but got: err=%v pdb=%+v", err, pdb)
		})
	})
})
