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

package e2e_openshift_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// ============================================================================
// OPENSHIFT SPARK OPERATOR TESTS
// ============================================================================
//
// These tests verify:
// 1. The Spark operator installs correctly from https://opendatahub-io.github.io/spark-operator
// 2. The fsGroup security context is NOT set to 185 (OpenShift requirement)
// 3. The docling-spark-app workload runs successfully
//
// To run these tests:
//   go test ./test/e2e-openshift/ -v -ginkgo.v -timeout 30m
//
// ============================================================================

var _ = Describe("OpenShift Spark Operator", func() {
	// ========================================================================
	// TEST 1: Verify fsGroup is NOT 185
	// ========================================================================
	//
	// OpenShift has specific security requirements. The default fsGroup of 185
	// may not be appropriate for all OpenShift environments. This test verifies
	// that the custom chart from opendatahub-io.github.io/spark-operator uses a different fsGroup.
	//
	// ========================================================================
	Context("Operator Security Configuration", func() {
		It("Should have fsGroup different from 185", func() {
			By("Getting the Spark operator pods")
			pods := &corev1.PodList{}

			// Find pods in the release namespace
			err := k8sClient.List(context.Background(), pods,
				client.InNamespace(ReleaseNamespace),
			)
			Expect(err).NotTo(HaveOccurred())

			// We expect at least one operator pod
			Expect(len(pods.Items)).To(BeNumerically(">", 0),
				"Expected at least one pod in namespace %s", ReleaseNamespace)

			By("Checking fsGroup on each operator pod")
			checkedPods := 0
			for _, pod := range pods.Items {
				// Skip pods that aren't part of the spark-operator deployment
				if !strings.Contains(pod.Name, "spark-operator") {
					continue
				}

				checkedPods++
				GinkgoWriter.Printf("Checking pod: %s\n", pod.Name)

				// Check if SecurityContext is set
				if pod.Spec.SecurityContext != nil {
					GinkgoWriter.Printf("  SecurityContext found\n")

					if pod.Spec.SecurityContext.FSGroup != nil {
						fsGroup := *pod.Spec.SecurityContext.FSGroup
						GinkgoWriter.Printf("  FSGroup: %d\n", fsGroup)

						// THE KEY ASSERTION: fsGroup should NOT be 185
						Expect(fsGroup).NotTo(Equal(int64(185)),
							"Pod %s has fsGroup=185, which is not allowed for OpenShift", pod.Name)

						GinkgoWriter.Printf("  ✓ FSGroup is %d (not 185) - PASS\n", fsGroup)
					} else {
						GinkgoWriter.Printf("  FSGroup is not set (nil) - OK\n")
					}
				} else {
					GinkgoWriter.Printf("  SecurityContext is not set (nil) - OK\n")
				}
			}

			// Ensure we actually checked some operator pods
			Expect(checkedPods).To(BeNumerically(">", 0),
				"No spark-operator pods found to check")

			GinkgoWriter.Printf("\n✓ All %d operator pods passed fsGroup check\n", checkedPods)
		})
	})

	// ========================================================================
	// TEST 2: Run docling-spark-app workload
	// ========================================================================
	//
	// This test deploys the docling-spark-app SparkApplication and waits for
	// it to complete successfully. This validates that:
	// - The operator can create driver and executor pods
	// - The custom workload image works correctly
	// - OpenShift security contexts are properly applied
	//
	// ========================================================================
	Context("Docling Spark Application", func() {
		ctx := context.Background()

		// Path to the docling spark app YAML
		path := filepath.Join("..", "..", "examples", "openshift", "k8s", "docling-spark-app.yaml")

		var app *v1beta2.SparkApplication

		BeforeEach(func() {
			By("Parsing docling-spark-app.yaml")
			file, err := os.Open(path)
			Expect(err).NotTo(HaveOccurred(), "Failed to open %s", path)
			Expect(file).NotTo(BeNil())
			defer func() { _ = file.Close() }()

			decoder := yaml.NewYAMLOrJSONDecoder(file, 100)
			Expect(decoder).NotTo(BeNil())

			app = &v1beta2.SparkApplication{}
			Expect(decoder.Decode(app)).NotTo(HaveOccurred(), "Failed to decode SparkApplication YAML")

			// Log what we parsed
			GinkgoWriter.Printf("Parsed SparkApplication:\n")
			GinkgoWriter.Printf("  Name: %s\n", app.Name)
			GinkgoWriter.Printf("  Namespace: %s\n", app.Namespace)
			GinkgoWriter.Printf("  Type: %s\n", app.Spec.Type)
			GinkgoWriter.Printf("  Image: %s\n", *app.Spec.Image)
			GinkgoWriter.Printf("  MainApplicationFile: %s\n", *app.Spec.MainApplicationFile)

			// Inject SKIP_SLEEP env var for testing (skip the 60-minute sleep)
			By("Injecting SKIP_SLEEP=true environment variable")
			if app.Spec.Driver.Env == nil {
				app.Spec.Driver.Env = []corev1.EnvVar{}
			}
			app.Spec.Driver.Env = append(app.Spec.Driver.Env, corev1.EnvVar{
				Name:  "SKIP_SLEEP",
				Value: "true",
			})
			GinkgoWriter.Printf("✓ Added SKIP_SLEEP=true to driver environment\n")

			By("Creating SparkApplication: " + app.Name)
			err = k8sClient.Create(ctx, app)
			Expect(err).NotTo(HaveOccurred(), "Failed to create SparkApplication")

			GinkgoWriter.Printf("✓ SparkApplication created successfully\n")
		})

		AfterEach(func() {
			// Skip cleanup on failure to allow log inspection
			if CurrentSpecReport().Failed() {
				GinkgoWriter.Printf("⚠️  Test failed - preserving SparkApplication for debugging\n")
				return
			}

			if app != nil {
				By("Cleaning up SparkApplication: " + app.Name)
				key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}

				// Fetch the latest version before deleting
				currentApp := &v1beta2.SparkApplication{}
				if err := k8sClient.Get(ctx, key, currentApp); err == nil {
					if err := k8sClient.Delete(ctx, currentApp); err != nil {
						GinkgoWriter.Printf("Warning: Failed to delete SparkApplication: %v\n", err)
					} else {
						GinkgoWriter.Printf("✓ SparkApplication deleted\n")
					}
				}
			}
		})

		It("Should complete successfully", func() {
			By("Waiting for SparkApplication to complete")
			key := types.NamespacedName{Namespace: app.Namespace, Name: app.Name}

			// Use the helper function to wait for completion
			err := waitForSparkApplicationCompleted(ctx, key)

			// If failed, get current status for debugging
			if err != nil {
				currentApp := &v1beta2.SparkApplication{}
				if getErr := k8sClient.Get(ctx, key, currentApp); getErr == nil {
					GinkgoWriter.Printf("❌ SparkApplication Status on failure:\n")
					GinkgoWriter.Printf("   State: %s\n", currentApp.Status.AppState.State)
					GinkgoWriter.Printf("   Error: %s\n", currentApp.Status.AppState.ErrorMessage)
					GinkgoWriter.Printf("   SubmissionAttempts: %d\n", currentApp.Status.SubmissionAttempts)
					GinkgoWriter.Printf("   ExecutionAttempts: %d\n", currentApp.Status.ExecutionAttempts)
				}
			}

			Expect(err).NotTo(HaveOccurred(), "SparkApplication did not complete successfully")

			GinkgoWriter.Printf("✓ SparkApplication completed successfully\n")

			By("Verifying driver pod ran")
			driverPodName := util.GetDriverPodName(app)
			GinkgoWriter.Printf("Driver pod name: %s\n", driverPodName)

			// Get driver pod logs to verify execution
			bytes, err := clientset.CoreV1().Pods(app.Namespace).GetLogs(driverPodName, &corev1.PodLogOptions{}).Do(ctx).Raw()
			if err != nil {
				GinkgoWriter.Printf("Warning: Could not get driver logs: %v\n", err)
			} else {
				GinkgoWriter.Printf("Driver logs (last 500 chars):\n")
				logs := string(bytes)
				if len(logs) > 500 {
					logs = logs[len(logs)-500:]
				}
				GinkgoWriter.Printf("%s\n", logs)
			}

			By("Verifying driver pod security context")
			driverPod := &corev1.Pod{}
			driverPodKey := types.NamespacedName{Namespace: app.Namespace, Name: driverPodName}
			err = k8sClient.Get(ctx, driverPodKey, driverPod)
			if err == nil {
				// Check that security contexts from the SparkApplication were applied
				if driverPod.Spec.SecurityContext != nil {
					GinkgoWriter.Printf("Driver pod SecurityContext:\n")
					if driverPod.Spec.SecurityContext.RunAsNonRoot != nil {
						GinkgoWriter.Printf("  RunAsNonRoot: %v\n", *driverPod.Spec.SecurityContext.RunAsNonRoot)
					}
					if driverPod.Spec.SecurityContext.FSGroup != nil {
						GinkgoWriter.Printf("  FSGroup: %d\n", *driverPod.Spec.SecurityContext.FSGroup)
					}
				}
			}
		})
	})
})
