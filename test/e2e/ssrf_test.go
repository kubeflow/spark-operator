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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

// The e2e deployments enable URL-scheme validation through Helm CI values and the
// Kustomize e2e overlay. Dry-run reaches admission without persisting an app or
// running spark-submit, so these tests prove validation wiring, not runtime egress denial.
var _ = Describe("SSRF admission", func() {
	ctx := context.Background()

	newApp := func(name, mainApplicationFile string) *v1beta2.SparkApplication {
		return &v1beta2.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "default",
			},
			Spec: v1beta2.SparkApplicationSpec{
				Type:                v1beta2.SparkApplicationTypeScala,
				SparkVersion:        "3.5.0",
				Mode:                v1beta2.DeployModeCluster,
				MainApplicationFile: &mainApplicationFile,
				Driver: v1beta2.DriverSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Cores:  ptr.To[int32](1),
						Memory: ptr.To("512m"),
					},
				},
				Executor: v1beta2.ExecutorSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Cores:  ptr.To[int32](1),
						Memory: ptr.To("512m"),
					},
					Instances: ptr.To[int32](1),
				},
			},
		}
	}

	Context("URL scheme and host policy", func() {
		DescribeTable("rejects a disallowed HTTP URL in each submit-time field family",
			func(name, expectedField string, configure func(*v1beta2.SparkApplication)) {
				app := newApp(name, "local:///app.py")
				configure(app)

				err := k8sClient.Create(ctx, app, &client.CreateOptions{DryRun: []string{metav1.DryRunAll}})
				Expect(err).To(HaveOccurred())
				Expect(apierrors.IsInvalid(err) || apierrors.IsForbidden(err)).To(BeTrue(), "expected Invalid or Forbidden, got: %v", err)
				Expect(err.Error()).To(ContainSubstring(expectedField))
				Expect(err.Error()).To(ContainSubstring("http"))
				Expect(err.Error()).To(ContainSubstring("not in the allowed list"))
			},
			Entry("main application file", "ssrf-main-file", "spec.mainApplicationFile", func(app *v1beta2.SparkApplication) {
				mainApplicationFile := "http://attacker.example.com/app.py"
				app.Spec.MainApplicationFile = &mainApplicationFile
			}),
			Entry("dependency repository", "ssrf-repository", "spec.deps.repositories", func(app *v1beta2.SparkApplication) {
				app.Spec.Deps.Repositories = []string{"http://attacker.example.com/maven"}
			}),
			Entry("Spark configuration", "ssrf-spark-conf", `spec.sparkConf["spark.jars"]`, func(app *v1beta2.SparkApplication) {
				app.Spec.SparkConf = map[string]string{"spark.jars": "http://attacker.example.com/evil.jar"}
			}),
		)

		It("accepts local URLs and an artifact upload destination", func() {
			app := newApp("ssrf-local-url", "local:///app.py")
			app.Spec.SparkConf = map[string]string{
				"spark.jars":                        "file:///opt/spark/jars/app.jar",
				"spark.kubernetes.file.upload.path": "s3a://artifact-bucket/submissions",
			}

			Expect(k8sClient.Create(ctx, app, &client.CreateOptions{DryRun: []string{metav1.DryRunAll}})).To(Succeed())
		})

		It("accepts a remote URL with an allowed scheme-qualified host", func() {
			app := newApp("ssrf-allowed-host", "https://test1.example.com/app.py")

			Expect(k8sClient.Create(ctx, app, &client.CreateOptions{DryRun: []string{metav1.DryRunAll}})).To(Succeed())
		})

		It("rejects a remote URL with an allowed scheme but a different host", func() {
			app := newApp("ssrf-disallowed-host", "https://attacker.example.com/app.py")

			err := k8sClient.Create(ctx, app, &client.CreateOptions{DryRun: []string{metav1.DryRunAll}})
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsInvalid(err) || apierrors.IsForbidden(err)).To(BeTrue(), "expected Invalid or Forbidden, got: %v", err)
			Expect(err.Error()).To(ContainSubstring("spec.mainApplicationFile"))
			Expect(err.Error()).To(ContainSubstring("attacker.example.com"))
			Expect(err.Error()).To(ContainSubstring("not in the allowed list"))
		})

		DescribeTable("rejects authority-bearing local URL forms",
			func(name, mainApplicationFile string) {
				app := newApp(name, mainApplicationFile)

				err := k8sClient.Create(ctx, app, &client.CreateOptions{DryRun: []string{metav1.DryRunAll}})
				Expect(err).To(HaveOccurred())
				Expect(apierrors.IsInvalid(err) || apierrors.IsForbidden(err)).To(BeTrue(), "expected Invalid or Forbidden, got: %v", err)
				Expect(err.Error()).To(ContainSubstring("spec.mainApplicationFile"))
				Expect(err.Error()).To(ContainSubstring("not in the allowed list"))
			},
			Entry("file URL", "ssrf-file-host", "file://attacker.example.com/app.py"),
			Entry("local URL", "ssrf-local-host", "local://attacker.example.com/app.py"),
		)

		It("rejects a disallowed URL in a scheduled application template", func() {
			app := newApp("ssrf-scheduled", "local:///app.py")
			mainApplicationFile := "http://attacker.example.com/app.py"
			app.Spec.MainApplicationFile = &mainApplicationFile
			scheduledApp := &v1beta2.ScheduledSparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ssrf-scheduled",
					Namespace: "default",
				},
				Spec: v1beta2.ScheduledSparkApplicationSpec{
					Schedule: "@every 1h",
					Template: app.Spec,
				},
			}

			err := k8sClient.Create(ctx, scheduledApp, &client.CreateOptions{DryRun: []string{metav1.DryRunAll}})
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsInvalid(err) || apierrors.IsForbidden(err)).To(BeTrue(), "expected Invalid or Forbidden, got: %v", err)
			Expect(err.Error()).To(ContainSubstring("spec.template.mainApplicationFile"))
			Expect(err.Error()).To(ContainSubstring("http"))
			Expect(err.Error()).To(ContainSubstring("not in the allowed list"))
		})
	})
})
