/*
Copyright 2025 The Kubeflow authors.

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

package sparkconnect

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

var _ = Describe("Options functions", func() {
	Context("imageOption", func() {
		It("handles nil executor template and falls back to spec.image", func() {
			image := "apache/spark:3.5.0"
			conn := &v1alpha1.SparkConnect{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-spark",
					Namespace: "default",
				},
				Spec: v1alpha1.SparkConnectSpec{
					SparkVersion: "3.5.0",
					Image:        &image,
					Server:       v1alpha1.ServerSpec{},
					Executor:     v1alpha1.ExecutorSpec{},
				},
			}

			args, err := imageOption(conn)
			Expect(err).NotTo(HaveOccurred())
			Expect(args).To(ContainElements(
				"--conf",
				SatisfyAll(
					ContainSubstring(common.SparkKubernetesContainerImage+"="+image),
				),
				"--conf",
				SatisfyAll(
					ContainSubstring(common.SparkKubernetesExecutorContainerImage+"="+image),
				),
			))
		})

		It("uses the first template container when the default executor container is absent", func() {
			image := "apache/spark:3.5.0"
			conn := &v1alpha1.SparkConnect{
				Spec: v1alpha1.SparkConnectSpec{
					Executor: v1alpha1.ExecutorSpec{
						SparkPodSpec: v1alpha1.SparkPodSpec{
							Template: &corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{{
										Name:  "executor",
										Image: image,
									}},
								},
							},
						},
					},
				},
			}

			args, err := imageOption(conn)
			Expect(err).NotTo(HaveOccurred())
			Expect(args).To(ContainElements(
				fmt.Sprintf("%s=%s", common.SparkKubernetesContainerImage, image),
				fmt.Sprintf("%s=%s", common.SparkKubernetesExecutorContainerImage, image),
			))
		})
	})

	Context("sparkConfOption", func() {
		It("preserves shell-sensitive Spark configuration values", func() {
			config := map[string]string{
				"spark.redaction.regex":          "(?i)secret|password|token|access[.]key|account[.]key",
				"spark.driver.extraJavaOptions":  `-Dmessage="hello world" -Dquote='value'`,
				"spark.example.shell_expression": "$HOME $(printf injected) `printf injected` ; & |",
				"spark.example.multiline":        "first line\nsecond line",
				"spark.example.empty":            "",
			}
			conn := &v1alpha1.SparkConnect{
				Spec: v1alpha1.SparkConnectSpec{SparkConf: config},
			}

			args, err := sparkConfOption(conn)
			Expect(err).NotTo(HaveOccurred())
			Expect(shellParsedSparkConfig(args)).To(Equal(config))
		})
	})

	Context("hadoopConfOption", func() {
		It("preserves shell-sensitive Hadoop configuration values", func() {
			conn := &v1alpha1.SparkConnect{
				Spec: v1alpha1.SparkConnectSpec{
					HadoopConf: map[string]string{
						"fs.example.regex":              "(?i)secret|password",
						"spark.hadoop.fs.example.value": "literal '$HOME' $(printf injected)",
					},
				},
			}

			args, err := hadoopConfOption(conn)
			Expect(err).NotTo(HaveOccurred())
			Expect(shellParsedSparkConfig(args)).To(Equal(map[string]string{
				"spark.hadoop.fs.example.regex": "(?i)secret|password",
				"spark.hadoop.fs.example.value": "literal '$HOME' $(printf injected)",
			}))
		})
	})

	Context("dependenciesOption", func() {
		It("emits spark-submit dependency flags", func() {
			conn := &v1alpha1.SparkConnect{
				Spec: v1alpha1.SparkConnectSpec{
					Dependencies: v1alpha1.Dependencies{
						Jars:            []string{"local:///opt/spark/jars/iceberg.jar"},
						Packages:        []string{"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0"},
						ExcludePackages: []string{"org.slf4j:slf4j-log4j12"},
						Repositories:    []string{"https://repo1.maven.org/maven2"},
						PyFiles:         []string{"local:///opt/spark/pyfiles/utils.zip"},
						Files:           []string{"local:///opt/spark/files/config.json"},
						Archives:        []string{"local:///opt/spark/archives/data.zip"},
					},
				},
			}

			args, err := dependenciesOption(conn)
			Expect(err).NotTo(HaveOccurred())
			Expect(args).To(Equal([]string{
				"--jars", "local:///opt/spark/jars/iceberg.jar",
				"--packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
				"--exclude-packages", "org.slf4j:slf4j-log4j12",
				"--repositories", "https://repo1.maven.org/maven2",
				"--py-files", "local:///opt/spark/pyfiles/utils.zip",
				"--files", "local:///opt/spark/files/config.json",
				"--archives", "local:///opt/spark/archives/data.zip",
			}))
		})
	})
})

func shellParsedSparkConfig(args []string) map[string]string {
	GinkgoHelper()

	output, err := exec.Command("bash", "-c", "printf '%s\\0' "+strings.Join(args, " ")).Output()
	Expect(err).NotTo(HaveOccurred())

	fields := bytes.Split(bytes.TrimSuffix(output, []byte{0}), []byte{0})
	Expect(len(fields) % 2).To(Equal(0))

	config := make(map[string]string, len(fields)/2)
	for index := 0; index < len(fields); index += 2 {
		Expect(string(fields[index])).To(Equal("--conf"))
		key, value, found := strings.Cut(string(fields[index+1]), "=")
		Expect(found).To(BeTrue())
		config[key] = value
	}
	return config
}
