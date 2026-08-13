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
	"os/exec"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

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

	Context("driverConfOption and executorConfOption with CPU resources", func() {
		It("includes CoreRequest and CoreLimit in driver configuration", func() {
			cores := int32(4)
			coreRequest := "3500m"
			coreLimit := "4"
			conn := &v1alpha1.SparkConnect{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-spark",
					Namespace: "default",
				},
				Spec: v1alpha1.SparkConnectSpec{
					SparkVersion: "3.5.0",
					Server: v1alpha1.ServerSpec{
						SparkPodSpec: v1alpha1.SparkPodSpec{
							Cores:       &cores,
							CoreRequest: &coreRequest,
							CoreLimit:   &coreLimit,
						},
					},
					Executor: v1alpha1.ExecutorSpec{},
				},
			}

			args, err := driverConfOption(conn)
			Expect(err).NotTo(HaveOccurred())
			config := shellParsedSparkConfig(args)

			// Verify Cores maps to spark.driver.cores (not affected by CoreRequest/CoreLimit)
			Expect(config).To(HaveKeyWithValue("spark.driver.cores", "4"))

			// Verify CoreRequest maps to physical CPU request
			Expect(config).To(HaveKeyWithValue(common.SparkKubernetesDriverRequestCores, "3500m"))

			// Verify CoreLimit maps to physical CPU limit
			Expect(config).To(HaveKeyWithValue(common.SparkKubernetesDriverLimitCores, "4"))
		})

		It("includes CoreRequest and CoreLimit in executor configuration", func() {
			cores := int32(4)
			instances := int32(2)
			coreRequest := "3500m"
			coreLimit := "4"
			conn := &v1alpha1.SparkConnect{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-spark",
					Namespace: "default",
				},
				Spec: v1alpha1.SparkConnectSpec{
					SparkVersion: "3.5.0",
					Server:       v1alpha1.ServerSpec{},
					Executor: v1alpha1.ExecutorSpec{
						SparkPodSpec: v1alpha1.SparkPodSpec{
							Cores:       &cores,
							CoreRequest: &coreRequest,
							CoreLimit:   &coreLimit,
						},
						Instances: &instances,
					},
				},
			}

			args, err := executorConfOption(conn)
			Expect(err).NotTo(HaveOccurred())
			config := shellParsedSparkConfig(args)

			// Verify Cores maps to spark.executor.cores (not affected by CoreRequest/CoreLimit)
			Expect(config).To(HaveKeyWithValue("spark.executor.cores", "4"))

			// Verify CoreRequest maps to physical CPU request
			Expect(config).To(HaveKeyWithValue(common.SparkKubernetesExecutorRequestCores, "3500m"))

			// Verify CoreLimit maps to physical CPU limit
			Expect(config).To(HaveKeyWithValue(common.SparkKubernetesExecutorLimitCores, "4"))
		})

		It("omits CPU configuration when CoreRequest and CoreLimit are not specified", func() {
			cores := int32(4)
			conn := &v1alpha1.SparkConnect{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-spark",
					Namespace: "default",
				},
				Spec: v1alpha1.SparkConnectSpec{
					SparkVersion: "3.5.0",
					Server: v1alpha1.ServerSpec{
						SparkPodSpec: v1alpha1.SparkPodSpec{
							Cores: &cores,
						},
					},
					Executor: v1alpha1.ExecutorSpec{},
				},
			}

			driverArgs, err := driverConfOption(conn)
			Expect(err).NotTo(HaveOccurred())
			driverConfig := shellParsedSparkConfig(driverArgs)

			// Verify Cores is present
			Expect(driverConfig).To(HaveKeyWithValue("spark.driver.cores", "4"))

			// Verify CPU request/limit are NOT present
			Expect(driverConfig).NotTo(HaveKey(common.SparkKubernetesDriverRequestCores))
			Expect(driverConfig).NotTo(HaveKey(common.SparkKubernetesDriverLimitCores))
		})

		It("includes only CoreRequest when CoreLimit is omitted", func() {
			cores := int32(4)
			coreRequest := "500m"
			conn := &v1alpha1.SparkConnect{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-spark",
					Namespace: "default",
				},
				Spec: v1alpha1.SparkConnectSpec{
					SparkVersion: "3.5.0",
					Server: v1alpha1.ServerSpec{
						SparkPodSpec: v1alpha1.SparkPodSpec{
							Cores:       &cores,
							CoreRequest: &coreRequest,
						},
					},
					Executor: v1alpha1.ExecutorSpec{},
				},
			}

			args, err := driverConfOption(conn)
			Expect(err).NotTo(HaveOccurred())
			config := shellParsedSparkConfig(args)

			// Verify CoreRequest is present
			Expect(config).To(HaveKeyWithValue(common.SparkKubernetesDriverRequestCores, "500m"))

			// Verify CoreLimit is NOT present
			Expect(config).NotTo(HaveKey(common.SparkKubernetesDriverLimitCores))
		})

		It("supports decimal CPU values", func() {
			cores := int32(4)
			coreRequest := "1.5"
			coreLimit := "2.5"
			conn := &v1alpha1.SparkConnect{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-spark",
					Namespace: "default",
				},
				Spec: v1alpha1.SparkConnectSpec{
					SparkVersion: "3.5.0",
					Server: v1alpha1.ServerSpec{
						SparkPodSpec: v1alpha1.SparkPodSpec{
							Cores:       &cores,
							CoreRequest: &coreRequest,
							CoreLimit:   &coreLimit,
						},
					},
					Executor: v1alpha1.ExecutorSpec{},
				},
			}

			args, err := driverConfOption(conn)
			Expect(err).NotTo(HaveOccurred())
			config := shellParsedSparkConfig(args)

			// Verify decimal values are preserved as strings
			Expect(config).To(HaveKeyWithValue(common.SparkKubernetesDriverRequestCores, "1.5"))
			Expect(config).To(HaveKeyWithValue(common.SparkKubernetesDriverLimitCores, "2.5"))
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
