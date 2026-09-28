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
	"k8s.io/utils/ptr"

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

var _ = Describe("gpuConfOption", func() {
	var conn *v1alpha1.SparkConnect

	BeforeEach(func() {
		conn = &v1alpha1.SparkConnect{
			ObjectMeta: metav1.ObjectMeta{Name: "gpu-connect", Namespace: "default"},
			Spec: v1alpha1.SparkConnectSpec{
				SparkVersion: "4.0.0",
				Image:        ptr.To("example.com/spark:gpu"),
			},
		}
	})

	It("returns no arguments for CPU-only sessions", func() {
		args, err := gpuConfOption(conn)
		Expect(err).NotTo(HaveOccurred())
		Expect(args).To(BeEmpty())
	})

	It("configures server and executor GPUs independently", func() {
		conn.Spec.Server.GPU = &v1alpha1.GPUSpec{Name: "amd.com/gpu", Quantity: 1}
		conn.Spec.Executor.GPU = &v1alpha1.GPUSpec{Name: "nvidia.com/gpu", Quantity: 2}
		args, err := gpuConfOption(conn)
		Expect(err).NotTo(HaveOccurred())
		Expect(shellParsedSparkConfig(args)).To(Equal(map[string]string{
			"spark.driver.resource.gpu.amount":   "1",
			"spark.driver.resource.gpu.vendor":   "amd.com",
			"spark.executor.resource.gpu.amount": "2",
			"spark.executor.resource.gpu.vendor": "nvidia.com",
		}))
	})

	It("only configures the role that requests a GPU", func() {
		conn.Spec.Executor.GPU = &v1alpha1.GPUSpec{Name: "nvidia.com/gpu", Quantity: 2}
		args, err := gpuConfOption(conn)
		Expect(err).NotTo(HaveOccurred())
		config := shellParsedSparkConfig(args)
		Expect(config).To(HaveKeyWithValue("spark.executor.resource.gpu.amount", "2"))
		Expect(config).NotTo(HaveKey("spark.driver.resource.gpu.amount"))
	})

	It("takes precedence over the same keys in sparkConf", func() {
		conn.Spec.Executor.GPU = &v1alpha1.GPUSpec{Name: "nvidia.com/gpu", Quantity: 2}
		conn.Spec.SparkConf = map[string]string{
			"spark.executor.resource.gpu.amount":          "9",
			"spark.executor.resource.gpu.vendor":          "old.example.com",
			"spark.executor.resource.gpu.discoveryScript": "/opt/spark/scripts/discover gpus.sh",
			"spark.task.resource.gpu.amount":              "0.25",
		}
		sparkConfArgs, err := sparkConfOption(conn)
		Expect(err).NotTo(HaveOccurred())
		gpuArgs, err := gpuConfOption(conn)
		Expect(err).NotTo(HaveOccurred())

		config := shellParsedSparkConfig(append(sparkConfArgs, gpuArgs...))
		Expect(config).To(HaveKeyWithValue("spark.executor.resource.gpu.amount", "2"))
		Expect(config).To(HaveKeyWithValue("spark.executor.resource.gpu.vendor", "nvidia.com"))
		Expect(config).To(HaveKeyWithValue("spark.executor.resource.gpu.discoveryScript", "/opt/spark/scripts/discover gpus.sh"))
		Expect(config).To(HaveKeyWithValue("spark.task.resource.gpu.amount", "0.25"))
	})

	It("does not create an executor pod template only for a GPU", func() {
		conn.Spec.Executor.GPU = &v1alpha1.GPUSpec{Name: "nvidia.com/gpu", Quantity: 2}
		args, err := executorPodTemplateOption(conn)
		Expect(err).NotTo(HaveOccurred())
		Expect(args).To(BeEmpty())
	})

	DescribeTable("rejects invalid GPU resources",
		func(server bool, name string, quantity int64) {
			gpu := &v1alpha1.GPUSpec{Name: name, Quantity: quantity}
			if server {
				conn.Spec.Server.GPU = gpu
			} else {
				conn.Spec.Executor.GPU = gpu
			}
			_, err := gpuConfOption(conn)
			Expect(err).To(HaveOccurred())
		},
		Entry("zero server GPUs", true, "nvidia.com/gpu", int64(0)),
		Entry("negative executor GPUs", false, "nvidia.com/gpu", int64(-1)),
		Entry("missing vendor", false, "gpu", int64(1)),
		Entry("empty resource name", true, "", int64(1)),
		Entry("invalid vendor", false, "bad..example/gpu", int64(1)),
		Entry("unsupported resource suffix", true, "nvidia.com/other", int64(1)),
	)
})
