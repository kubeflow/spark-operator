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
	"context"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

var _ = Describe("mutateServerService", func() {
	var (
		reconciler *Reconciler
		conn       *v1alpha1.SparkConnect
	)

	BeforeEach(func() {
		reconciler = &Reconciler{
			scheme: scheme.Scheme,
		}
		conn = &v1alpha1.SparkConnect{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-spark-connect",
				Namespace: "test-namespace",
				UID:       "test-uid",
			},
			Spec: v1alpha1.SparkConnectSpec{
				SparkVersion: "4.0.0",
				Server: v1alpha1.ServerSpec{
					SparkPodSpec: v1alpha1.SparkPodSpec{},
				},
				Executor: v1alpha1.ExecutorSpec{
					SparkPodSpec: v1alpha1.SparkPodSpec{},
				},
			},
		}
	})

	Context("when creating a new service", func() {
		It("should set appProtocol on all ports", func() {
			svc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: conn.Namespace,
				},
			}
			err := reconciler.mutateServerService(context.TODO(), conn, svc)
			Expect(err).NotTo(HaveOccurred())
			Expect(svc.Spec.Ports).To(HaveLen(4))

			expectedAppProtocols := map[string]string{
				"driver-rpc":           "tcp",
				"blockmanager":         "tcp",
				"web-ui":               "http",
				"spark-connect-server": "grpc",
			}

			for _, port := range svc.Spec.Ports {
				expected, ok := expectedAppProtocols[port.Name]
				Expect(ok).To(BeTrue(), "unexpected port name: %s", port.Name)
				Expect(port.AppProtocol).NotTo(BeNil(), "appProtocol should be set for port %s", port.Name)
				Expect(port.AppProtocol).To(Equal(ptr.To(expected)), "appProtocol mismatch for port %s", port.Name)
			}
		})

		It("should set correct port numbers", func() {
			svc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: conn.Namespace,
				},
			}
			err := reconciler.mutateServerService(context.TODO(), conn, svc)
			Expect(err).NotTo(HaveOccurred())

			expectedPorts := map[string]int32{
				"driver-rpc":           7078,
				"blockmanager":         7079,
				"web-ui":               4040,
				"spark-connect-server": 15002,
			}

			for _, port := range svc.Spec.Ports {
				Expect(port.Port).To(Equal(expectedPorts[port.Name]))
				Expect(port.Protocol).To(Equal(corev1.ProtocolTCP))
			}
		})

		It("should set selector labels", func() {
			svc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: conn.Namespace,
				},
			}
			err := reconciler.mutateServerService(context.TODO(), conn, svc)
			Expect(err).NotTo(HaveOccurred())

			labels := GetServerSelectorLabels(conn)
			for key, val := range labels {
				Expect(svc.Spec.Selector).To(HaveKeyWithValue(key, val))
			}
		})
	})

	Context("when service already exists", func() {
		It("should not overwrite existing ports", func() {
			svc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: conn.Namespace,
					// Non-zero CreationTimestamp indicates existing service.
					CreationTimestamp: metav1.Now(),
				},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{
						{
							Name: "spark-connect-server",
							Port: 15002,
						},
					},
				},
			}
			err := reconciler.mutateServerService(context.TODO(), conn, svc)
			Expect(err).NotTo(HaveOccurred())

			// Ports should remain unchanged for existing services.
			Expect(svc.Spec.Ports).To(HaveLen(1))
			Expect(svc.Spec.Ports[0].Name).To(Equal("spark-connect-server"))
		})
	})
})

var _ = Describe("mutateServerPod", func() {
	var (
		reconciler *Reconciler
		conn       *v1alpha1.SparkConnect
		image      string
	)

	BeforeEach(func() {
		reconciler = &Reconciler{
			scheme: scheme.Scheme,
		}
		image = "apache/spark:4.0.0"
		Expect(os.Setenv(common.EnvKubernetesServiceHost, "127.0.0.1")).NotTo(HaveOccurred())
		Expect(os.Setenv(common.EnvKubernetesServicePort, "443")).NotTo(HaveOccurred())
		conn = &v1alpha1.SparkConnect{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-spark-connect",
				Namespace: "test-namespace",
				UID:       "test-uid",
			},
			Spec: v1alpha1.SparkConnectSpec{
				Image:        &image,
				SparkVersion: "4.0.0",
				Server: v1alpha1.ServerSpec{
					SparkPodSpec: v1alpha1.SparkPodSpec{},
				},
				Executor: v1alpha1.ExecutorSpec{
					SparkPodSpec: v1alpha1.SparkPodSpec{},
				},
			},
		}
	})

	AfterEach(func() {
		Expect(os.Unsetenv(common.EnvKubernetesServiceHost)).NotTo(HaveOccurred())
		Expect(os.Unsetenv(common.EnvKubernetesServicePort)).NotTo(HaveOccurred())
	})

	Context("when creating a new server pod", func() {
		It("should set default TCP startup and readiness probes", func() {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: conn.Namespace,
				},
			}
			err := reconciler.mutateServerPod(context.TODO(), conn, pod)
			Expect(err).NotTo(HaveOccurred())
			Expect(pod.Spec.Containers).NotTo(BeEmpty())

			container := pod.Spec.Containers[0]
			Expect(container.Name).To(Equal(common.SparkDriverContainerName))
			Expect(container.StartupProbe).NotTo(BeNil())
			Expect(container.StartupProbe.TCPSocket).NotTo(BeNil())
			Expect(container.StartupProbe.TCPSocket.Port).To(Equal(intstr.FromInt(sparkConnectServerPort)))
			Expect(container.ReadinessProbe).NotTo(BeNil())
			Expect(container.ReadinessProbe.TCPSocket).NotTo(BeNil())
			Expect(container.ReadinessProbe.TCPSocket.Port).To(Equal(intstr.FromInt(sparkConnectServerPort)))
		})

		It("should preserve user-provided startup and readiness probes", func() {
			startupProbe := &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: "/startup",
						Port: intstr.FromInt(4040),
					},
				},
			}
			readinessProbe := &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: "/ready",
						Port: intstr.FromInt(4040),
					},
				},
			}
			conn.Spec.Server.Template = &corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:           common.SparkDriverContainerName,
							Image:          image,
							StartupProbe:   startupProbe,
							ReadinessProbe: readinessProbe,
						},
					},
				},
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: conn.Namespace,
				},
			}
			err := reconciler.mutateServerPod(context.TODO(), conn, pod)
			Expect(err).NotTo(HaveOccurred())

			container := pod.Spec.Containers[0]
			Expect(container.StartupProbe).To(Equal(startupProbe))
			Expect(container.ReadinessProbe).To(Equal(readinessProbe))
		})
		It("should not set a service account when none is configured", func() {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: conn.Namespace,
				},
			}
			Expect(reconciler.mutateServerPod(context.TODO(), conn, pod)).To(Succeed())
			Expect(pod.Spec.ServiceAccountName).To(BeEmpty())
		})

		It("should fall back to the operator default service account", func() {
			reconciler.options.DefaultServiceAccount = "spark-operator-spark"
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: conn.Namespace,
				},
			}
			Expect(reconciler.mutateServerPod(context.TODO(), conn, pod)).To(Succeed())
			Expect(pod.Spec.ServiceAccountName).To(Equal("spark-operator-spark"))
		})

		It("should preserve the service account specified in the server pod template", func() {
			reconciler.options.DefaultServiceAccount = "spark-operator-spark"
			conn.Spec.Server.Template = &corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					ServiceAccountName: "template-sa",
				},
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: conn.Namespace,
				},
			}
			Expect(reconciler.mutateServerPod(context.TODO(), conn, pod)).To(Succeed())
			Expect(pod.Spec.ServiceAccountName).To(Equal("template-sa"))
		})

		It("should fall back to the default when the server pod template has an empty service account", func() {
			reconciler.options.DefaultServiceAccount = "spark-operator-spark"
			conn.Spec.Server.Template = &corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					ServiceAccountName: "",
				},
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: conn.Namespace,
				},
			}
			Expect(reconciler.mutateServerPod(context.TODO(), conn, pod)).To(Succeed())
			Expect(pod.Spec.ServiceAccountName).To(Equal("spark-operator-spark"))
		})
	})
})

var _ = Describe("mutateServerPod GPU support", func() {
	var (
		conn       *v1alpha1.SparkConnect
		reconciler *Reconciler
	)

	BeforeEach(func() {
		conn = &v1alpha1.SparkConnect{
			ObjectMeta: metav1.ObjectMeta{Name: "gpu-connect", Namespace: "default", UID: "test-uid"},
			Spec: v1alpha1.SparkConnectSpec{
				SparkVersion: "4.0.0",
				Image:        ptr.To("example.com/spark:gpu"),
			},
		}
		reconciler = &Reconciler{scheme: scheme.Scheme}
		Expect(os.Setenv(common.EnvKubernetesServiceHost, "127.0.0.1")).To(Succeed())
		Expect(os.Setenv(common.EnvKubernetesServicePort, "443")).To(Succeed())
		DeferCleanup(os.Unsetenv, common.EnvKubernetesServiceHost)
		DeferCleanup(os.Unsetenv, common.EnvKubernetesServicePort)
	})

	It("does not request GPUs for CPU-only sessions", func() {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: conn.Namespace}}
		Expect(reconciler.mutateServerPod(context.Background(), conn, pod)).To(Succeed())
		Expect(pod.Spec.Containers[0].Resources.Requests).NotTo(HaveKey(corev1.ResourceName("nvidia.com/gpu")))
		Expect(pod.Spec.Containers[0].Resources.Limits).NotTo(HaveKey(corev1.ResourceName("nvidia.com/gpu")))
	})

	It("does not request a server GPU for an executor-only GPU session", func() {
		conn.Spec.Executor.GPU = &v1alpha1.GPUSpec{Name: "nvidia.com/gpu", Quantity: 2}
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: conn.Namespace}}
		Expect(reconciler.mutateServerPod(context.Background(), conn, pod)).To(Succeed())
		command := pod.Spec.Containers[0].Args[0]
		Expect(command).To(ContainSubstring("spark.executor.resource.gpu.amount=2"))
		Expect(command).To(ContainSubstring("spark.executor.resource.gpu.vendor=nvidia.com"))
		Expect(command).NotTo(ContainSubstring("spark.driver.resource.gpu.amount"))
		Expect(pod.Spec.Containers[0].Resources.Limits).NotTo(HaveKey(corev1.ResourceName("nvidia.com/gpu")))
	})

	DescribeTable("preserves the pod template and sidecars while setting matching GPU requests and limits",
		func(containerName string) {
			template := &corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"custom": "label"}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{
					{Name: "metrics", Image: "example.com/metrics:latest"},
					{Name: containerName, Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), "nvidia.com/gpu": resource.MustParse("9")},
						Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi"), "nvidia.com/gpu": resource.MustParse("9")},
					}},
				}},
			}
			// A custom container name uses the same first-container fallback as image selection.
			index := 1
			if containerName == "custom" {
				template.Spec.Containers[0], template.Spec.Containers[1] = template.Spec.Containers[1], template.Spec.Containers[0]
				index = 0
			}
			before := template.DeepCopy()
			conn.Spec.Server.GPU = &v1alpha1.GPUSpec{Name: "nvidia.com/gpu", Quantity: 2}
			conn.Spec.Server.Template = template

			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: conn.Namespace}}
			Expect(reconciler.mutateServerPod(context.Background(), conn, pod)).To(Succeed())

			containers := pod.Spec.Containers
			Expect(containers).To(HaveLen(2))
			Expect(containers[1-index]).To(Equal(before.Spec.Containers[1-index]))
			Expect(containers[index].Resources.Requests).To(HaveKeyWithValue(corev1.ResourceCPU, resource.MustParse("1")))
			Expect(containers[index].Resources.Limits).To(HaveKeyWithValue(corev1.ResourceMemory, resource.MustParse("1Gi")))
			gpuName := corev1.ResourceName("nvidia.com/gpu")
			Expect(containers[index].Resources.Requests.Name(gpuName, resource.DecimalSI).Value()).To(Equal(int64(2)))
			Expect(containers[index].Resources.Limits.Name(gpuName, resource.DecimalSI).Value()).To(Equal(int64(2)))
			// The user's template must not be mutated.
			Expect(template).To(Equal(before))
		},
		Entry("named container", common.SparkDriverContainerName),
		Entry("first container", "custom"),
	)
})

var _ = Describe("SparkConnect GPU API validation", func() {
	var conn *v1alpha1.SparkConnect

	BeforeEach(func() {
		conn = &v1alpha1.SparkConnect{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "gpu-connect-", Namespace: "default"},
			Spec: v1alpha1.SparkConnectSpec{
				SparkVersion: "4.0.0",
				Image:        ptr.To("example.com/spark:gpu"),
			},
		}
	})

	It("retains both GPU specifications through the API server and deep copies", func() {
		conn.Spec.Server.GPU = &v1alpha1.GPUSpec{Name: "amd.com/gpu", Quantity: 1}
		conn.Spec.Executor.GPU = &v1alpha1.GPUSpec{Name: "nvidia.com/gpu", Quantity: 2}
		Expect(k8sClient.Create(context.Background(), conn)).To(Succeed())
		DeferCleanup(k8sClient.Delete, context.Background(), conn)

		stored := &v1alpha1.SparkConnect{}
		Expect(k8sClient.Get(context.Background(), client.ObjectKeyFromObject(conn), stored)).To(Succeed())
		Expect(stored.Spec.Server.GPU).To(Equal(conn.Spec.Server.GPU))
		Expect(stored.Spec.Executor.GPU).To(Equal(conn.Spec.Executor.GPU))

		copied := stored.DeepCopy()
		copied.Spec.Server.GPU.Quantity = 3
		copied.Spec.Executor.GPU.Name = "amd.com/gpu"
		Expect(stored.Spec.Server.GPU.Quantity).To(Equal(int64(1)))
		Expect(stored.Spec.Executor.GPU.Name).To(Equal("nvidia.com/gpu"))
	})

	DescribeTable("rejects invalid GPU resources in the CRD schema",
		func(server bool, name string, quantity int64) {
			gpu := &v1alpha1.GPUSpec{Name: name, Quantity: quantity}
			if server {
				conn.Spec.Server.GPU = gpu
			} else {
				conn.Spec.Executor.GPU = gpu
			}
			err := k8sClient.Create(context.Background(), conn)
			Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected schema rejection, got %v", err)
		},
		Entry("zero server GPUs", true, "nvidia.com/gpu", int64(0)),
		Entry("negative executor GPUs", false, "nvidia.com/gpu", int64(-1)),
		Entry("missing vendor", false, "gpu", int64(1)),
		Entry("empty resource name", true, "", int64(1)),
		Entry("invalid vendor", false, "bad..example/gpu", int64(1)),
		Entry("unsupported resource suffix", true, "nvidia.com/other", int64(1)),
	)
})
