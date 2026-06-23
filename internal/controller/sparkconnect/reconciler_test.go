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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"

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

	Context("when the user supplies ports via Server.Service", func() {
		portNames := func(svc *corev1.Service) []string {
			names := make([]string, 0, len(svc.Spec.Ports))
			for _, p := range svc.Spec.Ports {
				names = append(names, p.Name)
			}
			return names
		}

		It("should preserve a custom port and append all required ports", func() {
			conn.Spec.Server.Service = &corev1.Service{
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{
						{Name: "custom", Port: 9999},
					},
				},
			}
			svc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Namespace: conn.Namespace},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{
						{Name: "custom", Port: 9999},
					},
				},
			}
			err := reconciler.mutateServerService(context.TODO(), conn, svc)
			Expect(err).NotTo(HaveOccurred())

			// Custom port preserved, all four required ports appended.
			Expect(svc.Spec.Ports).To(HaveLen(5))
			Expect(portNames(svc)).To(ContainElements(
				"custom", "driver-rpc", "blockmanager", "web-ui", "spark-connect-server",
			))
		})

		It("should not overwrite a user-defined required port", func() {
			conn.Spec.Server.Service = &corev1.Service{
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{
						{Name: "web-ui", Port: 8080},
					},
				},
			}
			svc := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Namespace: conn.Namespace},
				Spec: corev1.ServiceSpec{
					Ports: []corev1.ServicePort{
						{Name: "web-ui", Port: 8080},
					},
				},
			}
			err := reconciler.mutateServerService(context.TODO(), conn, svc)
			Expect(err).NotTo(HaveOccurred())

			// The user's web-ui port wins; the other three required ports are appended.
			Expect(svc.Spec.Ports).To(HaveLen(4))
			Expect(portNames(svc)).To(ContainElements(
				"driver-rpc", "blockmanager", "web-ui", "spark-connect-server",
			))
			for _, p := range svc.Spec.Ports {
				if p.Name == "web-ui" {
					Expect(p.Port).To(Equal(int32(8080)))
				}
			}
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
	})
})
