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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
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
