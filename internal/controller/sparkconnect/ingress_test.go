package sparkconnect

import (
	"testing"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetServerIngressName(t *testing.T) {
	tests := []struct {
		name     string
		conn     *v1alpha1.SparkConnect
		expected string
	}{
		{
			name: "no ingress specified",
			conn: &v1alpha1.SparkConnect{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-connect",
				},
				Spec: v1alpha1.SparkConnectSpec{
					Server: v1alpha1.ServerSpec{},
				},
			},
			expected: "test-connect-server",
		},
		{
			name: "ingress with custom name",
			conn: &v1alpha1.SparkConnect{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-connect",
				},
				Spec: v1alpha1.SparkConnectSpec{
					Server: v1alpha1.ServerSpec{
						Ingress: &networkingv1.Ingress{
							ObjectMeta: metav1.ObjectMeta{
								Name: "custom-ingress-name",
							},
						},
					},
				},
			},
			expected: "custom-ingress-name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetServerIngressName(tt.conn)
			if result != tt.expected {
				t.Errorf("GetServerIngressName() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestGetServerServiceName(t *testing.T) {
	tests := []struct {
		name     string
		conn     *v1alpha1.SparkConnect
		expected string
	}{
		{
			name: "no service specified",
			conn: &v1alpha1.SparkConnect{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-connect",
				},
				Spec: v1alpha1.SparkConnectSpec{
					Server: v1alpha1.ServerSpec{},
				},
			},
			expected: "test-connect-server",
		},
		{
			name: "service with custom name",
			conn: &v1alpha1.SparkConnect{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-connect",
				},
				Spec: v1alpha1.SparkConnectSpec{
					Server: v1alpha1.ServerSpec{
						Service: &corev1.Service{
							ObjectMeta: metav1.ObjectMeta{
								Name: "custom-service-name",
							},
						},
					},
				},
			},
			expected: "custom-service-name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetServerServiceName(tt.conn)
			if result != tt.expected {
				t.Errorf("GetServerServiceName() = %v, want %v", result, tt.expected)
			}
		})
	}
}