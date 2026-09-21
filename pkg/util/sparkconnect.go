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

package util

import (
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
)

// GetRequiredServerServicePorts returns the service ports that the Spark Connect
// server must expose in order to function.
func GetRequiredServerServicePorts() []corev1.ServicePort {
	return []corev1.ServicePort{
		{
			Name:        common.SparkConnectBlockManagerPortName,
			Port:        common.SparkConnectBlockManagerPort,
			TargetPort:  intstr.FromInt32(common.SparkConnectBlockManagerPort),
			Protocol:    corev1.ProtocolTCP,
			AppProtocol: ptr.To("tcp"),
		},
		{
			Name:        common.SparkConnectDriverRPCPortName,
			Port:        common.SparkConnectDriverRPCPort,
			TargetPort:  intstr.FromInt32(common.SparkConnectDriverRPCPort),
			Protocol:    corev1.ProtocolTCP,
			AppProtocol: ptr.To("tcp"),
		},
		{
			Name:        common.SparkConnectServerPortName,
			Port:        common.SparkConnectServerPort,
			TargetPort:  intstr.FromInt32(common.SparkConnectServerPort),
			Protocol:    corev1.ProtocolTCP,
			AppProtocol: ptr.To("grpc"),
		},
		{
			Name:        common.SparkConnectWebUIPortName,
			Port:        common.SparkConnectWebUIPort,
			TargetPort:  intstr.FromInt32(common.SparkConnectWebUIPort),
			Protocol:    corev1.ProtocolTCP,
			AppProtocol: ptr.To("http"),
		},
	}
}
