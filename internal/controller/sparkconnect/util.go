/*
Copyright 2025 The kubeflow authors.

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
	"fmt"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

// GetCommonLabels returns the labels for resources owned by SparkConnect.
func GetCommonLabels(conn *v1alpha1.SparkConnect) map[string]string {
	labels := map[string]string{
		common.LabelCreatedBySparkOperator: "true",
		common.LabelSparkConnectName:       conn.Name,
	}
	return labels
}

// GetServerSelectorLabels returns the labels used to select server pods owned by SparkConnect.
func GetServerSelectorLabels(conn *v1alpha1.SparkConnect) map[string]string {
	labels := map[string]string{
		common.LabelLaunchedBySparkOperator: "true",
		common.LabelSparkConnectName:        conn.Name,
		common.LabelSparkRole:               common.SparkRoleConnectServer,
		common.LabelSparkVersion:            conn.Spec.SparkVersion,
	}
	return labels
}

// GetExecutorSelectorLabels returns the labels used to select executor pods owned by Spark connect server pod.
func GetExecutorSelectorLabels(conn *v1alpha1.SparkConnect) map[string]string {
	labels := map[string]string{
		common.LabelLaunchedBySparkOperator: "true",
		common.LabelSparkConnectName:        conn.Name,
		common.LabelSparkRole:               common.SparkRoleExecutor,
	}
	return labels
}

// GetConfigMapName returns the name of the config map for SparkConnect.
func GetConfigMapName(conn *v1alpha1.SparkConnect) string {
	return fmt.Sprintf("%s-conf", conn.Name)
}

// GetServerPodName returns the name of the server pod for SparkConnect.
func GetServerPodName(conn *v1alpha1.SparkConnect) string {
	return fmt.Sprintf("%s-server", conn.Name)
}

// GetServerServiceName returns the name of the server service for SparkConnect.
func GetServerServiceName(conn *v1alpha1.SparkConnect) string {
	// Use the service specified in the server spec if provided.
	svc := conn.Spec.Server.Service
	if svc != nil {
		return svc.Name
	}

	// Otherwise, use the default service name.
	return fmt.Sprintf("%s-server", conn.Name)
}

// GetServerServiceHost returns the host of the server service for SparkConnect.
func GetServerServiceHost(conn *v1alpha1.SparkConnect) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", GetServerServiceName(conn), conn.Namespace)
}

// GetServerIngressName returns the name of the server ingress for SparkConnect.
func GetServerIngressName(conn *v1alpha1.SparkConnect) string {
	// Use the ingress specified in the server spec if provided.
	ingress := conn.Spec.Server.Ingress
	if ingress != nil && ingress.Name != "" {
		return ingress.Name
	}

	// Otherwise, use the default ingress name.
	return fmt.Sprintf("%s-server", conn.Name)
}