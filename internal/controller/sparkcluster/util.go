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

package sparkcluster

import (
	"fmt"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

// GetCommonLabels returns the labels for resources owned by SparkCluster.
func GetCommonLabels(cluster *v1alpha1.SparkCluster) map[string]string {
	return map[string]string{
		common.LabelCreatedBySparkOperator: "true",
		common.LabelSparkClusterName:       cluster.Name,
	}
}

// GetMasterSelectorLabels returns the labels used to select master pods.
func GetMasterSelectorLabels(cluster *v1alpha1.SparkCluster) map[string]string {
	return map[string]string{
		common.LabelLaunchedBySparkOperator: "true",
		common.LabelSparkClusterName:        cluster.Name,
		common.LabelSparkRole:               common.SparkRoleClusterMaster,
	}
}

// GetWorkerSelectorLabels returns the labels used to select worker pods for a given group.
func GetWorkerSelectorLabels(cluster *v1alpha1.SparkCluster, groupName string) map[string]string {
	return map[string]string{
		common.LabelLaunchedBySparkOperator: "true",
		common.LabelSparkClusterName:        cluster.Name,
		common.LabelSparkRole:               common.SparkRoleClusterWorker,
		LabelWorkerGroup:                    groupName,
	}
}

const (
	// LabelWorkerGroup identifies which worker group a worker pod belongs to.
	LabelWorkerGroup = common.LabelAnnotationPrefix + "worker-group"
)

// GetMasterPodName returns the name of the master pod.
func GetMasterPodName(cluster *v1alpha1.SparkCluster) string {
	return fmt.Sprintf("%s-master", cluster.Name)
}

// GetMasterServiceName returns the name of the master service.
func GetMasterServiceName(cluster *v1alpha1.SparkCluster) string {
	return fmt.Sprintf("%s-master", cluster.Name)
}

// GetMasterServiceHost returns the DNS name of the master service.
func GetMasterServiceHost(cluster *v1alpha1.SparkCluster) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local", GetMasterServiceName(cluster), cluster.Namespace)
}

// GetWorkerPodName returns the name of a worker pod.
func GetWorkerPodName(cluster *v1alpha1.SparkCluster, groupName string, index int) string {
	return fmt.Sprintf("%s-worker-%s-%d", cluster.Name, groupName, index)
}
