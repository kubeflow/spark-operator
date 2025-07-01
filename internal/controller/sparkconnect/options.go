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
	"fmt"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

type sparkConnectOptionFunc func(*v1alpha1.SparkConnect) ([]string, error)

// buildStartConnectServerArgs returns the arguments for starting Spark Connect server.
func buildStartConnectServerArgs(conn *v1alpha1.SparkConnect) ([]string, error) {
	optionFuncs := []sparkConnectOptionFunc{
		masterOption,
		namespaceOption,
		imageOption,
		sparkConfOption,
		hadoopConfOption,
		driverConfOption,
		executorConfOption,
		executorPodTemplateOption,
		dynamicAllocationOption,
	}

	args := []string{"${SPARK_HOME}/sbin/start-connect-server.sh"}

	for _, optionFunc := range optionFuncs {
		option, err := optionFunc(conn)
		if err != nil {
			return nil, err
		}
		args = append(args, option...)
	}

	return args, nil
}

func masterOption(_ *v1alpha1.SparkConnect) ([]string, error) {
	masterURL, err := util.GetMasterURL()
	if err != nil {
		return nil, fmt.Errorf("failed to get master URL: %v", err)
	}
	args := []string{
		"--master",
		masterURL,
	}
	return args, nil
}

func namespaceOption(conn *v1alpha1.SparkConnect) ([]string, error) {
	args := []string{
		"--conf",
		fmt.Sprintf("%s=%s", common.SparkKubernetesNamespace, conn.Namespace),
	}
	return args, nil
}

func imageOption(conn *v1alpha1.SparkConnect) ([]string, error) {
	var args []string

	executorImage := ""
	if conn.Spec.Image != nil && *conn.Spec.Image != "" {
		executorImage = *conn.Spec.Image
	}

	template := conn.Spec.Executor.Template
	if len(template.Spec.Containers) != 0 {
		index := 0
		for i, container := range conn.Spec.Executor.Template.Spec.Containers {
			if container.Name == common.Spark3DefaultExecutorContainerName {
				index = i
				break
			}
		}

		if template.Spec.Containers[index].Image != "" {
			executorImage = template.Spec.Containers[index].Image
		}
	}

	args = append(
		args,
		"--conf",
		fmt.Sprintf("%s=%s", common.SparkKubernetesContainerImage, executorImage),
		"--conf",
		fmt.Sprintf("%s=%s", common.SparkKubernetesExecutorContainerImage, executorImage),
	)

	return args, nil
}

func sparkConfOption(conn *v1alpha1.SparkConnect) ([]string, error) {
	if conn.Spec.SparkConf == nil {
		return nil, nil
	}
	var args []string
	// Add Spark configuration properties.
	for key, value := range conn.Spec.SparkConf {
		// Configuration property for the driver pod name has already been set.
		if key != common.SparkKubernetesDriverPodName {
			args = append(args, "--conf", fmt.Sprintf("%s=%s", key, value))
		}
	}
	return args, nil
}

func hadoopConfOption(conn *v1alpha1.SparkConnect) ([]string, error) {
	if conn.Spec.HadoopConf == nil {
		return nil, nil
	}
	var args []string
	// Add Hadoop configuration properties.
	for key, value := range conn.Spec.HadoopConf {
		args = append(args, "--conf", fmt.Sprintf("spark.hadoop.%s=%s", key, value))
	}
	return args, nil
}

func driverConfOption(conn *v1alpha1.SparkConnect) ([]string, error) {
	var args []string
	var property string

	property = fmt.Sprintf(common.SparkKubernetesDriverLabelTemplate, common.LabelLaunchedBySparkOperator)
	args = append(args, "--conf", fmt.Sprintf("%s=%s", property, "true"))

	property = fmt.Sprintf(common.SparkKubernetesDriverLabelTemplate, common.LabelSparkConnectName)
	args = append(args, "--conf", fmt.Sprintf("%s=%s", property, conn.Name))

	// Populate SparkApplication labels to driver pod
	for key, value := range conn.Labels {
		property = fmt.Sprintf(common.SparkKubernetesDriverLabelTemplate, key)
		args = append(args, "--conf", fmt.Sprintf("%s=%s", property, value))
	}

	// Driver cores
	if conn.Spec.Server.Cores != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%d", common.SparkDriverCores, *conn.Spec.Server.Cores))
	}

	// Driver memory
	if conn.Spec.Server.Memory != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", common.SparkDriverMemory, *conn.Spec.Server.Memory))
	}

	args = append(args, "--conf", "spark.driver.bindAddress=0.0.0.0")
	args = append(args, "--conf", "spark.driver.host=${POD_IP}")
	args = append(args, "--conf", "spark.driver.port=7078")

	// Driver pod name
	args = append(args, "--conf", fmt.Sprintf("%s=%s", common.SparkKubernetesDriverPodName, fmt.Sprintf("%s-server", conn.Name)))

	return args, nil
}

func executorConfOption(conn *v1alpha1.SparkConnect) ([]string, error) {
	var args []string
	var property string

	// Add a label to mark the pod as created by SparkOperator
	property = fmt.Sprintf(common.SparkKubernetesExecutorLabelTemplate, common.LabelLaunchedBySparkOperator)
	args = append(args, "--conf", fmt.Sprintf("%s=%s", property, "true"))

	// Add a label to recognize SparkConnect object name
	property = fmt.Sprintf(common.SparkKubernetesExecutorLabelTemplate, common.LabelSparkConnectName)
	args = append(args, "--conf", fmt.Sprintf("%s=%s", property, conn.Name))

	// Populate SparkApplication labels to executor pod
	for key, value := range conn.Labels {
		property := fmt.Sprintf(common.SparkKubernetesExecutorLabelTemplate, key)
		args = append(args, "--conf", fmt.Sprintf("%s=%s", property, value))
	}

	// Executor instances
	if conn.Spec.Executor.Instances != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%d", common.SparkExecutorInstances, *conn.Spec.Executor.Instances))
	}

	// Executor cores
	if conn.Spec.Executor.Cores != nil {
		// Property "spark.executor.cores" does not allow float values.
		args = append(args, "--conf",
			fmt.Sprintf("%s=%d", common.SparkExecutorCores, *conn.Spec.Executor.Cores))
	}

	// Use SparkConnect object name as executor pod name prefix.
	args = append(args, "--conf", fmt.Sprintf("%s=%s", common.SparkKubernetesExecutorPodNamePrefix, conn.Name))

	return args, nil
}

// executorPodTemplateOption returns the executor pod template arguments.
func executorPodTemplateOption(conn *v1alpha1.SparkConnect) ([]string, error) {
	if conn.Spec.Executor.Template == nil {
		return []string{}, nil
	}

	podTemplateFile := fmt.Sprintf("/tmp/spark/%s", ExecutorPodTemplateFileName)
	if err := util.WriteObjectToFile(conn.Spec.Executor.Template, podTemplateFile); err != nil {
		return []string{}, err
	}

	args := []string{
		"--conf",
		fmt.Sprintf("%s=%s", common.SparkKubernetesExecutorPodTemplateFile, podTemplateFile),
		"--conf",
		fmt.Sprintf("%s=%s", common.SparkKubernetesExecutorPodTemplateContainerName, common.Spark3DefaultExecutorContainerName),
	}
	return args, nil
}

func dynamicAllocationOption(conn *v1alpha1.SparkConnect) ([]string, error) {
	if conn.Spec.DynamicAllocation == nil || !conn.Spec.DynamicAllocation.Enabled {
		return nil, nil
	}

	var args []string
	dynamicAllocation := conn.Spec.DynamicAllocation
	args = append(args, "--conf",
		fmt.Sprintf("%s=true", common.SparkDynamicAllocationEnabled))

	if dynamicAllocation.InitialExecutors != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%d", common.SparkDynamicAllocationInitialExecutors, *dynamicAllocation.InitialExecutors))
	}

	if dynamicAllocation.MinExecutors != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%d", common.SparkDynamicAllocationMinExecutors, *dynamicAllocation.MinExecutors))
	}

	if dynamicAllocation.MaxExecutors != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%d", common.SparkDynamicAllocationMaxExecutors, *dynamicAllocation.MaxExecutors))
	}

	shuffleTrackingEnabled := true
	if dynamicAllocation.ShuffleTrackingEnabled != nil {
		shuffleTrackingEnabled = *dynamicAllocation.ShuffleTrackingEnabled
	}

	args = append(args, "--conf",
		fmt.Sprintf("%s=%t", common.SparkDynamicAllocationShuffleTrackingEnabled, shuffleTrackingEnabled))
	if dynamicAllocation.ShuffleTrackingTimeout != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%d", common.SparkDynamicAllocationShuffleTrackingTimeout, *dynamicAllocation.ShuffleTrackingTimeout))
	}

	return args, nil
}
