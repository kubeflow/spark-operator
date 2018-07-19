/*
Copyright 2017 Google LLC

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

package sparkapplication

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	"k8s.io/spark-on-k8s-operator/pkg/config"
	"k8s.io/spark-on-k8s-operator/pkg/util"
)

const (
	sparkHomeEnvVar             = "SPARK_HOME"
	kubernetesServiceHostEnvVar = "KUBERNETES_SERVICE_HOST"
	kubernetesServicePortEnvVar = "KUBERNETES_SERVICE_PORT"
)

// submission includes information of a Spark application to be submitted.
type submission struct {
	namespace string
	name      string
	args      []string
}

func newSubmission(args []string, app *v1alpha1.SparkApplication) *submission {
	return &submission{
		namespace: app.Namespace,
		name:      app.Name,
		args:      args,
	}
}

func buildSubmissionCommandArgs(app *v1alpha1.SparkApplication) ([]string, error) {
	var args []string
	if app.Spec.MainClass != nil {
		args = append(args, "--class", *app.Spec.MainClass)
	}
	masterURL, err := getMasterURL()
	if err != nil {
		return nil, err
	}

	args = append(args, "--master", masterURL)
	args = append(args, "--deploy-mode", string(app.Spec.Mode))
	args = append(args, "--conf", fmt.Sprintf("spark.kubernetes.namespace=%s", app.Namespace))
	args = append(args, "--conf", fmt.Sprintf("spark.app.name=%s", app.Name))

	// Add application dependencies.
	args = append(args, addDependenciesConfOptions(app)...)

	if app.Spec.Image != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", config.SparkContainerImageKey, *app.Spec.Image))
	}
	if app.Spec.InitContainerImage != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", config.SparkInitContainerImage, *app.Spec.InitContainerImage))
	}
	if app.Spec.ImagePullPolicy != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", config.SparkContainerImagePullPolicyKey, *app.Spec.ImagePullPolicy))
	}
	if len(app.Spec.ImagePullSecrets) > 0 {
		secretNames := strings.Join(app.Spec.ImagePullSecrets, ",")
		args = append(args, "--conf", fmt.Sprintf("%s=%s", config.SparkImagePullSecretKey, secretNames))
	}

	if app.Spec.SparkConf == nil {
		app.Spec.SparkConf = make(map[string]string)
	}
	// Operator triggered spark-submit should never wait for App completion
	app.Spec.SparkConf[config.SparkWaitAppCompletion] = "false"

	if app.Spec.SparkConfigMap != nil {
		args = append(args, "--conf", config.GetDriverAnnotationOption(config.SparkConfigMapAnnotation,
			*app.Spec.SparkConfigMap))
		args = append(args, "--conf", config.GetExecutorAnnotationOption(config.SparkConfigMapAnnotation,
			*app.Spec.SparkConfigMap))
	}

	if app.Spec.HadoopConfigMap != nil {
		args = append(args, "--conf", config.GetDriverAnnotationOption(config.HadoopConfigMapAnnotation,
			*app.Spec.HadoopConfigMap))
		args = append(args, "--conf", config.GetExecutorAnnotationOption(config.HadoopConfigMapAnnotation,
			*app.Spec.HadoopConfigMap))
	}

	// Add Spark configuration properties.
	for key, value := range app.Spec.SparkConf {
		args = append(args, "--conf", fmt.Sprintf("%s=%s", key, value))
	}

	// Add Hadoop configuration properties.
	for key, value := range app.Spec.HadoopConf {
		args = append(args, "--conf", fmt.Sprintf("spark.hadoop.%s=%s", key, value))
	}

	for key, value := range app.Spec.NodeSelector {
		conf := fmt.Sprintf("%s%s=%s", config.SparkNodeSelectorKeyPrefix, key, value)
		args = append(args, "--conf", conf)
	}

	// Add the driver and executor configuration options.
	// Note that when the controller submits the application, it expects that all dependencies are local
	// so init-container is not needed and therefore no init-container image needs to be specified.
	options, err := addDriverConfOptions(app)
	if err != nil {
		return nil, err
	}
	for _, option := range options {
		args = append(args, "--conf", option)
	}
	options, err = addExecutorConfOptions(app)
	if err != nil {
		return nil, err
	}
	for _, option := range options {
		args = append(args, "--conf", option)
	}

	reference := getOwnerReference(app)
	referenceStr, err := util.MarshalOwnerReference(reference)
	if err != nil {
		return nil, err
	}
	args = append(args, "--conf", config.GetDriverAnnotationOption(config.OwnerReferenceAnnotation, referenceStr))

	if app.Spec.MainApplicationFile != nil {
		// Add the main application file if it is present.
		args = append(args, *app.Spec.MainApplicationFile)
	}

	// Add application arguments.
	for _, argument := range app.Spec.Arguments {
		args = append(args, argument)
	}

	return args, nil
}

func getMasterURL() (string, error) {
	kubernetesServiceHost := os.Getenv(kubernetesServiceHostEnvVar)
	if kubernetesServiceHost == "" {
		return "", fmt.Errorf("environment variable %s is not found", kubernetesServiceHostEnvVar)
	}
	kubernetesServicePort := os.Getenv(kubernetesServicePortEnvVar)
	if kubernetesServicePort == "" {
		return "", fmt.Errorf("environment variable %s is not found", kubernetesServicePortEnvVar)
	}
	return fmt.Sprintf("k8s://https://%s:%s", kubernetesServiceHost, kubernetesServicePort), nil
}

func getOwnerReference(app *v1alpha1.SparkApplication) *metav1.OwnerReference {
	return &metav1.OwnerReference{
		APIVersion: v1alpha1.SchemeGroupVersion.String(),
		Kind:       reflect.TypeOf(v1alpha1.SparkApplication{}).Name(),
		Name:       app.Name,
		UID:        app.UID,
	}
}

func addDependenciesConfOptions(app *v1alpha1.SparkApplication) []string {
	var depsConfOptions []string

	if len(app.Spec.Deps.Jars) > 0 {
		depsConfOptions = append(depsConfOptions, "--jars", strings.Join(app.Spec.Deps.Jars, ","))
	}
	if len(app.Spec.Deps.Files) > 0 {
		depsConfOptions = append(depsConfOptions, "--files", strings.Join(app.Spec.Deps.Files, ","))
	}
	if len(app.Spec.Deps.PyFiles) > 0 {
		depsConfOptions = append(depsConfOptions, "--py-files", strings.Join(app.Spec.Deps.PyFiles, ","))
	}

	if app.Spec.Deps.JarsDownloadDir != nil {
		depsConfOptions = append(depsConfOptions, "--conf",
			fmt.Sprintf("%s=%s", config.SparkJarsDownloadDir, *app.Spec.Deps.JarsDownloadDir))
	}

	if app.Spec.Deps.FilesDownloadDir != nil {
		depsConfOptions = append(depsConfOptions, "--conf",
			fmt.Sprintf("%s=%s", config.SparkFilesDownloadDir, *app.Spec.Deps.FilesDownloadDir))
	}

	if app.Spec.Deps.DownloadTimeout != nil {
		depsConfOptions = append(depsConfOptions, "--conf",
			fmt.Sprintf("%s=%d", config.SparkDownloadTimeout, *app.Spec.Deps.DownloadTimeout))
	}

	if app.Spec.Deps.MaxSimultaneousDownloads != nil {
		depsConfOptions = append(depsConfOptions, "--conf",
			fmt.Sprintf("%s=%d", config.SparkMaxSimultaneousDownloads, *app.Spec.Deps.MaxSimultaneousDownloads))
	}

	return depsConfOptions
}

func addDriverConfOptions(app *v1alpha1.SparkApplication) ([]string, error) {
	var driverConfOptions []string

	driverConfOptions = append(driverConfOptions,
		fmt.Sprintf("%s%s=%s", config.SparkDriverLabelKeyPrefix, config.SparkAppNameLabel, app.Name))
	driverConfOptions = append(driverConfOptions,
		fmt.Sprintf("%s%s=%s", config.SparkDriverLabelKeyPrefix, config.SparkAppIDLabel, app.Status.AppID))
	driverConfOptions = append(driverConfOptions,
		fmt.Sprintf("%s%s=%s", config.SparkDriverLabelKeyPrefix, config.LaunchedBySparkOperatorLabel, "true"))

	if app.Spec.Driver.PodName != nil {
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("%s=%s", config.SparkDriverPodNameKey, *app.Spec.Driver.PodName))
	}

	if app.Spec.Driver.Image != nil {
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("%s=%s", config.SparkDriverContainerImageKey, *app.Spec.Driver.Image))
	}

	if app.Spec.Driver.Cores != nil {
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("spark.driver.cores=%f", *app.Spec.Driver.Cores))
	}
	if app.Spec.Driver.CoreLimit != nil {
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("%s=%s", config.SparkDriverCoreLimitKey, *app.Spec.Driver.CoreLimit))
	}
	if app.Spec.Driver.Memory != nil {
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("spark.driver.memory=%s", *app.Spec.Driver.Memory))
	}
	if app.Spec.Driver.MemoryOverhead != nil {
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("spark.driver.memoryOverhead=%s", *app.Spec.Driver.MemoryOverhead))
	}

	if app.Spec.Driver.ServiceAccount != nil {
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("%s=%s", config.SparkDriverServiceAccountName, *app.Spec.Driver.ServiceAccount))
	}

	for key, value := range app.Spec.Driver.Labels {
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("%s%s=%s", config.SparkDriverLabelKeyPrefix, key, value))
	}

	for key, value := range app.Spec.Driver.Annotations {
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("%s%s=%s", config.SparkDriverAnnotationKeyPrefix, key, value))
	}

	for key, value := range app.Spec.Driver.EnvSecretKeyRefs {
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("%s%s=%s:%s", config.SparkDriverSecretKeyRefKeyPrefix, key, value.Name, value.Key))
	}

	if app.Spec.Driver.Affinity != nil {
		affinityString, err := util.MarshalAffinity(app.Spec.Driver.Affinity)
		if err != nil {
			return nil, err
		}
		driverConfOptions = append(driverConfOptions,
			fmt.Sprintf("%s%s=%s", config.SparkDriverAnnotationKeyPrefix, config.AffinityAnnotation,
				affinityString))
	}

	driverConfOptions = append(driverConfOptions, config.GetDriverSecretConfOptions(app)...)
	driverConfOptions = append(driverConfOptions, config.GetDriverConfigMapConfOptions(app)...)
	driverConfOptions = append(driverConfOptions, config.GetDriverEnvVarConfOptions(app)...)

	options, err := config.GetDriverVolumeMountConfOptions(app)
	if err != nil {
		return nil, err
	}
	driverConfOptions = append(driverConfOptions, options...)

	return driverConfOptions, nil
}

func addExecutorConfOptions(app *v1alpha1.SparkApplication) ([]string, error) {
	var executorConfOptions []string

	executorConfOptions = append(executorConfOptions,
		fmt.Sprintf("%s%s=%s", config.SparkExecutorLabelKeyPrefix, config.SparkAppNameLabel, app.Name))
	executorConfOptions = append(executorConfOptions,
		fmt.Sprintf("%s%s=%s", config.SparkExecutorLabelKeyPrefix, config.SparkAppIDLabel, app.Status.AppID))
	executorConfOptions = append(executorConfOptions,
		fmt.Sprintf("%s%s=%s", config.SparkExecutorLabelKeyPrefix, config.LaunchedBySparkOperatorLabel, "true"))

	if app.Spec.Executor.Instances != nil {
		conf := fmt.Sprintf("spark.executor.instances=%d", *app.Spec.Executor.Instances)
		executorConfOptions = append(executorConfOptions, conf)
	}

	if app.Spec.Executor.Image != nil {
		executorConfOptions = append(executorConfOptions,
			fmt.Sprintf("%s=%s", config.SparkExecutorContainerImageKey, *app.Spec.Executor.Image))
	}

	if app.Spec.Executor.CoreRequest != nil {
		executorConfOptions = append(executorConfOptions,
			fmt.Sprintf("%s=%s", config.SparkExecutorCoreRequestKey, *app.Spec.Executor.CoreRequest))
	}
	if app.Spec.Executor.Cores != nil {
		// Property "spark.executor.cores" does not allow float values.
		executorConfOptions = append(executorConfOptions,
			fmt.Sprintf("spark.executor.cores=%d", int32(*app.Spec.Executor.Cores)))
	}
	if app.Spec.Executor.CoreLimit != nil {
		executorConfOptions = append(executorConfOptions,
			fmt.Sprintf("%s=%s", config.SparkExecutorCoreLimitKey, *app.Spec.Executor.CoreLimit))
	}
	if app.Spec.Executor.Memory != nil {
		executorConfOptions = append(executorConfOptions,
			fmt.Sprintf("spark.executor.memory=%s", *app.Spec.Executor.Memory))
	}
	if app.Spec.Executor.MemoryOverhead != nil {
		executorConfOptions = append(executorConfOptions,
			fmt.Sprintf("spark.executor.memoryOverhead=%s", *app.Spec.Executor.MemoryOverhead))
	}

	for key, value := range app.Spec.Executor.Labels {
		executorConfOptions = append(executorConfOptions,
			fmt.Sprintf("%s%s=%s", config.SparkExecutorLabelKeyPrefix, key, value))
	}

	for key, value := range app.Spec.Executor.Annotations {
		executorConfOptions = append(executorConfOptions,
			fmt.Sprintf("%s%s=%s", config.SparkExecutorAnnotationKeyPrefix, key, value))
	}

	for key, value := range app.Spec.Executor.EnvSecretKeyRefs {
		executorConfOptions = append(executorConfOptions,
			fmt.Sprintf("%s%s=%s:%s", config.SparkExecutorSecretKeyRefKeyPrefix, key, value.Name, value.Key))
	}

	if app.Spec.Executor.Affinity != nil {
		affinityString, err := util.MarshalAffinity(app.Spec.Executor.Affinity)
		if err != nil {
			return nil, err
		}
		executorConfOptions = append(executorConfOptions,
			fmt.Sprintf("%s%s=%s", config.SparkExecutorAnnotationKeyPrefix, config.AffinityAnnotation,
				affinityString))
	}

	executorConfOptions = append(executorConfOptions, config.GetExecutorSecretConfOptions(app)...)
	executorConfOptions = append(executorConfOptions, config.GetExecutorConfigMapConfOptions(app)...)
	executorConfOptions = append(executorConfOptions, config.GetExecutorEnvVarConfOptions(app)...)

	options, err := config.GetExecutorVolumeMountConfOptions(app)
	if err != nil {
		return nil, err
	}
	executorConfOptions = append(executorConfOptions, options...)

	return executorConfOptions, nil
}
