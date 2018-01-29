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

package controller

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	"k8s.io/spark-on-k8s-operator/pkg/config"
)

const (
	sparkHomeEnvVar             = "SPARK_HOME"
	kubernetesServiceHostEnvVar = "KUBERNETES_SERVICE_HOST"
	kubernetesServicePortEnvVar = "KUBERNETES_SERVICE_PORT"
)

// submission includes information of a Spark application to be submitted.
type submission struct {
	appName string
	appID   string
	args    []string
}

func newSubmission(args []string, app *v1alpha1.SparkApplication) *submission {
	return &submission{
		appName: app.Name,
		appID:   app.Status.AppID,
		args:    args,
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
	if app.Spec.ImagePullPolicy != nil {
		args = append(args, "--conf",
			fmt.Sprintf("%s=%s", config.SparkContainerImagePullPolicyKey, *app.Spec.ImagePullPolicy))
	}

	if app.Spec.SparkConfigMap != nil {
		config.AddConfigMapAnnotation(app, config.SparkDriverAnnotationKeyPrefix, config.SparkConfigMapAnnotation,
			*app.Spec.SparkConfigMap)
		config.AddConfigMapAnnotation(app, config.SparkExecutorAnnotationKeyPrefix, config.SparkConfigMapAnnotation,
			*app.Spec.SparkConfigMap)
	}

	if app.Spec.HadoopConfigMap != nil {
		config.AddConfigMapAnnotation(app, config.SparkDriverAnnotationKeyPrefix, config.HadoopConfigMapAnnotation,
			*app.Spec.HadoopConfigMap)
		config.AddConfigMapAnnotation(app, config.SparkExecutorAnnotationKeyPrefix, config.HadoopConfigMapAnnotation,
			*app.Spec.HadoopConfigMap)
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
	args = append(args, addDriverConfOptions(app)...)
	args = append(args, addExecutorConfOptions(app)...)

	reference := getOwnerReference(app)
	referenceData, err := reference.Marshal()
	if err != nil {
		return nil, err
	}
	args = append(args, "--conf", fmt.Sprintf("%s%s=%s", config.SparkDriverAnnotationKeyPrefix,
		config.OwnerReferenceAnnotation, string(referenceData)))

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

func getOwnerReference(app *v1alpha1.SparkApplication) metav1.OwnerReference {
	return metav1.OwnerReference{
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

func addDriverConfOptions(app *v1alpha1.SparkApplication) []string {
	var driverConfOptions []string

	driverConfOptions = append(
		driverConfOptions,
		"--conf",
		fmt.Sprintf("%s%s=%s", config.SparkDriverLabelKeyPrefix, config.SparkAppIDLabel, app.Status.AppID))

	if app.Spec.Driver.PodName != nil {
		driverConfOptions = append(
			driverConfOptions,
			"--conf",
			fmt.Sprintf("%s=%s", config.SparkDriverPodNameKey, *app.Spec.Driver.PodName))
	}

	if app.Spec.Driver.Image != nil {
		driverConfOptions = append(
			driverConfOptions,
			"--conf",
			fmt.Sprintf("%s=%s", config.SparkDriverContainerImageKey, *app.Spec.Driver.Image))
	}

	if app.Spec.Driver.Cores != nil {
		driverConfOptions = append(driverConfOptions, "--conf",
			fmt.Sprintf("spark.driver.cores=%s", *app.Spec.Driver.Cores))
	}
	if app.Spec.Driver.CoreLimit != nil {
		driverConfOptions = append(driverConfOptions, "--conf",
			fmt.Sprintf("%s=%s", config.SparkDriverCoreLimitKey, *app.Spec.Driver.CoreLimit))
	}
	if app.Spec.Driver.Memory != nil {
		driverConfOptions = append(driverConfOptions, "--conf",
			fmt.Sprintf("spark.driver.memory=%s", *app.Spec.Driver.Memory))
	}

	if app.Spec.Driver.ServiceAccount != nil {
		driverConfOptions = append(driverConfOptions, "--conf",
			fmt.Sprintf("%s=%s", config.SparkDriverServiceAccountName, *app.Spec.Driver.ServiceAccount))
	}

	for key, value := range app.Spec.Driver.Labels {
		driverConfOptions = append(driverConfOptions, "--conf",
			fmt.Sprintf("%s%s=%s", config.SparkDriverLabelKeyPrefix, key, value))
	}

	for key, value := range app.Spec.Driver.Annotations {
		driverConfOptions = append(driverConfOptions, "--conf",
			fmt.Sprintf("%s%s=%s", config.SparkDriverAnnotationKeyPrefix, key, value))
	}

	driverConfOptions = append(driverConfOptions, getDriverEnvVarConfOptions(app)...)
	driverConfOptions = append(driverConfOptions, getDriverSecretConfOptions(app)...)

	return driverConfOptions
}

func addExecutorConfOptions(app *v1alpha1.SparkApplication) []string {
	var executorConfOptions []string

	executorConfOptions = append(
		executorConfOptions,
		"--conf",
		fmt.Sprintf("%s%s=%s", config.SparkExecutorLabelKeyPrefix, config.SparkAppIDLabel, app.Status.AppID))

	if app.Spec.Executor.Image != nil {
		executorConfOptions = append(
			executorConfOptions,
			"--conf",
			fmt.Sprintf("%s=%s", config.SparkExecutorContainerImageKey, *app.Spec.Executor.Image))
	}

	if app.Spec.Executor.Instances != nil {
		conf := fmt.Sprintf("spark.executor.instances=%d", *app.Spec.Executor.Instances)
		executorConfOptions = append(executorConfOptions, "--conf", conf)
	}

	if app.Spec.Executor.Cores != nil {
		conf := fmt.Sprintf("spark.executor.cores=%s", *app.Spec.Executor.Cores)
		executorConfOptions = append(executorConfOptions, "--conf", conf)
	}
	if app.Spec.Executor.CoreLimit != nil {
		conf := fmt.Sprintf("%s=%s", config.SparkExecutorCoreLimitKey, *app.Spec.Executor.CoreLimit)
		executorConfOptions = append(executorConfOptions, "--conf", conf)
	}
	if app.Spec.Executor.Memory != nil {
		conf := fmt.Sprintf("spark.executor.memory=%s", *app.Spec.Executor.Memory)
		executorConfOptions = append(executorConfOptions, "--conf", conf)
	}

	for key, value := range app.Spec.Executor.Labels {
		executorConfOptions = append(executorConfOptions, "--conf",
			fmt.Sprintf("%s%s=%s", config.SparkExecutorLabelKeyPrefix, key, value))
	}

	for key, value := range app.Spec.Executor.Annotations {
		executorConfOptions = append(executorConfOptions, "--conf",
			fmt.Sprintf("%s%s=%s", config.SparkExecutorAnnotationKeyPrefix, key, value))
	}

	executorConfOptions = append(executorConfOptions, getExecutorEnvVarConfOptions(app)...)
	executorConfOptions = append(executorConfOptions, getExecutorSecretConfOptions(app)...)

	return executorConfOptions
}

func getDriverSecretConfOptions(app *v1alpha1.SparkApplication) []string {
	var secretConfOptions []string
	for _, secret := range app.Spec.Driver.Secrets {
		if secret.Type == v1alpha1.GCPServiceAccountSecret {
			conf := fmt.Sprintf("%s%s%s=%s",
				config.SparkDriverAnnotationKeyPrefix,
				config.GCPServiceAccountSecretAnnotationPrefix,
				secret.Name,
				secret.Path)
			secretConfOptions = append(secretConfOptions, "--conf", conf)
		} else {
			conf := fmt.Sprintf("%s%s%s=%s",
				config.SparkDriverAnnotationKeyPrefix,
				config.GeneralSecretsAnnotationPrefix,
				secret.Name,
				secret.Path)
			secretConfOptions = append(secretConfOptions, "--conf", conf)
		}
	}
	return secretConfOptions
}

func getExecutorSecretConfOptions(app *v1alpha1.SparkApplication) []string {
	var secretConfOptions []string
	for _, secret := range app.Spec.Executor.Secrets {
		if secret.Type == v1alpha1.GCPServiceAccountSecret {
			conf := fmt.Sprintf("%s%s%s=%s",
				config.SparkExecutorAnnotationKeyPrefix,
				config.GCPServiceAccountSecretAnnotationPrefix,
				secret.Name,
				secret.Path)
			secretConfOptions = append(secretConfOptions, "--conf", conf)
		} else {
			conf := fmt.Sprintf("%s%s%s=%s",
				config.SparkExecutorAnnotationKeyPrefix,
				config.GeneralSecretsAnnotationPrefix,
				secret.Name,
				secret.Path)
			secretConfOptions = append(secretConfOptions, "--conf", conf)
		}
	}
	return secretConfOptions
}

func getDriverEnvVarConfOptions(app *v1alpha1.SparkApplication) []string {
	var envVarConfOptions []string
	for key, value := range app.Spec.Driver.EnvVars {
		envVar := fmt.Sprintf("%s%s=%s", config.DriverEnvVarConfigKeyPrefix, key, value)
		envVarConfOptions = append(envVarConfOptions, "--conf", envVar)
	}
	return envVarConfOptions
}

func getExecutorEnvVarConfOptions(app *v1alpha1.SparkApplication) []string {
	var envVarConfOptions []string
	for key, value := range app.Spec.Executor.EnvVars {
		envVar := fmt.Sprintf("%s%s=%s", config.ExecutorEnvVarConfigKeyPrefix, key, value)
		envVarConfOptions = append(envVarConfOptions, "--conf", envVar)
	}
	return envVarConfOptions
}
