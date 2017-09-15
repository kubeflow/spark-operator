package controller

import (
	"fmt"
	"os"
	"strings"

	"github.com/liyinan926/spark-operator/pkg/apis/v1alpha1"
	"github.com/liyinan926/spark-operator/pkg/config"
)

const (
	kubernetesServiceHostEnvVar = "KUBERNETES_SERVICE_HOST"
	kubernetesServicePortEnvVar = "KUBERNETES_SERVICE_PORT"
)

func buildSubmissionCommand(app *v1alpha1.SparkApplication) (string, error) {
	var command string
	if app.Spec.MainClass != nil {
		command += fmt.Sprintf(" --class %s", *app.Spec.MainClass)
	}
	masterURL, err := getMasterURL()
	if err != nil {
		return "", nil
	}
	command += fmt.Sprintf(" --master %s", masterURL)
	command += fmt.Sprintf(" --kubernetes-namespace %s", app.Namespace)
	command += " --deploy-mode cluster"
	// Add application dependencies.
	if len(app.Spec.Deps.JarFiles) > 0 {
		command += fmt.Sprintf(" --jars %s", strings.Join(app.Spec.Deps.JarFiles, ","))
	}
	if len(app.Spec.Deps.JarFiles) > 0 {
		command += fmt.Sprintf(" --files %s", strings.Join(app.Spec.Deps.Files, ","))
	}
	if len(app.Spec.Deps.JarFiles) > 0 {
		command += fmt.Sprintf(" --py-files %s", strings.Join(app.Spec.Deps.PyFiles, ","))
	}
	if app.Spec.SparkConfigMap != nil {
		config.AddConfigMapAnnotation(app, config.SparkDriverAnnotationKeyPrefix, config.SparkConfigMapAnnotation, *app.Spec.SparkConfigMap)
		config.AddConfigMapAnnotation(app, config.SparkExecutorAnnotationKeyPrefix, config.SparkConfigMapAnnotation, *app.Spec.SparkConfigMap)
	}
	if app.Spec.HadoopConfigMap != nil {
		config.AddConfigMapAnnotation(app, config.SparkDriverAnnotationKeyPrefix, config.HadoopConfigMapAnnotation, *app.Spec.HadoopConfigMap)
		config.AddConfigMapAnnotation(app, config.SparkExecutorAnnotationKeyPrefix, config.HadoopConfigMapAnnotation, *app.Spec.HadoopConfigMap)
	}
	// Add Spark configuration properties.
	for key, value := range app.Spec.SparkConf {
		command += fmt.Sprintf(" --conf %s=%s", key, value)
	}
	// Add driver and executor environment variables using the --conf option.
	for _, conf := range getDriverEnvVarConfOptions(app) {
		command += conf
	}
	for _, conf := range getExecutorEnvVarConfOptions(app) {
		command += conf
	}
	// Add the main application file.
	command += " " + app.Spec.MainApplicationFile
	// Add application arguments.
	for _, argument := range app.Spec.Arguments {
		command += " " + argument
	}
	return command, nil
}

func getMasterURL() (string, error) {
	kubernetesServiceHost := os.Getenv(kubernetesServiceHostEnvVar)
	if kubernetesServiceHost == "" {
		return "", fmt.Errorf("Environment variable %s is not found", kubernetesServiceHostEnvVar)
	}
	kubernetesServicePort := os.Getenv(kubernetesServicePortEnvVar)
	if kubernetesServicePort == "" {
		return "", fmt.Errorf("Environment variable %s is not found", kubernetesServicePortEnvVar)
	}
	return fmt.Sprintf("k8s://%s:%s", kubernetesServiceHost, kubernetesServicePort), nil
}

func getDriverEnvVarConfOptions(app *v1alpha1.SparkApplication) []string {
	var envVarConfs []string
	for key, value := range app.Spec.Driver.DriverEnvVars {
		envVar := fmt.Sprintf(" --conf %s%s=%s", config.DriverEnvVarConfigKeyPrefix, key, value)
		envVarConfs = append(envVarConfs, envVar)
	}
	return envVarConfs
}

func getExecutorEnvVarConfOptions(app *v1alpha1.SparkApplication) []string {
	var envVarConfs []string
	for key, value := range app.Spec.Executor.ExecutorEnvVars {
		envVar := fmt.Sprintf(" --conf %s%s=%s", config.ExecutorEnvVarConfigKeyPrefix, key, value)
		envVarConfs = append(envVarConfs, envVar)
	}
	return envVarConfs
}
