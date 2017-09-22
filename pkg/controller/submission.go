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
	var command = "bin/spark-submit \\\n"
	if app.Spec.MainClass != nil {
		command += fmt.Sprintf(" --class %s \\\n", *app.Spec.MainClass)
	}
	masterURL, err := getMasterURL()
	if err != nil {
		return "", nil
	}

	command += fmt.Sprintf(" --master %s \\\n", masterURL)
	command += fmt.Sprintf(" --kubernetes-namespace %s \\\n", app.Namespace)
	command += fmt.Sprintf(" --deploy-mode %s \\\n", app.Spec.Mode)

	// Add application dependencies.
	if len(app.Spec.Deps.JarFiles) > 0 {
		command += fmt.Sprintf(" --jars %s \\\n", strings.Join(app.Spec.Deps.JarFiles, ","))
	}
	if len(app.Spec.Deps.JarFiles) > 0 {
		command += fmt.Sprintf(" --files %s \\\n", strings.Join(app.Spec.Deps.Files, ","))
	}
	if len(app.Spec.Deps.JarFiles) > 0 {
		command += fmt.Sprintf(" --py-files %s \\\n", strings.Join(app.Spec.Deps.PyFiles, ","))
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
		command += fmt.Sprintf(" --conf %s=%s \\\n", key, value)
	}

	// Add driver and executor environment variables using the --conf option.
	for _, conf := range getDriverEnvVarConfOptions(app) {
		command += conf
	}
	for _, conf := range getExecutorEnvVarConfOptions(app) {
		command += conf
	}

	// Add driver and executor secret annotations using the --conf option.
	for _, conf := range getDriverSecretConfOptions(app) {
		command += conf
	}
	for _, conf := range getExecutorSecretConfOptions(app) {
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

func getDriverSecretConfOptions(app *v1alpha1.SparkApplication) []string {
	var secretConfs []string
	for _, secret := range app.Spec.Driver.DriverSecrets {
		if secret.Type == v1alpha1.GCPServiceAccountSecret {
			conf := fmt.Sprintf(" --conf %s%s%s=%s \\\n",
				config.SparkDriverAnnotationKeyPrefix,
				config.GCPServiceAccountSecretAnnotationPrefix,
				secret.Name,
				secret.Path)
			secretConfs = append(secretConfs, conf)
		} else {
			conf := fmt.Sprintf(" --conf %s%s%s=%s \\\n",
				config.SparkDriverAnnotationKeyPrefix,
				config.GeneralSecretsAnnotationPrefix,
				secret.Name,
				secret.Path)
			secretConfs = append(secretConfs, conf)
		}
	}
	return secretConfs
}

func getExecutorSecretConfOptions(app *v1alpha1.SparkApplication) []string {
	var secretConfs []string
	for _, secret := range app.Spec.Executor.ExecutorSecrets {
		if secret.Type == v1alpha1.GCPServiceAccountSecret {
			conf := fmt.Sprintf(" --conf %s%s%s=%s \\\n",
				config.SparkExecutorAnnotationKeyPrefix,
				config.GCPServiceAccountSecretAnnotationPrefix,
				secret.Name,
				secret.Path)
			secretConfs = append(secretConfs, conf)
		} else {
			conf := fmt.Sprintf(" --conf %s%s%s=%s \\\n",
				config.SparkExecutorAnnotationKeyPrefix,
				config.GeneralSecretsAnnotationPrefix,
				secret.Name,
				secret.Path)
			secretConfs = append(secretConfs, conf)
		}
	}
	return secretConfs
}

func getDriverEnvVarConfOptions(app *v1alpha1.SparkApplication) []string {
	var envVarConfs []string
	for key, value := range app.Spec.Driver.DriverEnvVars {
		envVar := fmt.Sprintf(" --conf %s%s=%s \\\n", config.DriverEnvVarConfigKeyPrefix, key, value)
		envVarConfs = append(envVarConfs, envVar)
	}
	return envVarConfs
}

func getExecutorEnvVarConfOptions(app *v1alpha1.SparkApplication) []string {
	var envVarConfs []string
	for key, value := range app.Spec.Executor.ExecutorEnvVars {
		envVar := fmt.Sprintf(" --conf %s%s=%s \\\n", config.ExecutorEnvVarConfigKeyPrefix, key, value)
		envVarConfs = append(envVarConfs, envVar)
	}
	return envVarConfs
}
