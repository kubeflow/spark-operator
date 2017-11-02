package controller

import (
	"fmt"
	"os"
	"strings"

	"github.com/liyinan926/spark-operator/pkg/apis/v1alpha1"
	"github.com/liyinan926/spark-operator/pkg/config"
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
	args = append(args, "--kubernetes-namespace", app.Namespace)
	args = append(args, "--deploy-mode", string(app.Spec.Mode))
	args = append(args, "--conf", fmt.Sprintf("spark.app.name=%s", app.Name))
	args = append(args, "--conf", fmt.Sprintf("spark.executor.instances=%d", app.Spec.Executor.Instances))

	// Add application dependencies.
	if len(app.Spec.Deps.JarFiles) > 0 {
		args = append(args, "--jars", strings.Join(app.Spec.Deps.JarFiles, ","))
	}
	if len(app.Spec.Deps.JarFiles) > 0 {
		args = append(args, "--files", strings.Join(app.Spec.Deps.Files, ","))
	}
	if len(app.Spec.Deps.JarFiles) > 0 {
		args = append(args, "--py-files", strings.Join(app.Spec.Deps.PyFiles, ","))
	}

	// Add Spark configuration properties.
	for key, value := range app.Spec.SparkConf {
		args = append(args, "--conf", fmt.Sprintf("%s=%s", key, value))
	}

	// Add the application ID label.
	args = append(args, "--conf", fmt.Sprintf("%s%s=%s", config.SparkDriverLabelKeyPrefix, config.SparkAppIDLabel, app.Status.AppID))
	args = append(args, "--conf", fmt.Sprintf("%s%s=%s", config.SparkExecutorLabelKeyPrefix, config.SparkAppIDLabel, app.Status.AppID))

	if app.Spec.SparkConfigMap != nil {
		config.AddConfigMapAnnotation(app, config.SparkDriverAnnotationKeyPrefix, config.SparkConfigMapAnnotation, *app.Spec.SparkConfigMap)
		config.AddConfigMapAnnotation(app, config.SparkExecutorAnnotationKeyPrefix, config.SparkConfigMapAnnotation, *app.Spec.SparkConfigMap)
	}
	if app.Spec.HadoopConfigMap != nil {
		config.AddConfigMapAnnotation(app, config.SparkDriverAnnotationKeyPrefix, config.HadoopConfigMapAnnotation, *app.Spec.HadoopConfigMap)
		config.AddConfigMapAnnotation(app, config.SparkExecutorAnnotationKeyPrefix, config.HadoopConfigMapAnnotation, *app.Spec.HadoopConfigMap)
	}

	// Add driver and executor environment variables configuration option.
	for _, conf := range getDriverEnvVarConfOptions(app) {
		args = append(args, conf)
	}
	for _, conf := range getExecutorEnvVarConfOptions(app) {
		args = append(args, conf)
	}

	// Add driver and executor secret annotations configuration option.
	for _, conf := range getDriverSecretConfOptions(app) {
		args = append(args, conf)
	}
	for _, conf := range getExecutorSecretConfOptions(app) {
		args = append(args, conf)
	}

	// Add the driver and executor Docker image configuration options.
	// Note that when the controller submits the application, it expects that all dependencies are local
	// so init-container is not needed and therefore no init-container image needs to be specified.
	args = append(args, "--conf", fmt.Sprintf("spark.kubernetes.driver.docker.image=%s", app.Spec.Driver.Image))
	args = append(args, "--conf", fmt.Sprintf("spark.kubernetes.executor.docker.image=%s", app.Spec.Executor.Image))

	// Add the main application file.
	args = append(args, app.Spec.MainApplicationFile)
	// Add application arguments.
	for _, argument := range app.Spec.Arguments {
		args = append(args, argument)
	}

	return args, nil
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
	return fmt.Sprintf("k8s://https://%s:%s", kubernetesServiceHost, kubernetesServicePort), nil
}

func getDriverSecretConfOptions(app *v1alpha1.SparkApplication) []string {
	var secretConfs []string
	for _, secret := range app.Spec.Driver.DriverSecrets {
		if secret.Type == v1alpha1.GCPServiceAccountSecret {
			conf := fmt.Sprintf("%s%s%s=%s",
				config.SparkDriverAnnotationKeyPrefix,
				config.GCPServiceAccountSecretAnnotationPrefix,
				secret.Name,
				secret.Path)
			secretConfs = append(secretConfs, "--conf", conf)
		} else {
			conf := fmt.Sprintf("%s%s%s=%s",
				config.SparkDriverAnnotationKeyPrefix,
				config.GeneralSecretsAnnotationPrefix,
				secret.Name,
				secret.Path)
			secretConfs = append(secretConfs, "--conf", conf)
		}
	}
	return secretConfs
}

func getExecutorSecretConfOptions(app *v1alpha1.SparkApplication) []string {
	var secretConfs []string
	for _, secret := range app.Spec.Executor.ExecutorSecrets {
		if secret.Type == v1alpha1.GCPServiceAccountSecret {
			conf := fmt.Sprintf("%s%s%s=%s",
				config.SparkExecutorAnnotationKeyPrefix,
				config.GCPServiceAccountSecretAnnotationPrefix,
				secret.Name,
				secret.Path)
			secretConfs = append(secretConfs, "--conf", conf)
		} else {
			conf := fmt.Sprintf("%s%s%s=%s",
				config.SparkExecutorAnnotationKeyPrefix,
				config.GeneralSecretsAnnotationPrefix,
				secret.Name,
				secret.Path)
			secretConfs = append(secretConfs, "--conf", conf)
		}
	}
	return secretConfs
}

func getDriverEnvVarConfOptions(app *v1alpha1.SparkApplication) []string {
	var envVarConfs []string
	for key, value := range app.Spec.Driver.DriverEnvVars {
		envVar := fmt.Sprintf("%s%s=%s", config.DriverEnvVarConfigKeyPrefix, key, value)
		envVarConfs = append(envVarConfs, "--conf", envVar)
	}
	return envVarConfs
}

func getExecutorEnvVarConfOptions(app *v1alpha1.SparkApplication) []string {
	var envVarConfs []string
	for key, value := range app.Spec.Executor.ExecutorEnvVars {
		envVar := fmt.Sprintf("%s%s=%s", config.ExecutorEnvVarConfigKeyPrefix, key, value)
		envVarConfs = append(envVarConfs, "--conf", envVar)
	}
	return envVarConfs
}
