package controller

import (
	"fmt"

	"github.com/liyinan926/spark-operator/pkg/apis/v1alpha1"
)

func buildSubmissionCommand(app *v1alpha1.SparkApplication) string {
	var command string
	for _, conf := range addDriverEnvVar(app) {
		command += conf
	}
	for _, conf := range addExecutorEnvVar(app) {
		command += conf
	}
	return command
}

func addDriverEnvVar(app *v1alpha1.SparkApplication) []string {
	var envVarConfs []string
	for key, value := range app.Spec.Driver.DriverEnvVars {
		envVarConfs = append(envVarConfs, fmt.Sprintf(" --conf %s%s=%s ", driverEnvVarConfigKeyPrefix, key, value))
	}
	return envVarConfs
}

func addExecutorEnvVar(app *v1alpha1.SparkApplication) []string {
	var envVarConfs []string
	for key, value := range app.Spec.Executor.ExecutorEnvVars {
		envVarConfs = append(envVarConfs, fmt.Sprintf(" --conf %s%s=%s ", executorEnvVarConfigKeyPrefix, key, value))
	}
	return envVarConfs
}
