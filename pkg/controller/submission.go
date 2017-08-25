package controller

import (
	"fmt"

	"github.com/liyinan926/spark-operator/pkg/apis/v1alpha1"
)

func buildSubmissionCommand() string {
	return ""
}

func addDriverEnvVar(apps *v1alpha1.SparkApplication) []string {
	envVarConfs := make([]string)
	for key, value := range apps.Spec.Driver.DriverEnvVars {
		envVarConfs = append(envVarConfs, fmt.Printf("--conf %s%s=%s", driverEnvVarConfigKeyPrefix, key, value))
	}
	return envVarConfs
}

func addExecutorEnvVar(apps *v1alpha1.SparkApplication) []string {
	envVarConfs := make([]string)
	for key, value := range apps.Spec.Executor.ExecutorEnvVars {
		envVarConfs = append(envVarConfs, fmt.Printf("--conf %s%s=%s", executorEnvVarConfigKeyPrefix, key, value))
	}
	return envVarConfs
}
