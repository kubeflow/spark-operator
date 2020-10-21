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

package config

import (
	"fmt"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/golang/glog"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	res "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

// GetDriverAnnotationOption returns a spark-submit option for a driver annotation of the given key and value.
func GetDriverAnnotationOption(key string, value string) string {
	return fmt.Sprintf("%s%s=%s", SparkDriverAnnotationKeyPrefix, key, value)
}

// GetExecutorAnnotationOption returns a spark-submit option for an executor annotation of the given key and value.
func GetExecutorAnnotationOption(key string, value string) string {
	return fmt.Sprintf("%s%s=%s", SparkExecutorAnnotationKeyPrefix, key, value)
}

// GetDriverEnvVarConfOptions returns a list of spark-submit options for setting driver environment variables.
func GetDriverEnvVarConfOptions(app *v1beta2.SparkApplication) []string {
	var envVarConfOptions []string
	for key, value := range app.Spec.Driver.EnvVars {
		envVar := fmt.Sprintf("%s%s=%s", SparkDriverEnvVarConfigKeyPrefix, key, value)
		envVarConfOptions = append(envVarConfOptions, envVar)
	}
	return envVarConfOptions
}

// GetExecutorEnvVarConfOptions returns a list of spark-submit options for setting executor environment variables.
func GetExecutorEnvVarConfOptions(app *v1beta2.SparkApplication) []string {
	var envVarConfOptions []string
	for key, value := range app.Spec.Executor.EnvVars {
		envVar := fmt.Sprintf("%s%s=%s", SparkExecutorEnvVarConfigKeyPrefix, key, value)
		envVarConfOptions = append(envVarConfOptions, envVar)
	}
	return envVarConfOptions
}

// GetPrometheusConfigMapName returns the name of the ConfigMap for Prometheus configuration.
func GetPrometheusConfigMapName(app *v1beta2.SparkApplication) string {
	return fmt.Sprintf("%s-%s", app.Name, PrometheusConfigMapNameSuffix)
}

// GetK8sConfigMap gets the cmName configmap in app.Namespace and returns the configMapPath
func GetK8sConfigMap(app *v1beta2.SparkApplication, cmName string) (string, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		glog.Errorf("%v", err)
		return "", err
	}

	clientset, err := res.NewForConfig(config)
	if err != nil {
		glog.Errorf("%v", err)
		return "", err
	}

	configMapsInNamespace := clientset.ConfigMaps(app.Namespace)
	userProvidedConfigMap, err := configMapsInNamespace.Get(cmName, metav1.GetOptions{})

	if err != nil {
		glog.Errorf("%v", err)
		return "", err
	}

	configFound := getCm{configMap: userProvidedConfigMap}

	configMapPath, err := copyToFile(configFound, app.Namespace, app.Name, cmName)
	if err != nil {
		glog.Errorf("%v", err)
		return "", err
	}

	return configMapPath, nil
}
