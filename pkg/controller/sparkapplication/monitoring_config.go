/*
Copyright 2018 Google LLC

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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
)

const (
	metricsPropertiesKey          = "metrics.properties"
	prometheusConfigKey           = "prometheus.yaml"
	prometheusConfigMapNameSuffix = "prometheus-config"
	prometheusConfigMapMountPath  = "/etc/metrics/conf"
	prometheusScrapeAnnotation    = "prometheus.io/scrape"
	prometheusPortAnnotation      = "prometheus.io/port"
	prometheusPathAnnotation      = "prometheus.io/path"
)

func configPrometheusMonitoring(app *v1alpha1.SparkApplication, kubeClient clientset.Interface) error {
	prometheusConfigMapName := fmt.Sprintf("%s-%s", app.Name, prometheusConfigMapNameSuffix)
	configMap := buildPrometheusConfigMap(app, prometheusConfigMapName)
	if _, err := kubeClient.CoreV1().ConfigMaps(app.Namespace).Create(configMap); err != nil {
		return err
	}

	metricNamespace := fmt.Sprintf("%s/%s", app.Namespace, app.Name)
	metricConf := fmt.Sprintf("%s/%s", prometheusConfigMapMountPath, metricsPropertiesKey)
	if app.Spec.SparkConf == nil {
		app.Spec.SparkConf = make(map[string]string)
	}
	app.Spec.SparkConf["spark.metrics.namespace"] = metricNamespace
	app.Spec.SparkConf["spark.metrics.conf"] = metricConf

	port := config.DefaultPrometheusJavaAgentPort
	if app.Spec.Monitoring.Prometheus.Port != nil {
		port = *app.Spec.Monitoring.Prometheus.Port
	}
	javaOption := fmt.Sprintf("-javaagent:%s=%d:%s/%s", app.Spec.Monitoring.Prometheus.JmxExporterJar,
		port, prometheusConfigMapMountPath, prometheusConfigKey)

	if app.Spec.Monitoring.ExposeDriverMetrics {
		app.Spec.Driver.ConfigMaps = append(app.Spec.Driver.ConfigMaps, v1alpha1.NamePath{
			Name: prometheusConfigMapName,
			Path: prometheusConfigMapMountPath,
		})

		if app.Spec.Driver.Annotations == nil {
			app.Spec.Driver.Annotations = make(map[string]string)
		}
		app.Spec.Driver.Annotations[prometheusScrapeAnnotation] = "true"
		app.Spec.Driver.Annotations[prometheusPortAnnotation] = fmt.Sprintf("%d", port)
		app.Spec.Driver.Annotations[prometheusPathAnnotation] = "/metrics"

		if app.Spec.Driver.JavaOptions == nil {
			app.Spec.Driver.JavaOptions = &javaOption
		} else {
			*app.Spec.Driver.JavaOptions = *app.Spec.Driver.JavaOptions + " " + javaOption
		}
	}
	if app.Spec.Monitoring.ExposeExecutorMetrics {
		app.Spec.Executor.ConfigMaps = append(app.Spec.Executor.ConfigMaps, v1alpha1.NamePath{
			Name: prometheusConfigMapName,
			Path: prometheusConfigMapMountPath,
		})

		if app.Spec.Executor.Annotations == nil {
			app.Spec.Executor.Annotations = make(map[string]string)
		}
		app.Spec.Executor.Annotations[prometheusScrapeAnnotation] = "true"
		app.Spec.Executor.Annotations[prometheusPortAnnotation] = fmt.Sprintf("%d", port)
		app.Spec.Executor.Annotations[prometheusPathAnnotation] = "/metrics"

		if app.Spec.Executor.JavaOptions == nil {
			app.Spec.Executor.JavaOptions = &javaOption
		} else {
			*app.Spec.Executor.JavaOptions = *app.Spec.Executor.JavaOptions + " " + javaOption
		}
	}

	return nil
}

func buildPrometheusConfigMap(app *v1alpha1.SparkApplication, prometheusConfigMapName string) *corev1.ConfigMap {
	metricsProperties := config.DefaultMetricsProperties
	if app.Spec.Monitoring.MetricsProperties != nil {
		metricsProperties = *app.Spec.Monitoring.MetricsProperties
	}
	prometheusConfig := config.DefaultPrometheusConfiguration
	if app.Spec.Monitoring.Prometheus.Configuration != nil {
		prometheusConfig = *app.Spec.Monitoring.Prometheus.Configuration
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            prometheusConfigMapName,
			Namespace:       app.Namespace,
			OwnerReferences: []metav1.OwnerReference{*getOwnerReference(app)},
		},
		Data: map[string]string{
			metricsPropertiesKey: metricsProperties,
			prometheusConfigKey:  prometheusConfig,
		},
	}
}
