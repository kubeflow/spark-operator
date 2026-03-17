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
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

func configPrometheusMonitoring(ctx context.Context, app *v1beta2.SparkApplication, c client.Client) error {
	logger := log.FromContext(ctx)
	port := common.DefaultPrometheusJavaAgentPort
	if app.Spec.Monitoring.Prometheus != nil && app.Spec.Monitoring.Prometheus.Port != nil {
		port = *app.Spec.Monitoring.Prometheus.Port
	}

	// If one or both of the metricsPropertiesFile and Prometheus.ConfigFile are not set
	if !util.HasMetricsPropertiesFile(app) || !util.HasPrometheusConfigFile(app) {
		configMapName := util.GetPrometheusConfigMapName(app)
		configMap := buildPrometheusConfigMap(app, configMapName)
		key := types.NamespacedName{Namespace: configMap.Namespace, Name: configMap.Name}
		if retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			cm := &corev1.ConfigMap{}
			if err := c.Get(ctx, key, cm); err != nil {
				if errors.IsNotFound(err) {
					if createErr := c.Create(ctx, configMap); createErr != nil {
						// Handle upgrade scenario: ConfigMap exists in the cluster but is not
						// visible in the filtered cache because it was created before the
						// LabelCreatedBySparkOperator label selector was added to the informer.
						// Use Patch (merge patch) instead of Update because we cannot Get the
						// object from the filtered cache to obtain its resourceVersion.
						if errors.IsAlreadyExists(createErr) {
							base := &corev1.ConfigMap{
								ObjectMeta: metav1.ObjectMeta{
									Name:      key.Name,
									Namespace: key.Namespace,
								},
							}
							desired := base.DeepCopy()
							desired.Labels = map[string]string{
								common.LabelCreatedBySparkOperator: "true",
							}
							desired.Data = configMap.Data
							desired.OwnerReferences = configMap.OwnerReferences
							return c.Patch(ctx, desired, client.MergeFrom(base))
						}
						return createErr
					}
					return nil
				}
				return err
			}
			cm.Data = configMap.Data
			if cm.Labels == nil {
				cm.Labels = map[string]string{}
			}
			cm.Labels[common.LabelCreatedBySparkOperator] = "true"
			return c.Update(ctx, cm)
		}); retryErr != nil {
			return retryErr
		}
	}

	var javaOption string

	if app.Spec.Monitoring.Prometheus != nil {
		javaOption = fmt.Sprintf(
			"-javaagent:%s=%d:%s/%s",
			app.Spec.Monitoring.Prometheus.JmxExporterJar,
			port,
			common.PrometheusConfigMapMountPath,
			common.PrometheusConfigKey)
	}

	if util.HasPrometheusConfigFile(app) {
		configFile := *app.Spec.Monitoring.Prometheus.ConfigFile
		logger.V(1).Info("Overriding the default Prometheus configuration with config file in the Spark image.", "configFile", configFile)
		javaOption = fmt.Sprintf("-javaagent:%s=%d:%s", app.Spec.Monitoring.Prometheus.JmxExporterJar, port, configFile)
	}

	/* work around for push gateway issue: https://github.com/prometheus/pushgateway/issues/97 */
	metricNamespace := fmt.Sprintf("%s.%s", app.Namespace, app.Name)
	metricConf := fmt.Sprintf("%s/%s", common.PrometheusConfigMapMountPath, common.MetricsPropertiesKey)
	if app.Spec.SparkConf == nil {
		app.Spec.SparkConf = make(map[string]string)
	}
	app.Spec.SparkConf["spark.metrics.namespace"] = metricNamespace
	app.Spec.SparkConf["spark.metrics.conf"] = metricConf

	if util.HasMetricsPropertiesFile(app) {
		app.Spec.SparkConf["spark.metrics.conf"] = *app.Spec.Monitoring.MetricsPropertiesFile
	}

	if app.Spec.Monitoring.ExposeDriverMetrics {
		if app.Spec.Driver.Annotations == nil {
			app.Spec.Driver.Annotations = make(map[string]string)
		}
		app.Spec.Driver.Annotations[common.PrometheusScrapeAnnotation] = "true"
		app.Spec.Driver.Annotations[common.PrometheusPortAnnotation] = fmt.Sprintf("%d", port)
		app.Spec.Driver.Annotations[common.PrometheusPathAnnotation] = "/metrics"

		if app.Spec.Driver.JavaOptions == nil {
			app.Spec.Driver.JavaOptions = &javaOption
		} else {
			*app.Spec.Driver.JavaOptions = *app.Spec.Driver.JavaOptions + " " + javaOption
		}
	}
	if app.Spec.Monitoring.ExposeExecutorMetrics {
		if app.Spec.Executor.Annotations == nil {
			app.Spec.Executor.Annotations = make(map[string]string)
		}
		app.Spec.Executor.Annotations[common.PrometheusScrapeAnnotation] = "true"
		app.Spec.Executor.Annotations[common.PrometheusPortAnnotation] = fmt.Sprintf("%d", port)
		app.Spec.Executor.Annotations[common.PrometheusPathAnnotation] = "/metrics"

		if app.Spec.Executor.JavaOptions == nil {
			app.Spec.Executor.JavaOptions = &javaOption
		} else {
			*app.Spec.Executor.JavaOptions = *app.Spec.Executor.JavaOptions + " " + javaOption
		}
	}

	return nil
}

func buildPrometheusConfigMap(app *v1beta2.SparkApplication, prometheusConfigMapName string) *corev1.ConfigMap {
	configMapData := make(map[string]string)

	if !util.HasMetricsPropertiesFile(app) {
		metricsProperties := common.DefaultMetricsProperties
		if app.Spec.Monitoring.MetricsProperties != nil {
			metricsProperties = *app.Spec.Monitoring.MetricsProperties
		}
		configMapData[common.MetricsPropertiesKey] = metricsProperties
	}

	if app.Spec.Monitoring.Prometheus != nil && !util.HasPrometheusConfigFile(app) {
		prometheusConfig := common.DefaultPrometheusConfiguration
		if app.Spec.Monitoring.Prometheus.Configuration != nil {
			prometheusConfig = *app.Spec.Monitoring.Prometheus.Configuration
		}
		configMapData[common.PrometheusConfigKey] = prometheusConfig
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            prometheusConfigMapName,
			Namespace:       app.Namespace,
			OwnerReferences: []metav1.OwnerReference{util.GetOwnerReference(app)},
			Labels: map[string]string{
				common.LabelCreatedBySparkOperator: "true",
			},
		},
		Data: configMapData,
	}
}
