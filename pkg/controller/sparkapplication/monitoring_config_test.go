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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
)

func TestConfigPrometheusMonitoring(t *testing.T) {
	type testcase struct {
		app                   *v1beta2.SparkApplication
		metricsProperties     string
		metricsPropertiesFile string
		prometheusConfig      string
		port                  string
		driverJavaOptions     string
		executorJavaOptions   string
	}

	fakeClient := fake.NewSimpleClientset()
	testFn := func(test testcase, t *testing.T) {
		err := configPrometheusMonitoring(test.app, fakeClient)
		if err != nil {
			t.Errorf("failed to configure Prometheus monitoring: %v", err)
		}

		configMapName := config.GetPrometheusConfigMapName(test.app)
		configMap, err := fakeClient.CoreV1().ConfigMaps(test.app.Namespace).Get(context.TODO(), configMapName, metav1.GetOptions{})
		if err != nil {
			t.Errorf("failed to get ConfigMap %s: %v", configMapName, err)
		}

		if test.app.Spec.Monitoring.Prometheus.ConfigFile == nil &&
			test.app.Spec.Monitoring.MetricsPropertiesFile == nil &&
			len(configMap.Data) != 2 {
			t.Errorf("expected %d data items got %d", 2, len(configMap.Data))
		}

		if test.app.Spec.Monitoring.Prometheus.ConfigFile != nil &&
			test.app.Spec.Monitoring.MetricsPropertiesFile == nil &&
			len(configMap.Data) != 1 {
			t.Errorf("expected %d data items got %d", 1, len(configMap.Data))
		}

		if test.app.Spec.Monitoring.Prometheus.ConfigFile == nil &&
			test.app.Spec.Monitoring.MetricsPropertiesFile != nil &&
			len(configMap.Data) != 1 {
			t.Errorf("expected %d data items got %d", 1, len(configMap.Data))
		}

		if test.app.Spec.Monitoring.MetricsPropertiesFile == nil && configMap.Data[metricsPropertiesKey] != test.metricsProperties {
			t.Errorf("metrics.properties expected %s got %s", test.metricsProperties, configMap.Data[metricsPropertiesKey])
		}

		if test.app.Spec.Monitoring.Prometheus.ConfigFile == nil && configMap.Data[prometheusConfigKey] != test.prometheusConfig {
			t.Errorf("prometheus.yaml expected %s got %s", test.prometheusConfig, configMap.Data[prometheusConfigKey])
		}

		if test.app.Spec.Monitoring.Prometheus.ConfigFile == nil && configMap.Data[prometheusConfigKey] != test.prometheusConfig {
			t.Errorf("prometheus.yaml expected %s got %s", test.prometheusConfig, configMap.Data[prometheusConfigKey])
		}

		if test.app.Spec.Monitoring.ExposeDriverMetrics {
			if len(test.app.Spec.Driver.Annotations) != 3 {
				t.Errorf("expected %d driver annotations got %d", 3, len(test.app.Spec.Driver.Annotations))
			}
			if test.app.Spec.Driver.Annotations[prometheusPortAnnotation] != test.port {
				t.Errorf("java agent port expected %s got %s", test.port, test.app.Spec.Driver.Annotations[prometheusPortAnnotation])
			}

			if *test.app.Spec.Driver.JavaOptions != test.driverJavaOptions {
				t.Errorf("driver Java options expected %s got %s", test.driverJavaOptions, *test.app.Spec.Driver.JavaOptions)
			}
		}

		if test.app.Spec.Monitoring.ExposeExecutorMetrics {
			if len(test.app.Spec.Executor.Annotations) != 3 {
				t.Errorf("expected %d driver annotations got %d", 3, len(test.app.Spec.Executor.Annotations))
			}
			if test.app.Spec.Executor.Annotations[prometheusPortAnnotation] != test.port {
				t.Errorf("java agent port expected %s got %s", test.port, test.app.Spec.Executor.Annotations[prometheusPortAnnotation])
			}

			if *test.app.Spec.Executor.JavaOptions != test.executorJavaOptions {
				t.Errorf("driver Java options expected %s got %s", test.executorJavaOptions, *test.app.Spec.Executor.JavaOptions)
			}
		}

		if test.app.Spec.Monitoring.MetricsPropertiesFile != nil {
			if test.app.Spec.SparkConf["spark.metrics.conf"] != test.metricsPropertiesFile {
				t.Errorf("expected sparkConf %s got %s", test.metricsPropertiesFile, test.app.Spec.SparkConf["spark.metrics.conf"])
			}
		}
	}

	testcases := []testcase{
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "app1",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Monitoring: &v1beta2.MonitoringSpec{
						ExposeDriverMetrics:   true,
						ExposeExecutorMetrics: true,
						Prometheus: &v1beta2.PrometheusSpec{
							JmxExporterJar: "/prometheus/exporter.jar",
						},
					},
				},
			},
			metricsProperties:   config.DefaultMetricsProperties,
			prometheusConfig:    config.DefaultPrometheusConfiguration,
			port:                fmt.Sprintf("%d", config.DefaultPrometheusJavaAgentPort),
			driverJavaOptions:   "-javaagent:/prometheus/exporter.jar=8090:/etc/metrics/conf/prometheus.yaml",
			executorJavaOptions: "-javaagent:/prometheus/exporter.jar=8090:/etc/metrics/conf/prometheus.yaml",
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "app2",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Driver: v1beta2.DriverSpec{
						JavaOptions: stringptr("-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"),
					},
					Executor: v1beta2.ExecutorSpec{
						JavaOptions: stringptr("-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"),
					},
					Monitoring: &v1beta2.MonitoringSpec{
						ExposeDriverMetrics:   true,
						ExposeExecutorMetrics: true,
						MetricsProperties:     stringptr("testcase2dummy"),
						Prometheus: &v1beta2.PrometheusSpec{
							JmxExporterJar: "/prometheus/exporter.jar",
							Port:           int32ptr(8091),
							Configuration:  stringptr("testcase2dummy"),
						},
					},
				},
			},
			metricsProperties:   "testcase2dummy",
			prometheusConfig:    "testcase2dummy",
			port:                "8091",
			driverJavaOptions:   "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -javaagent:/prometheus/exporter.jar=8091:/etc/metrics/conf/prometheus.yaml",
			executorJavaOptions: "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -javaagent:/prometheus/exporter.jar=8091:/etc/metrics/conf/prometheus.yaml",
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "app2",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Driver: v1beta2.DriverSpec{
						JavaOptions: stringptr("-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"),
					},
					Executor: v1beta2.ExecutorSpec{
						JavaOptions: stringptr("-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"),
					},
					Monitoring: &v1beta2.MonitoringSpec{
						ExposeDriverMetrics:   true,
						ExposeExecutorMetrics: true,
						MetricsProperties:     stringptr("testcase3dummy"),
						Prometheus: &v1beta2.PrometheusSpec{
							JmxExporterJar: "/prometheus/exporter.jar",
							Port:           int32ptr(8091),
							ConfigFile:     stringptr("testcase3dummy.yaml"),
						},
					},
				},
			},
			metricsProperties:   "testcase3dummy",
			port:                "8091",
			driverJavaOptions:   "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -javaagent:/prometheus/exporter.jar=8091:testcase3dummy.yaml",
			executorJavaOptions: "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -javaagent:/prometheus/exporter.jar=8091:testcase3dummy.yaml",
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "app2",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Driver: v1beta2.DriverSpec{
						JavaOptions: stringptr("-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"),
					},
					Executor: v1beta2.ExecutorSpec{
						JavaOptions: stringptr("-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"),
					},
					Monitoring: &v1beta2.MonitoringSpec{
						ExposeDriverMetrics:   true,
						ExposeExecutorMetrics: true,
						MetricsPropertiesFile: stringptr("/testcase4dummy/metrics.properties"),
						Prometheus: &v1beta2.PrometheusSpec{
							JmxExporterJar: "/prometheus/exporter.jar",
							Port:           int32ptr(8091),
							ConfigFile:     stringptr("testcase4dummy.yaml"),
						},
					},
				},
			},
			metricsPropertiesFile: "/testcase4dummy/metrics.properties",
			port:                  "8091",
			driverJavaOptions:     "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -javaagent:/prometheus/exporter.jar=8091:testcase4dummy.yaml",
			executorJavaOptions:   "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -javaagent:/prometheus/exporter.jar=8091:testcase4dummy.yaml",
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "app2",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Driver: v1beta2.DriverSpec{
						JavaOptions: stringptr("-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"),
					},
					Executor: v1beta2.ExecutorSpec{
						JavaOptions: stringptr("-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"),
					},
					Monitoring: &v1beta2.MonitoringSpec{
						ExposeDriverMetrics:   true,
						ExposeExecutorMetrics: true,
						MetricsPropertiesFile: stringptr("/testcase5dummy/metrics.properties"),
						Prometheus: &v1beta2.PrometheusSpec{
							JmxExporterJar: "/prometheus/exporter.jar",
							Port:           int32ptr(8091),
						},
					},
				},
			},
			metricsPropertiesFile: "/testcase5dummy/metrics.properties",
			prometheusConfig:      config.DefaultPrometheusConfiguration,
			port:                  "8091",
			driverJavaOptions:     "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -javaagent:/prometheus/exporter.jar=8091:/etc/metrics/conf/prometheus.yaml",
			executorJavaOptions:   "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -javaagent:/prometheus/exporter.jar=8091:/etc/metrics/conf/prometheus.yaml",
		},
	}

	for _, test := range testcases {
		testFn(test, t)
	}
}
