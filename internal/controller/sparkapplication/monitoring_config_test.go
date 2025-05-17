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

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
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

	fakeClient := fake.NewFakeClient()

	testFn := func(test testcase, t *testing.T) {
		err := configPrometheusMonitoring(test.app, fakeClient)
		assert.NoError(t, err, "failed to configure Prometheus monitoring")

		configMapName := util.GetPrometheusConfigMapName(test.app)
		configMap := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: configMapName, Namespace: test.app.Namespace},
		}
		err = fakeClient.Get(context.TODO(), client.ObjectKeyFromObject(configMap), configMap)
		assert.NoError(t, err, "failed to get ConfigMap %s", configMapName)

		if test.app.Spec.Monitoring.Prometheus.ConfigFile == nil &&
			test.app.Spec.Monitoring.MetricsPropertiesFile == nil {
			assert.Len(t, configMap.Data, 2, "expected 2 data items")
		}

		if test.app.Spec.Monitoring.Prometheus.ConfigFile != nil &&
			test.app.Spec.Monitoring.MetricsPropertiesFile == nil {
			assert.Len(t, configMap.Data, 1, "expected 1 data item")
		}

		if test.app.Spec.Monitoring.Prometheus.ConfigFile == nil &&
			test.app.Spec.Monitoring.MetricsPropertiesFile != nil {
			assert.Len(t, configMap.Data, 1, "expected 1 data item")
		}

		if test.app.Spec.Monitoring.MetricsPropertiesFile == nil {
			assert.Equal(t, test.metricsProperties, configMap.Data[common.MetricsPropertiesKey], "metrics.properties mismatch")
		}

		if test.app.Spec.Monitoring.Prometheus.ConfigFile == nil {
			assert.Equal(t, test.prometheusConfig, configMap.Data[common.PrometheusConfigKey], "prometheus.yaml mismatch")
		}

		if test.app.Spec.Monitoring.ExposeDriverMetrics {
			assert.Len(t, test.app.Spec.Driver.Annotations, 3, "expected 3 driver annotations")
			assert.Equal(t, test.port, test.app.Spec.Driver.Annotations[common.PrometheusPortAnnotation], "java agent port mismatch")
			assert.Equal(t, test.driverJavaOptions, *test.app.Spec.Driver.JavaOptions, "driver Java options mismatch")

		}

		if test.app.Spec.Monitoring.ExposeExecutorMetrics {
			assert.Len(t, test.app.Spec.Executor.Annotations, 3, "expected 3 executor annotations")
			assert.Equal(t, test.port, test.app.Spec.Executor.Annotations[common.PrometheusPortAnnotation], "java agent port mismatch")
			assert.Equal(t, test.executorJavaOptions, *test.app.Spec.Executor.JavaOptions, "executor Java options mismatch")
		}

		if test.app.Spec.Monitoring.MetricsPropertiesFile != nil {
			assert.Equal(t, test.metricsPropertiesFile, test.app.Spec.SparkConf["spark.metrics.conf"], "sparkConf mismatch")
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
			metricsProperties:   common.DefaultMetricsProperties,
			prometheusConfig:    common.DefaultPrometheusConfiguration,
			port:                fmt.Sprintf("%d", common.DefaultPrometheusJavaAgentPort),
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
						JavaOptions: util.StringPtr("-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"),
					},
					Executor: v1beta2.ExecutorSpec{
						JavaOptions: util.StringPtr("-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"),
					},
					Monitoring: &v1beta2.MonitoringSpec{
						ExposeDriverMetrics:   true,
						ExposeExecutorMetrics: true,
						MetricsProperties:     util.StringPtr("testcase2dummy"),
						Prometheus: &v1beta2.PrometheusSpec{
							JmxExporterJar: "/prometheus/exporter.jar",
							Port:           util.Int32Ptr(8091),
							Configuration:  util.StringPtr("testcase2dummy"),
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
						JavaOptions: util.StringPtr("-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"),
					},
					Executor: v1beta2.ExecutorSpec{
						JavaOptions: util.StringPtr("-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"),
					},
					Monitoring: &v1beta2.MonitoringSpec{
						ExposeDriverMetrics:   true,
						ExposeExecutorMetrics: true,
						MetricsProperties:     util.StringPtr("testcase3dummy"),
						Prometheus: &v1beta2.PrometheusSpec{
							JmxExporterJar: "/prometheus/exporter.jar",
							Port:           util.Int32Ptr(8091),
							ConfigFile:     util.StringPtr("testcase3dummy.yaml"),
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
						JavaOptions: util.StringPtr("-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"),
					},
					Executor: v1beta2.ExecutorSpec{
						JavaOptions: util.StringPtr("-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"),
					},
					Monitoring: &v1beta2.MonitoringSpec{
						ExposeDriverMetrics:   true,
						ExposeExecutorMetrics: true,
						MetricsPropertiesFile: util.StringPtr("/testcase4dummy/metrics.properties"),
						Prometheus: &v1beta2.PrometheusSpec{
							JmxExporterJar: "/prometheus/exporter.jar",
							Port:           util.Int32Ptr(8091),
							ConfigFile:     util.StringPtr("testcase4dummy.yaml"),
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
						JavaOptions: util.StringPtr("-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"),
					},
					Executor: v1beta2.ExecutorSpec{
						JavaOptions: util.StringPtr("-XX:+PrintGCDetails -XX:+PrintGCTimeStamps"),
					},
					Monitoring: &v1beta2.MonitoringSpec{
						ExposeDriverMetrics:   true,
						ExposeExecutorMetrics: true,
						MetricsPropertiesFile: util.StringPtr("/testcase5dummy/metrics.properties"),
						Prometheus: &v1beta2.PrometheusSpec{
							JmxExporterJar: "/prometheus/exporter.jar",
							Port:           util.Int32Ptr(8091),
						},
					},
				},
			},
			metricsPropertiesFile: "/testcase5dummy/metrics.properties",
			prometheusConfig:      common.DefaultPrometheusConfiguration,
			port:                  "8091",
			driverJavaOptions:     "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -javaagent:/prometheus/exporter.jar=8091:/etc/metrics/conf/prometheus.yaml",
			executorJavaOptions:   "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -javaagent:/prometheus/exporter.jar=8091:/etc/metrics/conf/prometheus.yaml",
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "driver-only",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Monitoring: &v1beta2.MonitoringSpec{
						ExposeDriverMetrics:   true,
						ExposeExecutorMetrics: false,
						Prometheus: &v1beta2.PrometheusSpec{
							JmxExporterJar: "/prometheus/exporter.jar",
						},
					},
					Driver: v1beta2.DriverSpec{JavaOptions: util.StringPtr("testdummy")},
				},
			},
			metricsProperties: common.DefaultMetricsProperties,
			prometheusConfig:  common.DefaultPrometheusConfiguration,
			port:              fmt.Sprintf("%d", common.DefaultPrometheusJavaAgentPort),
			driverJavaOptions: "testdummy -javaagent:/prometheus/exporter.jar=8090:/etc/metrics/conf/prometheus.yaml",
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "executor-only",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Monitoring: &v1beta2.MonitoringSpec{
						ExposeDriverMetrics:   false,
						ExposeExecutorMetrics: true,
						Prometheus: &v1beta2.PrometheusSpec{
							JmxExporterJar: "/prometheus/exporter.jar",
						},
					},
					Executor: v1beta2.ExecutorSpec{JavaOptions: util.StringPtr("testdummy")},
				},
			},
			metricsProperties:   common.DefaultMetricsProperties,
			prometheusConfig:    common.DefaultPrometheusConfiguration,
			port:                fmt.Sprintf("%d", common.DefaultPrometheusJavaAgentPort),
			executorJavaOptions: "testdummy -javaagent:/prometheus/exporter.jar=8090:/etc/metrics/conf/prometheus.yaml",
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "custom-port-name",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					Monitoring: &v1beta2.MonitoringSpec{
						ExposeDriverMetrics:   true,
						ExposeExecutorMetrics: true,
						Prometheus: &v1beta2.PrometheusSpec{
							JmxExporterJar: "/prometheus/exporter.jar",
							Port:           util.Int32Ptr(1000),
							PortName:       util.StringPtr("metrics-port"),
						},
					},
				},
			},
			metricsProperties:   common.DefaultMetricsProperties,
			prometheusConfig:    common.DefaultPrometheusConfiguration,
			port:                "1000",
			driverJavaOptions:   "-javaagent:/prometheus/exporter.jar=1000:/etc/metrics/conf/prometheus.yaml",
			executorJavaOptions: "-javaagent:/prometheus/exporter.jar=1000:/etc/metrics/conf/prometheus.yaml",
		},
	}

	for _, test := range testcases {
		testFn(test, t)
	}
}
