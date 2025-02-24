/*
Copyright 2024 The Kubeflow authors.

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

package common

const (
	// PrometheusConfigMapNameSuffix is the name prefix of the Prometheus ConfigMap.
	PrometheusConfigMapNameSuffix = "prom-conf"

	// PrometheusConfigMapMountPath is the mount path of the Prometheus ConfigMap.
	PrometheusConfigMapMountPath = "/etc/metrics/conf"
)

const (
	MetricsPropertiesKey       = "metrics.properties"
	PrometheusConfigKey        = "prometheus.yaml"
	PrometheusScrapeAnnotation = "prometheus.io/scrape"
	PrometheusPortAnnotation   = "prometheus.io/port"
	PrometheusPathAnnotation   = "prometheus.io/path"
)

// DefaultMetricsProperties is the default content of metrics.properties.
const DefaultMetricsProperties = `
*.sink.jmx.class=org.apache.spark.metrics.sink.JmxSink
driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource
executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource`

// DefaultPrometheusConfiguration is the default content of prometheus.yaml.
const DefaultPrometheusConfiguration = `
lowercaseOutputName: true
attrNameSnakeCase: true
rules:
  - pattern: metrics<name=(\S+)\.(\S+)\.driver\.(BlockManager|DAGScheduler|jvm)\.(\S+)><>Value
    name: spark_driver_$3_$4
    type: GAUGE
    labels:
      app_namespace: "$1"
      app_id: "$2"
  - pattern: metrics<name=(\S+)\.(\S+)\.driver\.(\S+)\.StreamingMetrics\.streaming\.(\S+)><>Value
    name: spark_streaming_driver_$4
    type: GAUGE
    labels:
      app_namespace: "$1"
      app_id: "$2"
  - pattern: metrics<name=(\S+)\.(\S+)\.driver\.spark\.streaming\.(\S+)\.(\S+)><>Value
    name: spark_structured_streaming_driver_$4
    type: GAUGE
    labels:
      app_namespace: "$1"
      app_id: "$2"
      query_name: "$3"
  - pattern: metrics<name=(\S+)\.(\S+)\.(\S+)\.executor\.(\S+)><>Value
    name: spark_executor_$4
    type: GAUGE
    labels:
      app_namespace: "$1"
      app_id: "$2"
      executor_id: "$3"
  - pattern: metrics<name=(\S+)\.(\S+)\.driver\.DAGScheduler\.(.*)><>Count
    name: spark_driver_DAGScheduler_$3_count
    type: COUNTER
    labels:
      app_namespace: "$1"
      app_id: "$2"
  - pattern: metrics<name=(\S+)\.(\S+)\.driver\.HiveExternalCatalog\.(.*)><>Count
    name: spark_driver_HiveExternalCatalog_$3_count
    type: COUNTER
    labels:
      app_namespace: "$1"
      app_id: "$2"
  - pattern: metrics<name=(\S+)\.(\S+)\.driver\.CodeGenerator\.(.*)><>Count
    name: spark_driver_CodeGenerator_$3_count
    type: COUNTER
    labels:
      app_namespace: "$1"
      app_id: "$2"
  - pattern: metrics<name=(\S+)\.(\S+)\.driver\.LiveListenerBus\.(.*)><>Count
    name: spark_driver_LiveListenerBus_$3_count
    type: COUNTER
    labels:
      app_namespace: "$1"
      app_id: "$2"
  - pattern: metrics<name=(\S+)\.(\S+)\.driver\.LiveListenerBus\.(.*)><>Value
    name: spark_driver_LiveListenerBus_$3
    type: GAUGE
    labels:
      app_namespace: "$1"
      app_id: "$2"
  - pattern: metrics<name=(\S+)\.(\S+)\.(.*)\.executor\.(.*)><>Count
    name: spark_executor_$4_count
    type: COUNTER
    labels:
      app_namespace: "$1"
      app_id: "$2"
      executor_id: "$3"
  - pattern: metrics<name=(\S+)\.(\S+)\.([0-9]+)\.(jvm|NettyBlockTransfer)\.(.*)><>Value
    name: spark_executor_$4_$5
    type: GAUGE
    labels:
      app_namespace: "$1"
      app_id: "$2"
      executor_id: "$3"
  - pattern: metrics<name=(\S+)\.(\S+)\.([0-9]+)\.HiveExternalCatalog\.(.*)><>Count
    name: spark_executor_HiveExternalCatalog_$4_count
    type: COUNTER
    labels:
      app_namespace: "$1"
      app_id: "$2"
      executor_id: "$3"
  - pattern: metrics<name=(\S+)\.(\S+)\.([0-9]+)\.CodeGenerator\.(.*)><>Count
    name: spark_executor_CodeGenerator_$4_count
    type: COUNTER
    labels:
      app_namespace: "$1"
      app_id: "$2"
      executor_id: "$3"
`

// DefaultPrometheusJavaAgentPort is the default port used by the Prometheus JMX exporter.
const DefaultPrometheusJavaAgentPort int32 = 8090

// DefaultPrometheusPortProtocol is the default protocol used by the Prometheus JMX exporter.
const DefaultPrometheusPortProtocol string = "TCP"

// DefaultPrometheusPortName is the default port name used by the Prometheus JMX exporter.
const DefaultPrometheusPortName string = "jmx-exporter"
