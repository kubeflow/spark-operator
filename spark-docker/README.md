This directory contains a Dockerfile for a variant of the Spark image with built-in support for accessing Google Cloud Storage using the Hadoop FileSystem API and exposing Spark metrics (e.g., driver and executor metrics) to Prometheus in the Prometheus data model. The `conf` directory contains `metrics.properties` that configures the Spark metric system and `prometheus.yaml` that is the configuration file to be used with the [Prometheus JMX exporter](https://github.com/prometheus/jmx_exporter).

By default both configuration files under `conf` are put under `/prometheus` in the image. To make Spark be aware of `metrics.properties`, please set the Spark configuration property `spark.metrics.conf=/prometheus/metrics.properties`. The Prometheus JMX exporter runs as a Java agent to scrape metrics exposed through JMX. To configure the Java agent for the exporter, add the following to the Spark configuration property `spark.driver.extraJavaOptions` for the driver (or `spark.executor.extraJavaOptions` for executors):

```
-javaagent:/prometheus/jmx_prometheus_javaagent-0.11.0.jar=8090:/prometheus/prometheus.yaml
``` 

The JMX exporter exposes a HTTP server serving the metrics on the specified port (`8090` in the example above). To make Prometheus discover and scrape the metrics, please add the following annotations to the Spark driver or executors. Make sure the value of `prometheus.io/port` is the same as the port specified in the Java agent configuration.

```
"prometheus.io/scrape": "true"
"prometheus.io/port": "8090"
"prometheus.io/path": "/metrics"
```

A complete example `SparkApplication` specification with metric exporting to Prometheus enabled can be found [here](../examples/spark-pi-prometheus.yaml).   