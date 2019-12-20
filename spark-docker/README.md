# Custom Spark Images

This directory contains a Dockerfile for a variant of the Spark image with built-in support for accessing Google Cloud Storage (GCS) using the Hadoop FileSystem API and exposing Spark metrics (e.g., driver and executor metrics) to Prometheus in the Prometheus data model. This image can be used in Spark applications either managed by the Spark Operator or submitted through `spark-submit`, using the Kubernetes deployment mode.

## Exposing Spark Metrics to Prometheus

The `conf` directory contains [metrics.properties](conf/metrics.properties) that configures the Spark metric system and [prometheus.yaml](conf/prometheus.yaml) that is the configuration file to be used with the [Prometheus JMX exporter](https://github.com/prometheus/jmx_exporter).

By default configuration file `conf/prometheus.yaml` is put under `/prometheus` in the image, and configuration file `conf/metrics.properties` is put under `$SPARK_HOME/conf`. The Prometheus JMX exporter runs as a Java agent to scrape metrics exposed through JMX. To configure the Java agent for the exporter, add the following to the Spark configuration property `spark.driver.extraJavaOptions` for the driver (or `spark.executor.extraJavaOptions` for executors), e.g.,:

```
--conf spark.driver.extraJavaOptions="-javaagent:/prometheus/jmx_prometheus_javaagent-0.12.0.jar=8090:/prometheus/prometheus.yaml"
```

The JMX exporter exposes a HTTP server serving the metrics on the specified port (`8090` in the example above). To make Prometheus discover and scrape the metrics, please add the following Kubernetes annotations to the Spark driver or executors. Make sure the value of `prometheus.io/port` is the same as the port specified in the Java agent configuration.

```
"prometheus.io/scrape": "true"
"prometheus.io/port": "8090"
"prometheus.io/path": "/metrics"
```

To enable metric exporting to Prometheus for your `SparkApplication` resources, follow the instructions in [Monitoring](../docs/user-guide.md#monitoring). A complete example `SparkApplication` specification with metric exporting to Prometheus enabled can be found [here](../examples/spark-pi-prometheus.yaml).

## Accessing Google Cloud Storage (GCS)

The Dockerfile in this directory supports accessing GCS from your Spark applications as well as using jars and files located remotely in GCS. Specifically, the `conf` directory contains [spark-defaults.conf](conf/spark-defaults.conf) that defines default Spark configuration properties, including the ones for enabling the `gs://` file system scheme. To access jars or files stored in GCS with file URLs starting with `gs://`, add the following Spark configuration properties to your `spark-submit` command.

```
--conf spark.hadoop.fs.gs.project.id=<GCP project ID> \
--conf spark.hadoop.google.cloud.auth.service.account.enable=true \
--conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=<Path to GCS service account JSON key file> \
```

If you instead use the Spark Operator, add the configuration properties under `spec.hadoopConf` of your `SparkApplication` resources, e.g., 

```yaml
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: foo-gcs-bg
spec:
  type: Java
  mode: cluster
  image: gcr.io/spark-operator/spark:v2.4.5-SNAPSHOT-gcp-prometheus
  imagePullPolicy: Always
  hadoopConf:
    "fs.gs.project.id": "foo"
    "google.cloud.auth.service.account.enable": "true"
    "google.cloud.auth.service.account.json.keyfile": "/mnt/secrets/key.json"
  driver:
    cores: 1
    secrets:
    - name: "gcs-bq"
      path: "/mnt/secrets"
      secretType: GCPServiceAccount
    serviceAccount: spark
  executor:
    instances: 2
    cores: 1
    memory: "512m"
    secrets:
    - name: "gcs-bq"
      path: "/mnt/secrets"
      secretType: GCPServiceAccount
```

Note the three configuration properties under `spec.hadoopConf`. Specifically, `fs.gs.project.id` specifies the ID of the GCP project the GCS bucket is associated with, `google.cloud.auth.service.account.enable` enables authentication with GCS using a service account JSON key file, and `google.cloud.auth.service.account.json.keyfile` specifies the path to the JSON key file, which gets mounted into the driver and executor containers using a Kubernetes Secret named `gcs-bq`. Note that the `secretType` is set to `GCPServiceAccount` to indicate that the Secret stores a GCP service account key file.
