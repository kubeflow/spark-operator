# Monitoring Spark Applications with Prometheus and JMX Exporter

Spark Operator supports exporting Spark metrics in Prometheus format using the [JMX Prometheus Exporter](https://github.com/prometheus/jmx_exporter). This allows detailed monitoring of your Spark drivers and executors with tools like Prometheus and Grafana.

:::{admonition} Warning
:class: warning

The older documentation in [Kubeflow's monitoring section](https://kubeflow.github.io/spark-operator/docs/user-guide.html#monitoring) is outdated and fails with newer Spark images. This updated guide addresses [Issue #2380](https://github.com/kubeflow/spark-operator/issues/2380).
:::

## 1. Build a Spark Image with the JMX Exporter Jar

Start by building a custom Docker image that includes the JMX Prometheus Java agent.

**Dockerfile example:**

```Dockerfile
ARG SPARK_IMAGE=docker.io/spark:3.4.1
FROM ${SPARK_IMAGE}

# Switch to user root so we can add additional jars and configuration files.
USER root

# Setup for the Prometheus JMX exporter.
ENV JMX_EXPORTER_AGENT_VERSION=1.1.0
ADD https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_AGENT_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_AGENT_VERSION}.jar /opt/spark/jars
RUN chmod 644 /opt/spark/jars/jmx_prometheus_javaagent-${JMX_EXPORTER_AGENT_VERSION}.jar

USER ${spark_uid}
```

Build and push the image:

```bash
docker build -t <your-repo>/spark-jmx:3.4.1 .
docker push <your-repo>/spark-jmx:3.4.1
```

---

## 2. Configure the SparkApplication with Monitoring Enabled

Use the `monitoring.prometheus` section to enable the JMX exporter in both the driver and executor containers.

**SparkApplication YAML example:**

```yaml
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: spark-pi
spec:
  type: Java
  mode: cluster
  image: "<your-repo>/spark-jmx:3.4.1"
  imagePullPolicy: Always
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: "local:///opt/spark/examples/jars/spark-examples_2.12-3.4.1.jar"
  sparkVersion: "3.4.1"
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "512m"
    labels:
      version: 3.4.1
    serviceAccount: spark-operator-spark
  executor:
    cores: 1
    instances: 1
    memory: "512m"
    labels:
      version: 3.4.1
  monitoring:
    exposeDriverMetrics: true
    exposeExecutorMetrics: true
    prometheus:
      jmxExporterJar: "/opt/spark/jars/jmx_prometheus_javaagent-1.1.0.jar"
      port: 8090
```

---

## 3. Access and Monitor Metrics

### Prometheus Scraping

Set up a `PodMonitor` or `ServiceMonitor` to scrape the metrics:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: spark-pi
spec:
  selector:
    matchExpressions:
      - key: "spark-role"
        operator: "Exists"
  podMetricsEndpoints:
    - port: jmx-exporter
```

## 4. Quick Access to Grafana and Prometheus

Before accessing dashboards, make sure you have a Prometheus stack installed and configured.

> ℹ️ **Note**: You need to install a Prometheus stack to collect and visualize metrics.
>
> A good option is the [kube-prometheus-stack Helm chart](https://github.com/prometheus-community/helm-charts/tree/main/charts/kube-prometheus-stack).
>
> In your `values.yaml`, be sure to set the following to ensure Prometheus scrapes `PodMonitor` and `ServiceMonitor` objects cluster-wide:
>
> ```yaml
> serviceMonitorSelectorNilUsesHelmValues: false
> podMonitorSelectorNilUsesHelmValues: false
> ```

### Access Grafana:

```bash
kubectl port-forward $(kubectl get pods --selector=app.kubernetes.io/name=grafana -n monitoring --output=jsonpath="{.items..metadata.name}") -n monitoring 3001:3000

# Visit:
http://localhost:3001
# Login: admin / prom-operator

# Then import dashboard inside Grafana https://grafana.com/grafana/dashboards/23304
# Dashboard ID is: 23304
```

### Access Prometheus:

```bash
kubectl port-forward -n monitoring prometheus-prometheus-stack-kube-prom-prometheus-0 9090

# Visit:
http://localhost:9090
```

## 5. Troubleshooting

```bash
# Check prometheus exporter behavior
kubectl exec -it -n spark <driver-pod-name> -- curl http://localhost:8090/metrics

# Access prometheus exporter configuration
kubectl exec -it -n spark <driver-pod-name> -- cat /etc/metrics/conf/prometheus.yaml

# Check metrics are available inside Prometheus database
kubectl run -i --rm --tty shell --image=curlimages/curl -- sh
METRIC_NAME="spark_driver_livelistenerbus_queue_streams_size_type_gauges"
curl "http://prometheus-stack-kube-prom-prometheus.monitoring:9090/api/v1/query?query=$METRIC_NAME"
```

## Summary

| Step | Description |
|------|-------------|
| 1️⃣  | Build a custom Spark image with `jmx_prometheus_javaagent` |
| 2️⃣  | Use the `monitoring.prometheus` section in your SparkApplication |
| 3️⃣  | Use `PodMonitor` or `ServiceMonitor` to scrape metrics |
| 4️⃣  | Access Grafana and Prometheus via port-forward or SSH |
| 5️⃣  | Troubleshoot with `curl` and `kubectl exec` |
