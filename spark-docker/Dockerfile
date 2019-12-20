#
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

<<<<<<< HEAD
ARG SPARK_IMAGE=gcr.io/spark-operator/spark:v2.4.5
=======
ARG SPARK_IMAGE=gcr.io/spark-operator/spark:v2.4.5-SNAPSHOT
>>>>>>> Updated GCP specific image and user guide
FROM ${SPARK_IMAGE}

RUN mkdir -p $SPARK_HOME/conf && \
    mkdir -p /prometheus

# Spark configuration file storing default Spark configuration properties.
COPY conf/spark-defaults.conf $SPARK_HOME/conf

# Setup for Google Cloud Storage access.
RUN rm $SPARK_HOME/jars/guava-14.0.1.jar
ADD http://central.maven.org/maven2/com/google/guava/guava/23.0/guava-23.0.jar $SPARK_HOME/jars
# Add the connector jar needed to access Google Cloud Storage using the Hadoop FileSystem API.
ADD https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar $SPARK_HOME/jars
# Add the connector jar needed to access Cloud BigQuery using the Hadoop FileSystem API.
ADD https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-latest.jar $SPARK_HOME/jars

# Setup for metric exporting to Prometheus.
# Add the Prometheus JMX exporter Java agent jar for exposing metrics sent to the JmxSink to Prometheus.
ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.12.0/jmx_prometheus_javaagent-0.12.0.jar /prometheus
# Spark metrics configuration file goes into the default location so it gets automatically loaded.
COPY conf/metrics.properties $SPARK_HOME/conf
# Configuration file for the Prometheus JMX exporter.
COPY conf/prometheus.yaml /prometheus
