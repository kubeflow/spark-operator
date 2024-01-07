#
# Copyright 2017 Google LLC
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
# syntax=docker/dockerfile:1.2
FROM golang:1.19.2-alpine as builder

WORKDIR /workspace

# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum

# Cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

# Copy the go source code
COPY main.go main.go
COPY pkg/ pkg/

# Build
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -a -o /usr/bin/spark-operator main.go

FROM apache/spark:3.5.0-scala2.12-java11-python3-ubuntu

# https://github.com/apache/spark/blob/v3.5.0/pom.xml#L125
ARG HADOOP_AWS_VERSION=3.3.4
# https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/3.3.4
ARG AWS_JAVA_SDK_BUNDLE_VERSION=1.12.262
ARG JMX_PROMETHEUS_JAVAAGENT_VERSION=0.20.0
ARG POSTGRESQL_VERSION=9.4.1212

ENV SPARK_HOME /opt/spark

USER root

COPY --from=builder /usr/bin/spark-operator /usr/bin/

RUN apt-get update --allow-releaseinfo-change \
    && apt-get update \
    && apt-get install -y --no-install-recommends openssl curl tini \
    && rm -rf /var/lib/apt/lists/* \
    # download hadoop-aws, aws-java-sdk-bundle, prometheus and postgresql jars
    && curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar -o ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    && curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_JAVA_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_JAVA_SDK_BUNDLE_VERSION}.jar -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_JAVA_SDK_BUNDLE_VERSION}.jar \
    && curl https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_PROMETHEUS_JAVAAGENT_VERSION}/jmx_prometheus_javaagent-${JMX_PROMETHEUS_JAVAAGENT_VERSION}.jar -o ${SPARK_HOME}/jars/jmx_prometheus_javaagent-${JMX_PROMETHEUS_JAVAAGENT_VERSION}.jar \
    && curl https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRESQL_VERSION}/postgresql-${POSTGRESQL_VERSION}.jar -o ${SPARK_HOME}/jars/postgresql-${POSTGRESQL_VERSION}.jar

COPY hack/gencerts.sh /usr/bin/
COPY entrypoint.sh /usr/bin/

ENTRYPOINT ["/usr/bin/entrypoint.sh"]
