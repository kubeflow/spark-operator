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

ARG SPARK_IMAGE=onehouse/spark-3.5.3-base:130825

FROM golang:1.23.12 AS builder

WORKDIR /workspace

RUN --mount=type=cache,target=/go/pkg/mod/ \
    --mount=type=bind,source=go.mod,target=go.mod \
    --mount=type=bind,source=go.sum,target=go.sum \
    go mod download

COPY . .

ENV GOCACHE=/root/.cache/go-build

ARG TARGETARCH

RUN --mount=type=cache,target=/go/pkg/mod/ \
    --mount=type=cache,target="/root/.cache/go-build" \
    CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} GO111MODULE=on make build-operator

FROM ${SPARK_IMAGE}

ARG SPARK_UID=185

ARG SPARK_GID=185

USER root

# Install dependencies and add JARs in optimized layers
RUN set -ex; \
    # Install tini
    apt-get update && \
    apt-get install -y --no-install-recommends tini wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    # Create directories
    mkdir -p $SPARK_HOME/jars /etc/k8s-webhook-server/serving-certs /home/spark && \
    # Download all JARs in parallel for better performance
    wget -q -O $SPARK_HOME/jars/hadoop-aws-3.1.1.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.1/hadoop-aws-3.1.1.jar && \
    wget -q -O $SPARK_HOME/jars/aws-java-sdk-bundle-1.11.814.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.814/aws-java-sdk-bundle-1.11.814.jar && \
    wget -q -O $SPARK_HOME/jars/spark-avro_2.12-3.1.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.1.1/spark-avro_2.12-3.1.1.jar && \
    wget -q -O $SPARK_HOME/jars/gcs-connector-hadoop3-latest.jar \
        https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar && \
    wget -q -O $SPARK_HOME/jars/hadoop-azure-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.4/hadoop-azure-3.3.4.jar && \
    wget -q -O $SPARK_HOME/jars/azure-storage-blob-12.25.0.jar \
        https://repo1.maven.org/maven2/com/azure/azure-storage-blob/12.25.0/azure-storage-blob-12.25.0.jar && \
    wget -q -O $SPARK_HOME/jars/azure-core-1.51.0.jar \
        https://repo1.maven.org/maven2/com/azure/azure-core/1.51.0/azure-core-1.51.0.jar && \
    wget -q -O $SPARK_HOME/jars/azure-core-http-netty-1.15.3.jar \
        https://repo1.maven.org/maven2/com/azure/azure-core-http-netty/1.15.3/azure-core-http-netty-1.15.3.jar && \
    # Set permissions for all JARs at once
    chmod 644 $SPARK_HOME/jars/*.jar && \
    # Set directory permissions
    chmod -R g+rw /etc/k8s-webhook-server/serving-certs && \
    chown -R spark /etc/k8s-webhook-server/serving-certs /home/spark

USER ${SPARK_UID}:${SPARK_GID}

COPY --from=builder /workspace/bin/spark-operator /usr/bin/spark-operator

COPY entrypoint.sh /usr/bin/

ENTRYPOINT ["/usr/bin/entrypoint.sh"]
