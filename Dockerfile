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

ARG SPARK_IMAGE=spark:3.5.3

FROM golang:1.23.1 AS builder

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

# Add AWS Jars
RUN mkdir -p $SPARK_HOME/jars
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.1/hadoop-aws-3.1.1.jar $SPARK_HOME/jars
RUN chmod 644 $SPARK_HOME/jars/hadoop-aws-3.1.1.jar

ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.814/aws-java-sdk-bundle-1.11.814.jar $SPARK_HOME/jars
RUN chmod 644 $SPARK_HOME/jars/aws-java-sdk-bundle-1.11.814.jar

ADD https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.1.1/spark-avro_2.12-3.1.1.jar $SPARK_HOME/jars
RUN chmod 644 $SPARK_HOME/jars/spark-avro_2.12-3.1.1.jar

# Add gcs connector
ADD https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar  $SPARK_HOME/jars
RUN chmod 644 $SPARK_HOME/jars/gcs-connector-hadoop3-latest.jar

# Build Operator and run
RUN apt-get update \
    && apt-get install -y tini \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /etc/k8s-webhook-server/serving-certs /home/spark && \
    chmod -R g+rw /etc/k8s-webhook-server/serving-certs && \
    chown -R spark /etc/k8s-webhook-server/serving-certs /home/spark

USER ${SPARK_UID}:${SPARK_GID}

COPY --from=builder /workspace/bin/spark-operator /usr/bin/spark-operator

COPY entrypoint.sh /usr/bin/

ENTRYPOINT ["/usr/bin/entrypoint.sh"]
