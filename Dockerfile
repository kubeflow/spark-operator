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

ARG SPARK_IMAGE=gcr.io/spark-operator/spark:v3.1.1

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

FROM ${SPARK_IMAGE}
USER root
COPY --from=builder /usr/bin/spark-operator /usr/bin/
RUN apt-get update --allow-releaseinfo-change \
    && apt-get update \
    && apt-get install -y openssl curl tini \
    && rm -rf /var/lib/apt/lists/*
COPY hack/gencerts.sh /usr/bin/

COPY entrypoint.sh /usr/bin/
ENTRYPOINT ["/usr/bin/entrypoint.sh"]
