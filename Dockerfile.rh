# syntax=docker/dockerfile:1.0-experimental
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

# Build an OpenShift image.
# Before running docker build, make sure
# 1. Your Docker version is >= 18.09.3
# 2. export DOCKER_BUILDKIT=1

ARG SPARK_IMAGE=gcr.io/spark-operator/spark:v2.4.0

FROM golang:1.12.5-alpine as builder
ARG DEP_VERSION="0.5.3"
RUN apk add --no-cache bash git 
ADD https://github.com/golang/dep/releases/download/v${DEP_VERSION}/dep-linux-amd64 /usr/bin/dep
RUN chmod +x /usr/bin/dep

WORKDIR ${GOPATH}/src/github.com/GoogleCloudPlatform/spark-on-k8s-operator
COPY Gopkg.toml Gopkg.lock ./
RUN dep ensure -vendor-only
COPY . ./
RUN go generate && CGO_ENABLED=0 GOOS=linux go build -o /usr/bin/spark-operator

FROM ${SPARK_IMAGE}
COPY --from=builder /usr/bin/spark-operator /usr/bin/
USER root

# Comment out the following three lines if you do not have a RedHat subscription.
COPY hack/install_packages.sh /
RUN --mount=target=/opt/spark/credentials,type=secret,id=credentials,required /install_packages.sh
RUN rm /install_packages.sh

RUN chmod -R u+x /tmp

COPY hack/gencerts.sh /usr/bin/
COPY entrypoint.sh /usr/bin/
USER 185
ENTRYPOINT ["/usr/bin/entrypoint.sh"]
