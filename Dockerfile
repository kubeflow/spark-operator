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
RUN apk add --no-cache openssl curl tini
COPY hack/gencerts.sh /usr/bin/

COPY entrypoint.sh /usr/bin/
ENTRYPOINT ["/usr/bin/entrypoint.sh"]
