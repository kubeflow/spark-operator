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

ARG SPARK_IMAGE=docker.io/apache/spark:4.0.4@sha256:94ad730f7510002d8a1615de269f27cdeca4d4eef51657384db3fa9246b5a4d8

FROM docker.io/library/golang:1.27.0@sha256:0ecdc2a9f6156af6451080bfe3d8382a662fcc4e209608c6f919e643453514c1 AS builder

WORKDIR /workspace

RUN --mount=type=cache,target=/go/pkg/mod/ \
    --mount=type=bind,source=go.mod,target=go.mod \
    --mount=type=bind,source=go.sum,target=go.sum \
    go mod download

COPY . .

ENV GOCACHE=/root/.cache/go-build

ARG TARGETARCH

# Build metadata. When unset, the Makefile derives these from the git tree
# inside the build context, preserving the behaviour of a plain `docker build`.
ARG VERSION=
ARG GIT_COMMIT=
ARG GIT_TREE_STATE=
ARG SOURCE_DATE_EPOCH=

RUN --mount=type=cache,target=/go/pkg/mod/ \
    --mount=type=cache,target="/root/.cache/go-build" \
    CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} GO111MODULE=on \
    make build-operator \
      ${VERSION:+VERSION=$VERSION} \
      ${GIT_COMMIT:+GIT_COMMIT=$GIT_COMMIT} \
      ${GIT_TREE_STATE:+GIT_TREE_STATE=$GIT_TREE_STATE} \
      ${SOURCE_DATE_EPOCH:+SOURCE_DATE_EPOCH=$SOURCE_DATE_EPOCH}

# Export-only stage. `docker build` targets the final stage by default, so this
# is never built unless requested via --target=artifacts. CI uses it to extract
# the exact binary that ships in the image.
FROM scratch AS artifacts
COPY --from=builder /workspace/bin/spark-operator /spark-operator

FROM ${SPARK_IMAGE}

ARG SPARK_UID=185

ARG SPARK_GID=185

USER root

RUN apt-get update \
    && apt-get install -y catatonit \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /etc/k8s-webhook-server/serving-certs /home/spark && \
    chmod -R g+rw /etc/k8s-webhook-server/serving-certs && \
    chown -R spark /etc/k8s-webhook-server/serving-certs /home/spark

USER ${SPARK_UID}:${SPARK_GID}

COPY --from=builder /workspace/bin/spark-operator /usr/bin/spark-operator

COPY entrypoint.sh /usr/bin/

ENTRYPOINT ["/usr/bin/entrypoint.sh"]
