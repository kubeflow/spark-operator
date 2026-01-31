#!/usr/bin/env bash

# Copyright 2026 The Kubeflow Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail

CURRENT_DIR=$(dirname "${BASH_SOURCE[0]}")
SPARK_OPERATOR_ROOT=$(realpath "${CURRENT_DIR}/../..")
SPARK_OPERATOR_PKG="github.com/kubeflow/spark-operator/v2"

cd "$CURRENT_DIR/../.."


# Get the kube-openapi binary to generate OpenAPI spec.
OPENAPI_PKG=$(go list -m -mod=readonly -f "{{.Dir}}" k8s.io/kube-openapi)
echo ">> Using ${OPENAPI_PKG}"

echo "Generating OpenAPI specification for Kubeflow Spark Operator"

# This list needs to cover all of the types used transitively from the Kubeflow Spark Operator APIs.
# Update this list if Kubeflow Spark Operator depends on new external APIs.
EXTRA_PACKAGES=(
  k8s.io/apimachinery/pkg/apis/meta/v1
  k8s.io/apimachinery/pkg/api/resource
  k8s.io/apimachinery/pkg/runtime
  k8s.io/apimachinery/pkg/util/intstr
  k8s.io/api/core/v1
  k8s.io/api/networking/v1
)

# Generate OpenAPI Go code for each API version
for VERSION in v1alpha1 v1beta2; do
  echo "Generating OpenAPI for ${VERSION}"

  go run ${OPENAPI_PKG}/cmd/openapi-gen \
    --go-header-file "${SPARK_OPERATOR_ROOT}/hack/boilerplate.go.txt" \
    --output-pkg "${SPARK_OPERATOR_PKG}/api/${VERSION}" \
    --output-dir "${SPARK_OPERATOR_ROOT}/api/${VERSION}" \
    --output-file "zz_generated.openapi.go" \
    --report-filename "${SPARK_OPERATOR_ROOT}/hack/openapi/violation_exceptions.list" \
    "${EXTRA_PACKAGES[@]}" \
    "${SPARK_OPERATOR_ROOT}/api/${VERSION}"
done

# Generating OpenAPI Swagger for Kubeflow Spark Operator.
echo "Generate OpenAPI Swagger for Kubeflow Spark Operator"
go run hack/swagger/main.go >api/openapi-spec/swagger.json
