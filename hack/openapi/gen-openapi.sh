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
# openapi-gen does not automatically resolve transitive dependencies, so all external API
# packages referenced directly or indirectly by the CRD schemas must be listed here.
#
# For example, Kubernetes networking types (k8s.io/api/networking/v1) reference
# apimachinery types such as intstr.IntOrString (e.g. NetworkPolicyPort), which must
# be explicitly included to avoid unresolved OpenAPI schemas.
EXTRA_PACKAGES=(
  k8s.io/apimachinery/pkg/apis/meta/v1
  k8s.io/apimachinery/pkg/api/resource
  k8s.io/apimachinery/pkg/runtime
  k8s.io/apimachinery/pkg/util/intstr
  k8s.io/api/core/v1
  k8s.io/api/networking/v1
)

go run ${OPENAPI_PKG}/cmd/openapi-gen \
  --go-header-file "${SPARK_OPERATOR_ROOT}/hack/boilerplate.go.txt" \
  --output-pkg "${SPARK_OPERATOR_PKG}/api/v1beta2" \
  --output-dir "${SPARK_OPERATOR_ROOT}/api/v1beta2" \
  --output-file "zz_generated.openapi.go" \
  --report-filename "${SPARK_OPERATOR_ROOT}/hack/openapi/violation_exceptions.list" \
  "${EXTRA_PACKAGES[@]}" \
  "${SPARK_OPERATOR_ROOT}/api/v1beta2"

# Generating OpenAPI Swagger for Kubeflow Spark Operator.
echo "Generate OpenAPI Swagger for Kubeflow Spark Operator"
go run hack/swagger/main.go >api/openapi-spec/swagger.json
