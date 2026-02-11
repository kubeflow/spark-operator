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

# Run this script from the root location: `make python-api`

set -o errexit
set -o nounset

# Use CONTAINER_TOOL from environment (passed by Makefile), default to docker
CONTAINER_TOOL="${CONTAINER_TOOL:-docker}"

SPARK_OPERATOR_ROOT="$(pwd)"

VERSION_FILE="${SPARK_OPERATOR_ROOT}/VERSION"
if [ ! -f "${VERSION_FILE}" ]; then
  echo "Missing VERSION file at ${VERSION_FILE}"
  exit 1
fi
API_VERSION=$(sed 's/^v//' "${VERSION_FILE}")
API_OUTPUT_PATH="api/python_api"
PKG_ROOT="${API_OUTPUT_PATH}/kubeflow_spark_api"

OPENAPI_GENERATOR_VERSION="v7.13.0"
SWAGGER_CODEGEN_CONF="hack/python-api/swagger_config.json"
SWAGGER_CODEGEN_FILE="api/openapi-spec/swagger.json"

echo "Generating Python API models for Kubeflow Spark Operator using ${CONTAINER_TOOL}..."
# We need to add user to allow container override existing files.
${CONTAINER_TOOL} run --user "$(id -u)":"$(id -g)" --rm \
  -v "${SPARK_OPERATOR_ROOT}:/local" docker.io/openapitools/openapi-generator-cli:${OPENAPI_GENERATOR_VERSION} generate \
  -g python \
  -i "local/${SWAGGER_CODEGEN_FILE}" \
  -c "local/${SWAGGER_CODEGEN_CONF}" \
  -o "local/${API_OUTPUT_PATH}" \
  -p=packageVersion="${API_VERSION}" \
  --global-property models,modelTests=false,modelDocs=false,supportingFiles=__init__.py

echo "Removing unused files for the Python API"
git clean -f ${API_OUTPUT_PATH}/.openapi-generator
git clean -f ${API_OUTPUT_PATH}/.github
git clean -f ${API_OUTPUT_PATH}/test
git clean -f ${PKG_ROOT}/api

# Revert manually created files.
git checkout ${PKG_ROOT}/__init__.py

# Manually modify the SDK version in the __init__.py file.
if [[ $(uname) == "Darwin" ]]; then
  sed -i '' -e "s/__version__.*/__version__ = \"${API_VERSION}\"/" ${PKG_ROOT}/__init__.py
else
  sed -i -e "s/__version__.*/__version__ = \"${API_VERSION}\"/" ${PKG_ROOT}/__init__.py
fi
