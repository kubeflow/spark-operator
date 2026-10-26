#!/usr/bin/env bash

# Copyright The Kubeflow Authors.
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

# Verifies that every published artifact of a Spark Operator release is
# available: the controller image, the Helm chart (OCI and the classic
# repo), the Python SDK package on PyPI, and the Kustomize manifests.
#
# Usage: VERSION=X.Y.Z hack/verify-release.sh

set -euo pipefail

if [[ -z "${VERSION:-}" ]]; then
  echo "VERSION is not set. Usage: VERSION=X.Y.Z hack/verify-release.sh" >&2
  exit 1
fi

# Accept both X.Y.Z and vX.Y.Z so a tag copy-pasted from GitHub works too.
VERSION="${VERSION#v}"

# Spark Operator uses SemVer-style release-candidate tags (X.Y.Z-rc.N) but
# PyPI requires PEP 440 version identifiers (X.Y.ZrcN).
PEP440_VERSION="${VERSION/-rc./rc}"

HELM_REPO_URL="https://kubeflow.github.io/spark-operator"

echo "Verifying release artifacts for v${VERSION}..."

echo
echo "==> Checking controller image: ghcr.io/kubeflow/spark-operator/controller:${VERSION}"
docker buildx imagetools inspect "ghcr.io/kubeflow/spark-operator/controller:${VERSION}"

echo
echo "==> Checking Helm chart (OCI): oci://ghcr.io/kubeflow/helm-charts/spark-operator@${VERSION}"
helm show chart "oci://ghcr.io/kubeflow/helm-charts/spark-operator" --version "${VERSION}"

echo
echo "==> Checking Helm chart (repo): spark-operator/spark-operator@${VERSION}"
helm repo add --force-update spark-operator "${HELM_REPO_URL}" > /dev/null
helm repo update spark-operator > /dev/null
helm show chart spark-operator/spark-operator --version "${VERSION}"

echo
echo "==> Checking Python SDK package: kubeflow-spark-api==${PEP440_VERSION}"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "${TMP_DIR}"' EXIT
python3 -m pip download --no-deps --dest "${TMP_DIR}" "kubeflow-spark-api==${PEP440_VERSION}"

echo
echo "==> Checking Kustomize manifests: config/default?ref=v${VERSION}"
kustomize build "github.com/kubeflow/spark-operator/config/default?ref=v${VERSION}" | grep -F "ghcr.io/kubeflow/spark-operator/controller"

echo
echo "All release artifacts for v${VERSION} verified successfully."
