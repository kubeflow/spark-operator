#!/usr/bin/env bash

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
helm repo add --force-update spark-operator "${HELM_REPO_URL}"
helm repo update spark-operator
helm search repo spark-operator/spark-operator --versions --devel | grep -F " ${VERSION} "

echo
echo "==> Checking Python SDK package: kubeflow-spark-api==${PEP440_VERSION}"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "${TMP_DIR}"' EXIT
pip download --no-deps --dest "${TMP_DIR}" "kubeflow-spark-api==${PEP440_VERSION}"

echo
echo "==> Checking Kustomize manifests: config/default?ref=v${VERSION}"
kustomize build "github.com/kubeflow/spark-operator/config/default?ref=v${VERSION}" | grep "image:"

echo
echo "All release artifacts for v${VERSION} verified successfully."
