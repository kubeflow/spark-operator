#!/usr/bin/env bash
# scripts/setup-envtest-binaries.sh
# Download kube-apiserver and etcd for envtest to the location expected by tests.
# Usage: from repo root: bash scripts/setup-envtest-binaries.sh
# (Make executable with chmod +x scripts/setup-envtest-binaries.sh)

set -euo pipefail

# --- Configuration: edit if your tests expect a different K8S version
K8S_VERSION="v1.32.0"            # kube-apiserver/kube-controller-manager version envtest expects
ETCD_VERSION="v3.5.11"           # etcd version (3.5.x recommended)
TARGET_DIR="./bin/k8s/${K8S_VERSION}-linux-amd64"  # target where envtest looked for binaries

echo
echo "==> Setting up envtest binaries"
echo "Kubernetes version: ${K8S_VERSION}"
echo "etcd version:       ${ETCD_VERSION}"
echo "Target directory:   ${TARGET_DIR}"
echo

mkdir -p "${TARGET_DIR}"

# Helper to check for curl or wget
download() {
  local url="$1" out="$2"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL -o "${out}" "${url}"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO "${out}" "${url}"
  else
    echo "Error: need curl or wget to download files." >&2
    exit 2
  fi
}

# Download Kubernetes server tarball and extract kube-apiserver
K8S_TAR="/tmp/k8s-server-${K8S_VERSION}.tar.gz"
echo "Downloading Kubernetes server ${K8S_VERSION} -> ${K8S_TAR} ..."
download "https://dl.k8s.io/${K8S_VERSION}/kubernetes-server-linux-amd64.tar.gz" "${K8S_TAR}"

echo "Extracting kube-apiserver from ${K8S_TAR}..."
rm -rf /tmp/kubernetes || true
tar -C /tmp -xzf "${K8S_TAR}"

KUBE_APISERVER_SRC="/tmp/kubernetes/server/bin/kube-apiserver"
if [ ! -f "${KUBE_APISERVER_SRC}" ]; then
  echo "Error: kube-apiserver not found at ${KUBE_APISERVER_SRC}" >&2
  echo "Listing /tmp/kubernetes contents:"
  ls -la /tmp/kubernetes || true
  exit 3
fi

cp -v "${KUBE_APISERVER_SRC}" "${TARGET_DIR}/"

# Download etcd tarball and extract etcd binary
ETCD_TAR="/tmp/etcd-${ETCD_VERSION}-linux-amd64.tar.gz"
echo "Downloading etcd ${ETCD_VERSION} -> ${ETCD_TAR} ..."
download "https://github.com/etcd-io/etcd/releases/download/${ETCD_VERSION}/etcd-${ETCD_VERSION}-linux-amd64.tar.gz" "${ETCD_TAR}"

echo "Extracting etcd from ${ETCD_TAR}..."
rm -rf "/tmp/etcd-${ETCD_VERSION}-linux-amd64" || true
tar -C /tmp -xzf "${ETCD_TAR}"

ETCD_SRC="/tmp/etcd-${ETCD_VERSION}-linux-amd64/etcd"
if [ -f "${ETCD_SRC}" ]; then
  cp -v "${ETCD_SRC}" "${TARGET_DIR}/"
else
  # fallback: search extracted tmp for etcd binary
  found_etcd=$(find /tmp -type f -name etcd -print -quit || true)
  if [ -n "${found_etcd}" ]; then
    cp -v "${found_etcd}" "${TARGET_DIR}/"
  else
    echo "ERROR: etcd binary not found inside extracted etcd tarball." >&2
    exit 4
  fi
fi

# Make everything executable
chmod +x "${TARGET_DIR}"/*

# Export KUBEBUILDER_ASSETS for current shell user (print instruction)
ABS_TARGET_DIR="$(cd "$(dirname "${TARGET_DIR}")" && pwd)/$(basename "${TARGET_DIR}")"
echo
echo "Binaries installed in: ${ABS_TARGET_DIR}"
echo
echo "To use these binaries for running tests in this shell, run:"
echo "  export KUBEBUILDER_ASSETS=\"${ABS_TARGET_DIR}\""
echo
echo "You can also add that export line to your shell profile (e.g. ~/.bashrc) if you want it persistent."
echo

# Add bin/ to .gitignore if not present (local convenience)
if ! rg -q "^bin/" .gitignore 2>/dev/null || [ ! -f .gitignore ]; then
  echo "Adding 'bin/' to .gitignore (local convenience)"
  echo "bin/" >> .gitignore
  echo "Note: bin/ contains large local binaries; do not commit them."
fi

echo "Done. Now run the tests that failed, e.g.:"
echo "  go test ./internal/controller/scheduledsparkapplication -v"
echo "or to run all controller tests:"
echo "  go test ./internal/controller/... -v"
