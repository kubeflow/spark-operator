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

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_ROOT="${SCRIPT_DIR}/.."

makefile="${SCRIPT_ROOT}/Makefile"
dockerfile="${SCRIPT_ROOT}/Dockerfile"
verify_script="${SCRIPT_ROOT}/hack/verify-spark-version.sh"

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

TARGET_VERSION="${1:-}"

[[ -n "${TARGET_VERSION}" ]] ||
  fail "usage: $0 <spark-version>"

[[ "${TARGET_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] ||
  fail "Spark version must use x.y.z format, got: ${TARGET_VERSION}"

command -v docker > /dev/null 2>&1 ||
  fail "docker is required"

docker buildx version > /dev/null 2>&1 ||
  fail "docker buildx is required"

[[ -x "${verify_script}" ]] ||
  fail "verification script is not executable: ${verify_script}"

echo "Verifying current Spark runtime references..."
"${verify_script}"

runtime_pattern='^SPARK_IMAGE \?= docker\.io/apache/spark:[0-9]+\.[0-9]+\.[0-9]+@sha256:[0-9a-f]{64}$'

runtime_line="$(
  grep -E "${runtime_pattern}" "${makefile}" || true
)"

runtime_count="$(
  grep -Ec "${runtime_pattern}" "${makefile}" || true
)"

[[ "${runtime_count}" -eq 1 ]] ||
  fail "expected exactly one SPARK_IMAGE runtime reference in Makefile"

if [[ "${runtime_line}" =~ docker\.io/apache/spark:([0-9]+\.[0-9]+\.[0-9]+)@sha256:([0-9a-f]{64}) ]]; then
  CURRENT_VERSION="${BASH_REMATCH[1]}"
else
  fail "unexpected SPARK_IMAGE format in Makefile"
fi

if [[ "${TARGET_VERSION}" == "${CURRENT_VERSION}" ]]; then
  fail "Spark runtime is already ${TARGET_VERSION}"
fi

WORKLOAD_IMAGE="docker.io/apache/spark:${TARGET_VERSION}"
CONTROLLER_IMAGE="docker.io/apache/spark:${TARGET_VERSION}-scala2.13-java21-ubuntu"

inspect_image() {
  local image="$1"
  local description="$2"
  local output
  local media_type
  local digest

  echo "Inspecting ${description}: ${image}" >&2

  if ! output="$(docker buildx imagetools inspect "${image}" 2>&1)"; then
    echo "${output}" >&2
    fail "${description} is not available: ${image}"
  fi

  media_type="$(
    awk '$1 == "MediaType:" { print $2; exit }' <<< "${output}"
  )"

  case "${media_type}" in
    application/vnd.oci.image.index.v1+json | application/vnd.docker.distribution.manifest.list.v2+json)
      ;;
    *)
      fail "${description} is not a multi-platform image index: ${image}"
      ;;
  esac

  digest="$(
    awk '$1 == "Digest:" { print $2; exit }' <<< "${output}"
  )"

  [[ "${digest}" =~ ^sha256:[0-9a-f]{64}$ ]] ||
    fail "could not determine index digest for ${image}"

  awk '$1 == "Platform:" && $2 == "linux/amd64" { found=1 } END { exit !found }' <<< "${output}" ||
    fail "${description} does not provide linux/amd64"

  awk '$1 == "Platform:" && $2 == "linux/arm64" { found=1 } END { exit !found }' <<< "${output}" ||
    fail "${description} does not provide linux/arm64"

  printf '%s\n' "${digest}"
}

WORKLOAD_DIGEST="$(inspect_image "${WORKLOAD_IMAGE}" "Spark workload image")"
CONTROLLER_DIGEST="$(inspect_image "${CONTROLLER_IMAGE}" "Spark controller image")"

echo
echo "Spark runtime update:"
echo "  current version:    ${CURRENT_VERSION}"
echo "  target version:     ${TARGET_VERSION}"
echo "  workload image:     ${WORKLOAD_IMAGE}@${WORKLOAD_DIGEST}"
echo "  controller image:   ${CONTROLLER_IMAGE}@${CONTROLLER_DIGEST}"
echo

managed_version_files=(
  "config/samples/v1beta2_scheduledsparkapplication.yaml"
  "config/samples/v1beta2_sparkapplication.yaml"

  "examples/spark-pi-configmap.yaml"
  "examples/spark-pi-custom-resource.yaml"
  "examples/spark-pi-dynamic-allocation.yaml"
  "examples/spark-pi-emptydir.yaml"
  "examples/spark-pi-kube-scheduler.yaml"
  "examples/spark-pi-pod-template.yaml"
  "examples/spark-pi-prometheus-servlet.yaml"
  "examples/spark-pi-python.yaml"
  "examples/spark-pi-scheduled.yaml"
  "examples/spark-pi-ttl.yaml"
  "examples/spark-pi-volcano.yaml"
  "examples/spark-pi-yunikorn.yaml"
  "examples/spark-pi.yaml"

  "examples/sparkconnect/spark-connect-custom-resource.yaml"
  "examples/sparkconnect/spark-connect-dependencies.yaml"
  "examples/sparkconnect/spark-connect.yaml"

  "test/e2e/bad_examples/fail-application.yaml"
  "test/e2e/bad_examples/fail-submission.yaml"
  "test/e2e/sparkconnect_resources_test.go"

  "docs/website/contributor-guide/index.md"
  "docs/website/index.rst"
  "docs/website/user-guide/building-custom-images.md"
  "docs/website/user-guide/running-sparkapplication-on-schedule.md"
  "docs/website/user-guide/spark-connect.md"
  "docs/website/user-guide/using-sparkapplication.md"
  "docs/website/user-guide/volcano-integration.md"
  "docs/website/user-guide/writing-sparkapplication.md"
)

for relative_file in "${managed_version_files[@]}"; do
  file="${SCRIPT_ROOT}/${relative_file}"
  [[ -f "${file}" ]] ||
    fail "managed file does not exist: ${relative_file}"
done

echo "Updating Spark runtime pins..."

sed -i.bak -E \
  "s|^SPARK_IMAGE \?= docker\.io/apache/spark:${CURRENT_VERSION}@sha256:[0-9a-f]{64}$|SPARK_IMAGE ?= ${WORKLOAD_IMAGE}@${WORKLOAD_DIGEST}|" \
  "${makefile}"
rm -f "${makefile}.bak"

sed -i.bak -E \
  "s|^ARG SPARK_IMAGE=docker\.io/apache/spark:${CURRENT_VERSION}-scala2\.13-java21-ubuntu@sha256:[0-9a-f]{64}$|ARG SPARK_IMAGE=${CONTROLLER_IMAGE}@${CONTROLLER_DIGEST}|" \
  "${dockerfile}"
rm -f "${dockerfile}.bak"

current_version_regex="${CURRENT_VERSION//./\\.}"

for relative_file in "${managed_version_files[@]}"; do
  file="${SCRIPT_ROOT}/${relative_file}"

  sed -E -i.bak \
    -e "s|(apache/spark:)${current_version_regex}|\\1${TARGET_VERSION}|g" \
    -e "s|(your-registry/spark:)${current_version_regex}|\\1${TARGET_VERSION}|g" \
    -e "s|(sparkVersion:[[:space:]]*\"?)${current_version_regex}|\\1${TARGET_VERSION}|g" \
    -e "s|(SparkVersion:[[:space:]]*\")${current_version_regex}|\\1${TARGET_VERSION}|g" \
    -e "s|(spark\\.apache\\.org/version:[[:space:]]*\"?)${current_version_regex}|\\1${TARGET_VERSION}|g" \
    -e "s|(org\\.apache\\.spark:spark-avro_2\\.13:)${current_version_regex}|\\1${TARGET_VERSION}|g" \
    "${file}"

  rm -f "${file}.bak"
done

label_version_files=(
  "config/samples/v1beta2_scheduledsparkapplication.yaml"
  "config/samples/v1beta2_sparkapplication.yaml"

  "examples/spark-pi-emptydir.yaml"
  "examples/spark-pi.yaml"

  "test/e2e/bad_examples/fail-application.yaml"
  "test/e2e/bad_examples/fail-submission.yaml"

  "docs/website/user-guide/volcano-integration.md"
)

for relative_file in "${label_version_files[@]}"; do
  file="${SCRIPT_ROOT}/${relative_file}"

  sed -E -i.bak \
    "s|(^[[:space:]]+version:[[:space:]]*\"?)${current_version_regex}|\\1${TARGET_VERSION}|g" \
    "${file}"

  rm -f "${file}.bak"
done

spark_connect_doc="${SCRIPT_ROOT}/docs/website/user-guide/spark-connect.md"

sed -i.bak \
  "s|Spark version \`${CURRENT_VERSION}\`|Spark version \`${TARGET_VERSION}\`|g" \
  "${spark_connect_doc}"
rm -f "${spark_connect_doc}.bak"

contributor_guide="${SCRIPT_ROOT}/docs/website/contributor-guide/index.md"

sed -i.bak \
  "s|Spark ${CURRENT_VERSION} Scala-only image|Spark ${TARGET_VERSION} Scala-only image|g" \
  "${contributor_guide}"
rm -f "${contributor_guide}.bak"

landing_page="${SCRIPT_ROOT}/docs/website/index.rst"

sed -i.bak \
  -e "s|docker.io/apache/spark:<span class=\"hl-num\">${CURRENT_VERSION}</span>|docker.io/apache/spark:<span class=\"hl-num\">${TARGET_VERSION}</span>|g" \
  -e "s|sparkVersion</span>: <span class=\"hl-str\">\"${CURRENT_VERSION}\"</span>|sparkVersion</span>: <span class=\"hl-str\">\"${TARGET_VERSION}\"</span>|g" \
  "${landing_page}"
rm -f "${landing_page}.bak"

echo "Verifying updated Spark runtime references..."
"${verify_script}"

echo
echo "Spark runtime updated successfully:"
echo "  ${CURRENT_VERSION} -> ${TARGET_VERSION}"
echo
echo "Review the resulting diff, then run the project validation and e2e tests."
