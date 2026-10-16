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

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

runtime_pattern='^SPARK_IMAGE \?= docker\.io/apache/spark:[0-9]+\.[0-9]+\.[0-9]+@sha256:[0-9a-f]{64}$'

runtime_line="$(
  grep -E "${runtime_pattern}" "${makefile}" || true
)"

runtime_count="$(
  grep -Ec "${runtime_pattern}" "${makefile}" || true
)"

[[ "${runtime_count}" -eq 1 ]] ||
  fail "expected exactly one Spark runtime image reference in Makefile"

if [[ "${runtime_line}" =~ docker\.io/apache/spark:([0-9]+\.[0-9]+\.[0-9]+)@sha256:([0-9a-f]{64}) ]]; then
  SPARK_VERSION="${BASH_REMATCH[1]}"
  SPARK_IMAGE_DIGEST="${BASH_REMATCH[2]}"
else
  fail "unexpected SPARK_IMAGE format in Makefile"
fi

controller_pattern='^ARG SPARK_IMAGE=docker\.io/apache/spark:[0-9]+\.[0-9]+\.[0-9]+-scala2\.13-java21-ubuntu@sha256:[0-9a-f]{64}$'

controller_line="$(
  grep -E "${controller_pattern}" "${dockerfile}" || true
)"

controller_count="$(
  grep -Ec "${controller_pattern}" "${dockerfile}" || true
)"

[[ "${controller_count}" -eq 1 ]] ||
  fail "expected exactly one Spark controller base image reference in Dockerfile"

if [[ "${controller_line}" =~ docker\.io/apache/spark:([0-9]+\.[0-9]+\.[0-9]+)-scala2\.13-java21-ubuntu@sha256:([0-9a-f]{64}) ]]; then
  CONTROLLER_SPARK_VERSION="${BASH_REMATCH[1]}"
  CONTROLLER_IMAGE_DIGEST="${BASH_REMATCH[2]}"
else
  fail "unexpected Spark controller image format in Dockerfile"
fi

if [[ "${CONTROLLER_SPARK_VERSION}" != "${SPARK_VERSION}" ]]; then
  fail "Spark version mismatch: Makefile=${SPARK_VERSION}, Dockerfile=${CONTROLLER_SPARK_VERSION}"
fi

managed_files=(
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

for relative_file in "${managed_files[@]}"; do
  file="${SCRIPT_ROOT}/${relative_file}"
  [[ -f "${file}" ]] || fail "managed file does not exist: ${relative_file}"
done

check_versions() {
  local relative_file="$1"
  local pattern="$2"
  local description="$3"
  local file="${SCRIPT_ROOT}/${relative_file}"
  local match
  local version

  while IFS= read -r match; do
    [[ -n "${match}" ]] || continue
    version="$(grep -Eo '[0-9]+\.[0-9]+\.[0-9]+' <<< "${match}")"
    if [[ "${version}" != "${SPARK_VERSION}" ]]; then
      echo "ERROR: ${relative_file}: ${description} uses ${version}, expected ${SPARK_VERSION}" >&2
      return 1
    fi
  done < <(grep -Eo "${pattern}" "${file}" || true)
}

ret=0

for relative_file in "${managed_files[@]}"; do
  check_versions \
    "${relative_file}" \
    '((docker\.io/)?apache/spark|your-registry/spark):[0-9]+\.[0-9]+\.[0-9]+' \
    "Spark image" || ret=1

  check_versions \
    "${relative_file}" \
    '(sparkVersion|SparkVersion):[[:space:]]*"?[0-9]+\.[0-9]+\.[0-9]+' \
    "sparkVersion" || ret=1

  check_versions \
    "${relative_file}" \
    'spark\.apache\.org/version:[[:space:]]*"?[0-9]+\.[0-9]+\.[0-9]+' \
    "Spark version label" || ret=1

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
  check_versions \
    "${relative_file}" \
    '^[[:space:]]+version:[[:space:]]*"?[0-9]+\.[0-9]+\.[0-9]+' \
    "Spark version label" || ret=1
done

check_versions \
  "examples/sparkconnect/spark-connect-dependencies.yaml" \
  'org\.apache\.spark:spark-avro_2\.13:[0-9]+\.[0-9]+\.[0-9]+' \
  "spark-avro dependency" || ret=1

grep -Fq \
  "Spark version \`${SPARK_VERSION}\`" \
  "${SCRIPT_ROOT}/docs/website/user-guide/spark-connect.md" ||
  {
    echo "ERROR: spark-connect.md does not document Spark version ${SPARK_VERSION}" >&2
    ret=1
  }

grep -Fq \
  "Spark ${SPARK_VERSION} Scala-only image" \
  "${SCRIPT_ROOT}/docs/website/contributor-guide/index.md" ||
  {
    echo "ERROR: contributor guide does not document Spark ${SPARK_VERSION} controller image" >&2
    ret=1
  }

landing_page="${SCRIPT_ROOT}/docs/website/index.rst"

grep -Fq \
  "docker.io/apache/spark:<span class=\"hl-num\">${SPARK_VERSION}</span>" \
  "${landing_page}" ||
  {
    echo "ERROR: landing page Spark image does not use ${SPARK_VERSION}" >&2
    ret=1
  }

grep -Fq \
  "sparkVersion</span>: <span class=\"hl-str\">\"${SPARK_VERSION}\"</span>" \
  "${landing_page}" ||
  {
    echo "ERROR: landing page sparkVersion does not use ${SPARK_VERSION}" >&2
    ret=1
  }

if [[ "${ret}" -ne 0 ]]; then
  exit "${ret}"
fi

echo "Spark runtime references are consistent."
echo "  version:           ${SPARK_VERSION}"
echo "  workload digest:   sha256:${SPARK_IMAGE_DIGEST}"
echo "  controller digest: sha256:${CONTROLLER_IMAGE_DIGEST}"
echo
echo "Excluded from automatic runtime updates:"
echo "  examples/spark-pi-prometheus.yaml"
echo "  docs/website/user-guide/monitoring-with-jmx-and-prometheus.md"
echo "  historical release data and version-independent test fixtures"
