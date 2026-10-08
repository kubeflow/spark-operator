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

# Validates that every versioned asset in the repository agrees with the
# VERSION file. Used by the "Check Release" PR workflow and by the "Release"
# workflow before anything is published.
#
# Usage: hack/release/check-release.sh [--require-new-tag]
#
#   --require-new-tag  Fail if a git tag named after VERSION already exists.
#
# When GITHUB_OUTPUT is set, the following outputs are written to it:
#   version        vX.Y.Z[-rc.N]  (git tag, GitHub release name)
#   chart-version  X.Y.Z[-rc.N]   (Helm chart version, image tags)
#   pypi-version   X.Y.Z[rcN]     (PEP 440 normalized Python package version)
#   major-minor    X.Y
#   branch         release-X.Y
#   is-prerelease  true|false

set -o errexit
set -o nounset
set -o pipefail

SEMVER_PATTERN='^v([0-9]+)\.([0-9]+)\.([0-9]+)(-rc\.([0-9]+))?$'

CHART_FILE="charts/spark-operator-chart/Chart.yaml"
KUSTOMIZATION_FILE="config/default/kustomization.yaml"
PYTHON_INIT_FILE="api/python_api/kubeflow_spark_api/__init__.py"
CHANGELOG_FILE="CHANGELOG.md"

REQUIRE_NEW_TAG=false
for arg in "$@"; do
  case "${arg}" in
    --require-new-tag) REQUIRE_NEW_TAG=true ;;
    *)
      echo "Unknown argument: ${arg}" >&2
      exit 1
      ;;
  esac
done

errors=0
fail() {
  echo "ERROR: $*" >&2
  errors=$((errors + 1))
}

VERSION=$(tr -d ' \n' < VERSION)
if [[ ! ${VERSION} =~ ${SEMVER_PATTERN} ]]; then
  echo "ERROR: VERSION '${VERSION}' does not match ${SEMVER_PATTERN}" >&2
  exit 1
fi
MAJOR="${BASH_REMATCH[1]}"
MINOR="${BASH_REMATCH[2]}"
PATCH="${BASH_REMATCH[3]}"
RC="${BASH_REMATCH[5]}"

CHART_VERSION="${VERSION#v}"
MAJOR_MINOR="${MAJOR}.${MINOR}"
BRANCH="release-${MAJOR_MINOR}"
if [[ -n ${RC} ]]; then
  IS_PRERELEASE=true
  PYPI_VERSION="${MAJOR}.${MINOR}.${PATCH}rc${RC}"
else
  IS_PRERELEASE=false
  PYPI_VERSION="${MAJOR}.${MINOR}.${PATCH}"
fi
echo "VERSION=${VERSION} chart=${CHART_VERSION} pypi=${PYPI_VERSION} branch=${BRANCH} prerelease=${IS_PRERELEASE}"

# Helm chart version and appVersion (the chart uses appVersion as the default image tag).
chart_version=$(awk '/^version:/{print $2; exit}' "${CHART_FILE}")
chart_app_version=$(awk '/^appVersion:/{print $2; exit}' "${CHART_FILE}" | tr -d '"')
[[ ${chart_version} == "${CHART_VERSION}" ]] ||
  fail "${CHART_FILE} version '${chart_version}' != '${CHART_VERSION}'"
[[ ${chart_app_version} == "${CHART_VERSION}" ]] ||
  fail "${CHART_FILE} appVersion '${chart_app_version}' != '${CHART_VERSION}'"

# Kustomize image tag. Images are published without the leading "v".
kustomize_tag=$(awk '/newTag:/{print $2; exit}' "${KUSTOMIZATION_FILE}" | tr -d '"')
[[ ${kustomize_tag} == "${CHART_VERSION}" ]] ||
  fail "${KUSTOMIZATION_FILE} newTag '${kustomize_tag}' != '${CHART_VERSION}' (run: make kustomize-set-image)"

# Python API package version.
python_version=$(sed -n 's/^__version__ = "\(.*\)"/\1/p' "${PYTHON_INIT_FILE}")
[[ ${python_version} == "${CHART_VERSION}" ]] ||
  fail "${PYTHON_INIT_FILE} __version__ '${python_version}' != '${CHART_VERSION}'"

# Changelog entry is mandatory for final releases.
if [[ ${IS_PRERELEASE} == false ]]; then
  grep -qE "^## \[${VERSION//./\\.}\]" "${CHANGELOG_FILE}" ||
    fail "${CHANGELOG_FILE} has no '## [${VERSION}]' section (run: make release VERSION=${VERSION} GITHUB_TOKEN=...)"
fi

if [[ ${REQUIRE_NEW_TAG} == true ]]; then
  if git ls-remote --exit-code --tags origin "refs/tags/${VERSION}" > /dev/null 2>&1; then
    fail "tag '${VERSION}' already exists on origin"
  fi
fi

if ((errors > 0)); then
  echo "${errors} release check(s) failed." >&2
  exit 1
fi
echo "All release checks passed for ${VERSION}."

if [[ -n ${GITHUB_OUTPUT:-} ]]; then
  {
    echo "version=${VERSION}"
    echo "chart-version=${CHART_VERSION}"
    echo "pypi-version=${PYPI_VERSION}"
    echo "major-minor=${MAJOR_MINOR}"
    echo "branch=${BRANCH}"
    echo "is-prerelease=${IS_PRERELEASE}"
  } >> "${GITHUB_OUTPUT}"
fi
