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

# Bumps every versioned asset in the repository to a new release version and,
# for final releases, prepends a CHANGELOG.md section. Run it via:
#
#   make release VERSION=vX.Y.Z[-rc.N] [GITHUB_TOKEN=<token>]
#
# Environment:
#   VERSION            (required) vX.Y.Z or vX.Y.Z-rc.N
#   GITHUB_TOKEN       (required for final releases) token used to generate the changelog
#   UPSTREAM_REMOTE    git remote pointing at kubeflow/spark-operator (default: upstream, falls back to origin)
#   PREVIOUS_VERSION   override the start of the changelog range (default: computed)
#   CHANGELOG_HEAD_REF override the end of the changelog range (default: release-X.Y if it exists upstream, else master)

set -o errexit
set -o nounset
set -o pipefail

SEMVER_PATTERN='^v([0-9]+)\.([0-9]+)\.([0-9]+)(-rc\.([0-9]+))?$'

CHART_FILE="charts/spark-operator-chart/Chart.yaml"
KUSTOMIZATION_FILE="config/default/kustomization.yaml"
PYTHON_INIT_FILE="api/python_api/kubeflow_spark_api/__init__.py"

VERSION="${VERSION:-}"
if [[ ! ${VERSION} =~ ${SEMVER_PATTERN} ]]; then
  echo "ERROR: VERSION must be vX.Y.Z or vX.Y.Z-rc.N, got '${VERSION}'." >&2
  echo "Usage: make release VERSION=vX.Y.Z[-rc.N] [GITHUB_TOKEN=<token>]" >&2
  exit 1
fi
MAJOR="${BASH_REMATCH[1]}"
MINOR="${BASH_REMATCH[2]}"
PATCH="${BASH_REMATCH[3]}"
RC="${BASH_REMATCH[5]}"
CHART_VERSION="${VERSION#v}"

# Fail before touching any file if the changelog cannot be generated.
if [[ -z ${RC} ]]; then
  if [[ -z ${GITHUB_TOKEN:-} ]]; then
    echo "ERROR: GITHUB_TOKEN is required to generate the changelog for ${VERSION}." >&2
    echo "Usage: make release VERSION=${VERSION} GITHUB_TOKEN=<token>" >&2
    exit 1
  fi
  if ! python3 -c 'import github' 2> /dev/null; then
    echo "ERROR: PyGithub is required to generate the changelog: pip install PyGithub==2.3.0" >&2
    exit 1
  fi
fi

# Portable in-place sed (GNU and BSD).
sed_i() {
  if sed --version > /dev/null 2>&1; then
    sed -i "$@"
  else
    sed -i '' "$@"
  fi
}

printf '%s\n' "${VERSION}" > VERSION
echo "Updated VERSION to ${VERSION}"

sed_i -e "s/^version: .*/version: ${CHART_VERSION}/" \
  -e "s/^appVersion: .*/appVersion: ${CHART_VERSION}/" "${CHART_FILE}"
echo "Updated ${CHART_FILE} version and appVersion to ${CHART_VERSION}"

sed_i -e "s|^\([[:space:]]*newTag:\).*|\1 ${CHART_VERSION}|" "${KUSTOMIZATION_FILE}"
echo "Updated ${KUSTOMIZATION_FILE} image tag to ${CHART_VERSION}"

sed_i -e "s/^__version__ = .*/__version__ = \"${CHART_VERSION}\"/" "${PYTHON_INIT_FILE}"
echo "Updated ${PYTHON_INIT_FILE} __version__ to ${CHART_VERSION}"

if [[ -n ${RC} ]]; then
  echo "Skipping changelog generation for release candidate ${VERSION}."
  exit 0
fi

REMOTE="${UPSTREAM_REMOTE:-upstream}"
if ! git remote get-url "${REMOTE}" > /dev/null 2>&1; then
  REMOTE=origin
fi
git fetch --quiet --tags "${REMOTE}"

if [[ -z ${PREVIOUS_VERSION:-} ]]; then
  if ((PATCH > 0)); then
    PREVIOUS_VERSION="v${MAJOR}.${MINOR}.$((PATCH - 1))"
  else
    # First final release of a minor (or major) line: start from the newest older .0 final
    # release (release candidates excluded), the point the new line diverged from on master.
    PREVIOUS_VERSION=$(git tag --list 'v*' --sort=-v:refname |
      grep -E '^v[0-9]+\.[0-9]+\.0$' |
      awk -F'[v.]' -v maj="${MAJOR}" -v min="${MINOR}" \
        '($2 < maj) || ($2 == maj && $3 < min) { print; exit }' || true)
  fi
fi
if ! git rev-parse --verify --quiet "refs/tags/${PREVIOUS_VERSION}" > /dev/null; then
  echo "ERROR: previous release tag '${PREVIOUS_VERSION}' not found. Set PREVIOUS_VERSION explicitly." >&2
  exit 1
fi

RELEASE_BRANCH="release-${MAJOR}.${MINOR}"
if [[ -z ${CHANGELOG_HEAD_REF:-} ]]; then
  if git ls-remote --exit-code --heads "${REMOTE}" "${RELEASE_BRANCH}" > /dev/null 2>&1; then
    CHANGELOG_HEAD_REF="${RELEASE_BRANCH}"
  else
    CHANGELOG_HEAD_REF=master
  fi
fi

echo "Generating changelog for ${VERSION} from ${PREVIOUS_VERSION}..${CHANGELOG_HEAD_REF}"
python3 hack/generate-changelog.py \
  --token="${GITHUB_TOKEN}" \
  --range="${PREVIOUS_VERSION}..${CHANGELOG_HEAD_REF}" \
  --version="${VERSION}"
