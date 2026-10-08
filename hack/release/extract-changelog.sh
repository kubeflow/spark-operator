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

# Prints the body of the CHANGELOG.md section for a release, i.e. everything
# between "## [<version>]..." and the next "## [v..." header.
#
# Usage: hack/release/extract-changelog.sh vX.Y.Z [CHANGELOG.md]

set -o errexit
set -o nounset
set -o pipefail

VERSION="${1:?usage: $0 vX.Y.Z [CHANGELOG.md]}"
CHANGELOG_FILE="${2:-CHANGELOG.md}"

if [[ ! -f ${CHANGELOG_FILE} ]]; then
  echo "ERROR: ${CHANGELOG_FILE} not found" >&2
  exit 1
fi

body=$(awk -v header="## [${VERSION}]" '
  index($0, header) == 1 { found = 1; next }
  found && /^## \[v[0-9]/ { exit }
  found { print }
' "${CHANGELOG_FILE}")

# Trim leading and trailing blank lines.
body=$(printf '%s\n' "${body}" | sed -e '/./,$!d' | sed -e ':a' -e '/^\n*$/{$d;N;ba' -e '}')

if [[ -z ${body} ]]; then
  echo "ERROR: no changelog section for ${VERSION} in ${CHANGELOG_FILE}" >&2
  exit 1
fi
printf '%s\n' "${body}"
