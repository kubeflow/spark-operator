#!/usr/bin/env bash

# Copyright 2017 The Kubernetes Authors.
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

SCRIPT_ROOT=$(dirname "${BASH_SOURCE[0]}")/..

_tmp="$(mktemp -d)"
DIFF_ROOTS=("${SCRIPT_ROOT}/api" "${SCRIPT_ROOT}/pkg")

cleanup() {
  rm -rf "${_tmp}"
}
trap "cleanup" EXIT SIGINT

for root in "${DIFF_ROOTS[@]}"; do
  rel="${root#"${SCRIPT_ROOT}/"}"
  mkdir -p "${_tmp}/${rel}"
  cp -a "${root}"/* "${_tmp}/${rel}"
done

"${SCRIPT_ROOT}/hack/update-codegen.sh"

ret=0
for root in "${DIFF_ROOTS[@]}"; do
  rel="${root#"${SCRIPT_ROOT}/"}"
  echo "diffing ${root} against freshly generated codegen"
  diff -Naupr "${root}" "${_tmp}/${rel}" || ret=$?
  cp -a "${_tmp}/${rel}"/* "${root}"
done

if [[ $ret -eq 0 ]]; then
  echo "codegen up to date."
else
  echo "codegen is out of date. Please run hack/update-codegen.sh"
  exit 1
fi
