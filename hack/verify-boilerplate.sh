#!/usr/bin/env bash

# Copyright The Kubeflow authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Wrapper script that calls boilerplate.py to verify copyright headers.
# See: https://github.com/kubernetes/steering/issues/299

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_ROOT=$(dirname "${BASH_SOURCE[0]}")/..
cd "${SCRIPT_ROOT}"

# Run the Python verification script
ret=0
files=$(python3 hack/boilerplate/boilerplate.py "$@") || ret=1

if [[ $ret -eq 1 ]]; then
    echo "" >&2
    echo "Boilerplate header verification failed for the following files:" >&2
    echo "" >&2
    for file in ${files}; do
        echo "  ${file}" >&2
    done
    echo "" >&2
    echo "For new files, use the copyright header WITHOUT a year:" >&2
    echo "  Copyright The Kubeflow authors." >&2
    echo "" >&2
    echo "See hack/boilerplate for the complete list of templates." >&2
    echo "Reference: https://github.com/kubernetes/steering/issues/299" >&2
    exit 1
fi

echo "Boilerplate header verification passed."
exit 0
