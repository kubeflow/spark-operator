#!/bin/bash

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

# echo commands to the terminal output
set -ex

# Check whether there is a passwd entry for the container UID
myuid="$(id -u)"
# If there is no passwd entry for the container UID, attempt to fake one
# You can also refer to the https://github.com/docker-library/official-images/pull/13089#issuecomment-1534706523
# It's to resolve OpenShift random UID case.
# See also: https://github.com/docker-library/postgres/pull/448
if ! getent passwd "$myuid" &> /dev/null; then
  for wrapper in {/usr,}/lib{/*,}/libnss_wrapper.so; do
    if [ -s "$wrapper" ]; then
      NSS_WRAPPER_PASSWD="$(mktemp)"
      NSS_WRAPPER_GROUP="$(mktemp)"
      export LD_PRELOAD="$wrapper" NSS_WRAPPER_PASSWD NSS_WRAPPER_GROUP
      mygid="$(id -g)"
      printf 'spark:x:%s:%s:%s:%s:/bin/false\n' "$myuid" "$mygid" "${SPARK_USER_NAME:-anonymous uid}" "$SPARK_HOME" > "$NSS_WRAPPER_PASSWD"
      printf 'spark:x:%s:\n' "$mygid" > "$NSS_WRAPPER_GROUP"
      break
    fi
  done
fi

# Path for catatonit may differ depending on base container OS
CATATONIT=$(command -v catatonit) || {
  echo "error: catatonit not found in PATH" >&2
  exit 1
}
exec "$CATATONIT" -- /usr/bin/spark-operator "$@"
