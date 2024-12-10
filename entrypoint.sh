#!/bin/bash

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
        printf 'spark:x:%s:%s:${SPARK_USER_NAME:-anonymous uid}:%s:/bin/false\n' "$myuid" "$mygid" "$SPARK_HOME" > "$NSS_WRAPPER_PASSWD"
        printf 'spark:x:%s:\n' "$mygid" > "$NSS_WRAPPER_GROUP"
        break
      fi
    done
fi

exec /usr/bin/tini -s -- /usr/bin/spark-operator "$@"
