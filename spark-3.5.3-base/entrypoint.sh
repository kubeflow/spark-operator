#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Prevent any errors from being silently ignored
set -eo pipefail

attempt_setup_fake_passwd_entry() {
  # Check whether there is a passwd entry for the container UID
  local myuid; myuid="$(id -u)"
  # If there is no passwd entry for the container UID, attempt to fake one
  # You can also refer to the https://github.com/docker-library/official-images/pull/13089#issuecomment-1534706523
  # It's to resolve OpenShift random UID case.
  # See also: https://github.com/docker-library/postgres/pull/448
  if ! getent passwd "$myuid" &> /dev/null; then
      local wrapper
      for wrapper in {/usr,}/lib{/*,}/libnss_wrapper.so; do
        if [ -s "$wrapper" ]; then
          NSS_WRAPPER_PASSWD="$(mktemp)"
          NSS_WRAPPER_GROUP="$(mktemp)"
          export LD_PRELOAD="$wrapper" NSS_WRAPPER_PASSWD NSS_WRAPPER_GROUP
          local mygid; mygid="$(id -g)"
          printf 'spark:x:%s:%s:${SPARK_USER_NAME:-anonymous uid}:%s:/bin/false\n' "$myuid" "$mygid" "$SPARK_HOME" > "$NSS_WRAPPER_PASSWD"
          printf 'spark:x:%s:\n' "$mygid" > "$NSS_WRAPPER_GROUP"
          break
        fi
      done
  fi
}

if [ -z "$JAVA_HOME" ]; then
  JAVA_HOME=$(java -XshowSettings:properties -version 2>&1 > /dev/null | grep 'java.home' | awk '{print $3}')
fi

SPARK_CLASSPATH="$SPARK_CLASSPATH:${SPARK_HOME}/jars/*"
for v in "${!SPARK_JAVA_OPT_@}"; do
    SPARK_EXECUTOR_JAVA_OPTS+=( "${!v}" )
done

if [ -n "$SPARK_EXTRA_CLASSPATH" ]; then
  SPARK_CLASSPATH="$SPARK_CLASSPATH:$SPARK_EXTRA_CLASSPATH"
fi

if ! [ -z "${PYSPARK_PYTHON+x}" ]; then
    export PYSPARK_PYTHON
fi
if ! [ -z "${PYSPARK_DRIVER_PYTHON+x}" ]; then
    export PYSPARK_DRIVER_PYTHON
fi

# If HADOOP_HOME is set and SPARK_DIST_CLASSPATH is not set, set it here so Hadoop jars are available to the executor.
# It does not set SPARK_DIST_CLASSPATH if already set, to avoid overriding customizations of this value from elsewhere e.g. Docker/K8s.
if [ -n "${HADOOP_HOME}"  ] && [ -z "${SPARK_DIST_CLASSPATH}"  ]; then
  export SPARK_DIST_CLASSPATH="$($HADOOP_HOME/bin/hadoop classpath)"
fi

if ! [ -z "${HADOOP_CONF_DIR+x}" ]; then
  SPARK_CLASSPATH="$HADOOP_CONF_DIR:$SPARK_CLASSPATH";
fi

if ! [ -z "${SPARK_CONF_DIR+x}" ]; then
  SPARK_CLASSPATH="$SPARK_CONF_DIR:$SPARK_CLASSPATH";
elif ! [ -z "${SPARK_HOME+x}" ]; then
  SPARK_CLASSPATH="$SPARK_HOME/conf:$SPARK_CLASSPATH";
fi

# SPARK-43540: add current working directory into executor classpath
SPARK_CLASSPATH="$SPARK_CLASSPATH:$PWD"

# Switch to spark if no USER specified (root by default) otherwise use USER directly
switch_spark_if_root() {
  if [ $(id -u) -eq 0 ]; then
    echo gosu spark
  fi
}

case "$1" in
  driver)
    shift 1
    CMD=(
      "$SPARK_HOME/bin/spark-submit"
      --conf "spark.driver.bindAddress=$SPARK_DRIVER_BIND_ADDRESS"
      --conf "spark.executorEnv.SPARK_DRIVER_POD_IP=$SPARK_DRIVER_BIND_ADDRESS"
      --deploy-mode client
      "$@"
    )
    attempt_setup_fake_passwd_entry
    # Execute the container CMD under tini for better hygiene
    exec $(switch_spark_if_root) /usr/bin/tini -s -- "${CMD[@]}"
    ;;
  executor)
    shift 1
    CMD=(
      ${JAVA_HOME}/bin/java
      "${SPARK_EXECUTOR_JAVA_OPTS[@]}"
      -Xms"$SPARK_EXECUTOR_MEMORY"
      -Xmx"$SPARK_EXECUTOR_MEMORY"
      -cp "$SPARK_CLASSPATH:$SPARK_DIST_CLASSPATH"
      org.apache.spark.scheduler.cluster.k8s.KubernetesExecutorBackend
      --driver-url "$SPARK_DRIVER_URL"
      --executor-id "$SPARK_EXECUTOR_ID"
      --cores "$SPARK_EXECUTOR_CORES"
      --app-id "$SPARK_APPLICATION_ID"
      --hostname "$SPARK_EXECUTOR_POD_IP"
      --resourceProfileId "$SPARK_RESOURCE_PROFILE_ID"
      --podName "$SPARK_EXECUTOR_POD_NAME"
    )
    attempt_setup_fake_passwd_entry
    # Execute the container CMD under tini for better hygiene
    exec $(switch_spark_if_root) /usr/bin/tini -s -- "${CMD[@]}"
    ;;

  *)
    # Non-spark-on-k8s command provided, proceeding in pass-through mode...
    exec "$@"
    ;;
esac