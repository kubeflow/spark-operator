#!/bin/bash

DOCKERFILE_RESOURCES=$(cat Dockerfile | grep -o "COPY [a-zA-Z0-9].*? " | cut -c6-)

for resource in $DOCKERFILE_RESOURCES; do
  # If the resource is different
  if ! git diff  --quiet origin/master -- $resource; then
    ## And the appVersion hasn't been updated
    if ! git diff origin/master -- charts/spark-operator-chart/Chart.yaml | grep +appVersion; then
      echo "resource used in docker.io/kubeflow/spark-operator has changed in $resource, need to update the appVersion in charts/spark-operator-chart/Chart.yaml"
      git diff origin/master -- $resource;
      echo "failing the build... " && false
    fi
  fi
done
