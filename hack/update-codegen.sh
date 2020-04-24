#!/bin/bash
#
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

SCRIPT_ROOT=$(dirname ${BASH_SOURCE})/..

# generate the code with:
# --output-base    because this script should also be able to run inside the vendor dir of
#                  k8s.io/kubernetes. The output-base is needed for the generators to output into the vendor dir
#                  instead of the $GOPATH directly. For normal projects this can be dropped.
${SCRIPT_ROOT}/hack/generate-groups.sh "all" \
  github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis \
  sparkoperator.k8s.io:v1beta1,v1beta2 \
  --go-header-file "$(dirname ${BASH_SOURCE})/custom-boilerplate.go.txt" \
  --output-base "$(dirname ${BASH_SOURCE})/../../../.."

# To use your own boilerplate text append:
#   --go-header-file ${SCRIPT_ROOT}/hack/custom-boilerplate.go.txt

