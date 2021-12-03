#
# Copyright 2021 Google LLC
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
#

FROM golang:1.15.2-alpine

RUN apk add git
RUN git clone https://github.com/ahmetb/gen-crd-api-reference-docs.git && \
    cd gen-crd-api-reference-docs && \
    git checkout ccf856504caaeac38151b57a950d3f8a7942b9db && \
    go build