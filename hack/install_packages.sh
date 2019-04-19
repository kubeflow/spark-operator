#!/usr/bin/env bash
#
# Copyright 2019 The Kubernetes Authors.
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
set -e
arg1=$(head -2 /opt/spark/credentials | tail -1)
arg2=$(head -3 /opt/spark/credentials | tail -1)
arg3=$(head -1 /opt/spark/credentials | tail -1)

subscription-manager register --username=$arg1 --password=$arg2  --name=docker
subscription-manager attach --pool=$arg3 && yum install -y openssl
subscription-manager remove --all
subscription-manager unregister
subscription-manager clean
