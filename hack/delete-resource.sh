#!/bin/bash
#
# Copyright 2018 Google LLC
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
#
# Generates a CA certificate, a server key, and a server certificate signed by the CA.

set -e
SCRIPT=`basename ${BASH_SOURCE[0]}`

function usage {
  cat<< EOF
  Info: $SCRIPT is used for deleting a resource from inside a pod.
  Example usage: $SCRIPT -k namespace -n sparkoperator
  Options:
  -h | --help               	Display help information.
  -k | --kind <kind>            The kind of the resource to delete (e.g. namespaces, serviceaccounts). Needs to be plural.
  -n | --name <name>            The name of the resource to delete.
EOF
}

function parse_arguments {
  while [ $# -gt 0 ]
  do
    case "$1" in
      -k|--kind)
      if [ -n "$2" ]; then
        KIND="$2"
      else
        echo "-k or --kind requires a value and it needs to be plural (e.g. namespaces)."
        exit 1
      fi
      shift 2
      continue
      ;;
      -n|--name)
      if [ -n "$2" ]; then
        NAME="$2"
      else
        echo "-n or --name requires a value."
        exit 1
      fi
      shift 2
      continue
      ;;
      -h|--help)
      usage
      exit 0
      ;;
      --)              # End of all options.
        shift
        break
      ;;
      '')              # End of all options.
        break
      ;;
      *)
        echo "Unrecognized option: $1"
        exit 1
      ;;
    esac
  done
}

function error {
  echo "Error: Resource $1 not defined!"
  usage
  exit 1
}

parse_arguments "$@"

if [ -z ${KIND} ]; then
  error "kind"
fi
if [ -z ${NAME} ]; then
  error "name"
fi

echo "Deleting ${KIND} ${NAME}..."

curl -ik \
  -X DELETE \
  -H "Authorization: Bearer $(cat /var/run/secrets/kubernetes.io/serviceaccount/token)" \
  -H 'Accept: application/json' \
  -H 'Content-Type: application/json' \
https://kubernetes.default.svc/api/v1/${KIND}/${NAME}

printf "${KIND} ${NAME} deleted.\n"
