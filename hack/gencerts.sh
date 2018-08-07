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

NAMESPACE=$1
if [ -z $NAMESPACE ]; then
	NAMESPACE="sparkoperator"
fi

CN_BASE="spark-webhook"
TMP_DIR="/tmp/spark-pod-webhook-certs"

echo "Generating certs for the Spark pod admission webhook in ${TMP_DIR}."
mkdir -p ${TMP_DIR}
cat > ${TMP_DIR}/server.conf << EOF
[req]
req_extensions = v3_req
distinguished_name = req_distinguished_name
[req_distinguished_name]
[ v3_req ]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth, serverAuth
EOF

# Create a certificate authority.
openssl genrsa -out ${TMP_DIR}/ca-key.pem 2048
openssl req -x509 -new -nodes -key ${TMP_DIR}/ca-key.pem -days 100000 -out ${TMP_DIR}/ca-cert.pem -subj "/CN=${CN_BASE}_ca"

# Create a server certificate.
openssl genrsa -out ${TMP_DIR}/server-key.pem 2048
# Note the CN is the DNS name of the service of the webhook.
openssl req -new -key ${TMP_DIR}/server-key.pem -out ${TMP_DIR}/server.csr -subj "/CN=spark-webhook.sparkoperator.svc" -config ${TMP_DIR}/server.conf
openssl x509 -req -in ${TMP_DIR}/server.csr -CA ${TMP_DIR}/ca-cert.pem -CAkey ${TMP_DIR}/ca-key.pem -CAcreateserial -out ${TMP_DIR}/server-cert.pem -days 100000 -extensions v3_req -extfile ${TMP_DIR}/server.conf

# Base64 encode secrets and then remove the trailing newline to avoid issues in the curl command
ca_cert=$(cat ${TMP_DIR}/ca-cert.pem | base64 | tr -d '\n')
ca_key=$(cat ${TMP_DIR}/ca-key.pem | base64 | tr -d '\n')
server_cert=$(cat ${TMP_DIR}/server-cert.pem | base64 | tr -d '\n')
server_key=$(cat ${TMP_DIR}/server-key.pem | base64 | tr -d '\n')

# Create the secret resource
echo "Creating a secret for the certificate and keys"
curl -ik \
  -X POST \
  -H "Authorization: Bearer $(cat /var/run/secrets/kubernetes.io/serviceaccount/token)" \
  -H 'Accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "kind": "Secret",
  "apiVersion": "v1",
  "metadata": {
    "name": "spark-webhook-certs",
    "namespace": "'"$NAMESPACE"'"
  },
  "data": {
    "ca-cert.pem": "'"$ca_cert"'",
    "ca-key.pem": "'"$ca_key"'",
    "server-cert.pem": "'"$server_cert"'",
    "server-key.pem": "'"$server_key"'"
  }
}' \
https://kubernetes.default.svc/api/v1/namespaces/${NAMESPACE}/secrets

# Clean up after we're done.
printf "\nDeleting ${TMP_DIR}.\n"
rm -rf ${TMP_DIR}
