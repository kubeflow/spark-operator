/*
Copyright 2024 The Kubeflow authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package certificate_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"testing"
	"time"

	"k8s.io/client-go/util/cert"

	"github.com/kubeflow/spark-operator/pkg/certificate"
	"github.com/kubeflow/spark-operator/pkg/common"
)

func TestNewPrivateKey(t *testing.T) {
	_, err := certificate.NewPrivateKey()
	if err != nil {
		t.Errorf("failed to generate private key: %v", err)
	}
}

func TestNewSignedServerCert(t *testing.T) {
	cfg := cert.Config{
		CommonName:   "test-server",
		Organization: []string{"test-org"},
		NotBefore:    time.Now(),
	}

	caKey, _ := rsa.GenerateKey(rand.Reader, common.RSAKeySize)
	caCert := &x509.Certificate{}
	serverKey, _ := rsa.GenerateKey(rand.Reader, common.RSAKeySize)

	serverCert, err := certificate.NewSignedServerCert(cfg, caKey, caCert, serverKey)
	if err != nil {
		t.Errorf("failed to generate signed server certificate: %v", err)
	}

	if serverCert == nil {
		t.Error("server certificate is nil")
	}
}
