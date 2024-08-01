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

package certificate

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math"
	"math/big"
	"time"

	"k8s.io/client-go/util/cert"

	"github.com/kubeflow/spark-operator/pkg/common"
)

func NewPrivateKey() (*rsa.PrivateKey, error) {
	key, err := rsa.GenerateKey(rand.Reader, common.RSAKeySize)
	if err != nil {
		return nil, fmt.Errorf("failed to generate private key: %v", err)
	}
	return key, nil
}

func NewSignedServerCert(cfg cert.Config, caKey *rsa.PrivateKey, caCert *x509.Certificate, serverKey *rsa.PrivateKey) (*x509.Certificate, error) {
	// Generate a random serial number in [1, max).
	serial, err := rand.Int(rand.Reader, new(big.Int).SetInt64(math.MaxInt64-1))
	if err != nil {
		return nil, fmt.Errorf("failed to generate serial number: %v", err)
	}
	serial.Add(serial, big.NewInt(1))

	now := time.Now()
	notBefore := now.UTC()
	if !cfg.NotBefore.IsZero() {
		notBefore = cfg.NotBefore.UTC()
	}

	// Create a certificate template for webhook server
	certTmpl := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   cfg.CommonName,
			Organization: cfg.Organization,
		},
		DNSNames:              cfg.AltNames.DNSNames,
		IPAddresses:           cfg.AltNames.IPs,
		NotBefore:             notBefore,
		NotAfter:              now.AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageContentCommitment | x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, &certTmpl, caCert, serverKey.Public(), caKey)
	if err != nil {
		return nil, fmt.Errorf("failed to generate certificate: %v", err)
	}

	serverCert, err := x509.ParseCertificate(certBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse certificate: %v", err)
	}

	return serverCert, nil
}
