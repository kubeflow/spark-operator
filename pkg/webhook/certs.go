/*
Copyright 2018 Google LLC

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

package webhook

import (
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net"

	"k8s.io/client-go/util/cert"

	"github.com/kubeflow/spark-operator/pkg/util"
)

const (
	Organization = "spark-operator"
)

// certProvider is a container of a X509 certificate file and a corresponding key file for the
// webhook server, and a CA certificate file for the API server to verify the server certificate.
type certProvider struct {
	caKey      *rsa.PrivateKey
	caCert     *x509.Certificate
	serverKey  *rsa.PrivateKey
	serverCert *x509.Certificate
}

// NewCertProvider creates a new CertProvider instance.
func NewCertProvider(name, namespace string) (*certProvider, error) {
	commonName := fmt.Sprintf("%s.%s.svc", name, namespace)

	// Generate CA private caKey
	caKey, err := util.NewPrivateKey()
	if err != nil {
		return nil, fmt.Errorf("failed to generate CA private key: %v", err)
	}

	// Generate self-signed CA certificate
	caCfg := cert.Config{
		CommonName:   commonName,
		Organization: []string{Organization},
	}
	caCert, err := cert.NewSelfSignedCACert(caCfg, caKey)
	if err != nil {
		return nil, fmt.Errorf("failed to generate self-signed CA certificate: %v", err)
	}

	// Generate server private key
	serverKey, err := util.NewPrivateKey()
	if err != nil {
		return nil, fmt.Errorf("failed to generate server private key: %v", err)
	}

	// Generate signed server certificate
	var ips []net.IP
	dnsNames := []string{"localhost"}
	hostIP := net.ParseIP(commonName)
	if hostIP.To4() != nil {
		ips = append(ips, hostIP.To4())
	} else {
		dnsNames = append(dnsNames, commonName)
	}
	serverCfg := cert.Config{
		CommonName:   commonName,
		Organization: []string{Organization},
		AltNames:     cert.AltNames{IPs: ips, DNSNames: dnsNames},
	}
	serverCert, err := util.NewSignedServerCert(serverCfg, caKey, caCert, serverKey)
	if err != nil {
		return nil, fmt.Errorf("failed to generate signed server certificate: %v", err)
	}

	certProvider := certProvider{
		caKey:      caKey,
		caCert:     caCert,
		serverKey:  serverKey,
		serverCert: serverCert,
	}

	return &certProvider, nil
}

// CAKey returns the PEM-encoded CA private key.
func (cp *certProvider) CAKey() ([]byte, error) {
	if cp.caKey == nil {
		return nil, errors.New("CA key is not set")
	}
	data := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(cp.caKey),
	})
	return data, nil
}

// CACert returns the PEM-encoded CA certificate.
func (cp *certProvider) CACert() ([]byte, error) {
	if cp.caCert == nil {
		return nil, errors.New("CA certificate is not set")
	}
	data := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cp.serverCert.Raw,
	})
	return data, nil
}

// ServerKey returns the PEM-encoded server private key.
func (cp *certProvider) ServerKey() ([]byte, error) {
	if cp.serverKey == nil {
		return nil, errors.New("server key is not set")
	}
	data := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(cp.serverKey),
	})
	return data, nil
}

// ServerCert returns the PEM-encoded server cert.
func (cp *certProvider) ServerCert() ([]byte, error) {
	if cp.serverCert == nil {
		return nil, errors.New("server cert is not set")
	}
	data := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cp.serverCert.Raw,
	})
	return data, nil
}

// TLSConfig returns the TLS configuration.
func (cp *certProvider) TLSConfig() (*tls.Config, error) {
	keyPEMBlock, err := cp.ServerKey()
	if err != nil {
		return nil, fmt.Errorf("failed to get server key: %v", err)
	}

	certPEMBlock, err := cp.ServerCert()
	if err != nil {
		return nil, fmt.Errorf("failed to get server certificate: %v", err)
	}

	tlsCert, err := tls.X509KeyPair(certPEMBlock, keyPEMBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to generate TLS certificate: %v", err)
	}

	cfg := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
	}
	return cfg, nil
}
