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
package cert

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"os"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/cert"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
)

const (
	Organization = "spark-operator"
)

// CertProvider is a container of a X509 certificate file and a corresponding key file for the
// webhook server, and a CA certificate file for the API server to verify the server certificate.
type CertProvider struct {
	client     client.Client
	commonName string
	caKey      *rsa.PrivateKey
	caCert     *x509.Certificate
	serverKey  *rsa.PrivateKey
	serverCert *x509.Certificate
}

// NewCertProvider creates a new CertProvider instance.
func NewCertProvider(client client.Client, name, namespace string) *CertProvider {
	commonName := fmt.Sprintf("%s.%s.svc", name, namespace)
	certProvider := CertProvider{
		client:     client,
		commonName: commonName,
	}
	return &certProvider
}

// SyncSecret syncs the secret containing the certificates to the given name and namespace.
func (cp *CertProvider) SyncSecret(name, namespace string) error {
	secret := &corev1.Secret{}
	key := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}
	if err := cp.client.Get(context.TODO(), key, secret); err != nil {
		return fmt.Errorf("failed to get secret: %v", err)
	}

	if len(secret.Data[common.CAKeyPem]) == 0 ||
		len(secret.Data[common.CACertPem]) == 0 ||
		len(secret.Data[common.ServerCertPem]) == 0 ||
		len(secret.Data[common.ServerKeyPem]) == 0 {
		if err := cp.Generate(); err != nil {
			return fmt.Errorf("failed to generate certificate: %v", err)
		}
		if err := cp.updateSecret(secret); err != nil {
			return err
		}
		return nil
	}
	return cp.parseSecret(secret)
}

// CAKey returns the PEM-encoded CA private key.
func (cp *CertProvider) CAKey() ([]byte, error) {
	if cp.caKey == nil {
		return nil, fmt.Errorf("CA key is not set")
	}
	data := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(cp.caKey),
	})
	return data, nil
}

// CACert returns the PEM-encoded CA certificate.
func (cp *CertProvider) CACert() ([]byte, error) {
	if cp.caCert == nil {
		return nil, fmt.Errorf("CA certificate is not set")
	}
	data := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cp.caCert.Raw,
	})
	return data, nil
}

// ServerKey returns the PEM-encoded server private key.
func (cp *CertProvider) ServerKey() ([]byte, error) {
	if cp.serverKey == nil {
		return nil, fmt.Errorf("server key is not set")
	}
	data := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(cp.serverKey),
	})
	return data, nil
}

// ServerCert returns the PEM-encoded server cert.
func (cp *CertProvider) ServerCert() ([]byte, error) {
	if cp.serverCert == nil {
		return nil, fmt.Errorf("server cert is not set")
	}
	data := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cp.serverCert.Raw,
	})
	return data, nil
}

// TLSConfig returns the TLS configuration.
func (cp *CertProvider) TLSConfig() (*tls.Config, error) {
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

// WriteFile saves the certificate and key to the given path.
func (cp *CertProvider) WriteFile(path, certName, keyName string) error {
	if err := os.MkdirAll(path, 0755); err != nil {
		return err
	}
	serverCert, err := cp.ServerCert()
	if err != nil {
		return err
	}
	serverKey, err := cp.ServerKey()
	if err != nil {
		return err
	}
	if err := os.WriteFile(path+"/"+certName, serverCert, 0600); err != nil {
		return err
	}
	if err := os.WriteFile(path+"/"+keyName, serverKey, 0600); err != nil {
		return err
	}
	return nil
}

func (cp *CertProvider) Generate() error {
	// Generate CA private caKey
	caKey, err := util.NewPrivateKey()
	if err != nil {
		return fmt.Errorf("failed to generate CA private key: %v", err)
	}

	// Generate self-signed CA certificate
	caCfg := cert.Config{
		CommonName:   cp.commonName,
		Organization: []string{Organization},
	}
	caCert, err := cert.NewSelfSignedCACert(caCfg, caKey)
	if err != nil {
		return fmt.Errorf("failed to generate self-signed CA certificate: %v", err)
	}

	// Generate server private key
	serverKey, err := util.NewPrivateKey()
	if err != nil {
		return fmt.Errorf("failed to generate server private key: %v", err)
	}

	// Generate signed server certificate
	var ips []net.IP
	dnsNames := []string{"localhost"}
	hostIP := net.ParseIP(cp.commonName)
	if hostIP.To4() != nil {
		ips = append(ips, hostIP.To4())
	} else {
		dnsNames = append(dnsNames, cp.commonName)
	}
	serverCfg := cert.Config{
		CommonName:   cp.commonName,
		Organization: []string{Organization},
		AltNames:     cert.AltNames{IPs: ips, DNSNames: dnsNames},
	}
	serverCert, err := util.NewSignedServerCert(serverCfg, caKey, caCert, serverKey)
	if err != nil {
		return fmt.Errorf("failed to generate signed server certificate: %v", err)
	}

	cp.caKey = caKey
	cp.caCert = caCert
	cp.serverKey = serverKey
	cp.serverCert = serverCert
	return nil
}

func (cp *CertProvider) parseSecret(secret *corev1.Secret) error {
	if secret == nil {
		return fmt.Errorf("secret is nil")
	}
	caKeyPem, _ := pem.Decode(secret.Data[common.CAKeyPem])
	caCertPem, _ := pem.Decode(secret.Data[common.CACertPem])
	serverKeyPem, _ := pem.Decode(secret.Data[common.ServerKeyPem])
	serverCertPem, _ := pem.Decode(secret.Data[common.ServerCertPem])
	if caKeyPem == nil || caCertPem == nil || serverKeyPem == nil || serverCertPem == nil {
		return fmt.Errorf("failed to decode secret data to pem block")
	}
	caKey, err := x509.ParsePKCS1PrivateKey(caKeyPem.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse CA private key: %v", err)
	}
	caCert, err := x509.ParseCertificate(caCertPem.Bytes)
	if err != nil {
		return fmt.Errorf("failed to prase CA certificate: %v", err)
	}
	serverKey, err := x509.ParsePKCS1PrivateKey(serverKeyPem.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse server private key: %v", err)
	}
	serverCert, err := x509.ParseCertificate(serverCertPem.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse server certificate: %v", err)
	}
	cp.caKey = caKey
	cp.caCert = caCert
	cp.serverKey = serverKey
	cp.serverCert = serverCert
	return nil
}

func (cp *CertProvider) updateSecret(secret *corev1.Secret) error {
	caKey, err := cp.CAKey()
	if err != nil {
		return fmt.Errorf("failed to get CA key: %v", err)
	}
	caCert, err := cp.CACert()
	if err != nil {
		return fmt.Errorf("failed to get CA certificate: %v", err)
	}
	serverKey, err := cp.ServerKey()
	if err != nil {
		return fmt.Errorf("failed to get server key: %v", err)
	}
	serverCert, err := cp.ServerCert()
	if err != nil {
		return fmt.Errorf("failed to get server certificate: %v", err)
	}
	if secret.Data == nil {
		secret.Data = make(map[string][]byte)
	}
	secret.Data[common.CAKeyPem] = caKey
	secret.Data[common.CACertPem] = caCert
	secret.Data[common.ServerKeyPem] = serverKey
	secret.Data[common.ServerCertPem] = serverCert
	if err := cp.client.Update(context.TODO(), secret); err != nil {
		return err
	}
	return nil
}
