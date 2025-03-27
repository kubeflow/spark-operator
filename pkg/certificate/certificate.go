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
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/cert"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kubeflow/spark-operator/pkg/common"
)

const (
	Organization = "spark-operator"
)

// Provider is a container of a X509 certificate file and a corresponding key file for the
// webhook server, and a CA certificate file for the API server to verify the server certificate.
type Provider struct {
	client     client.Client
	commonName string
	caKey      *rsa.PrivateKey
	caCert     *x509.Certificate
	serverKey  *rsa.PrivateKey
	serverCert *x509.Certificate
}

// NewProvider creates a new Provider instance.
func NewProvider(client client.Client, name, namespace string) *Provider {
	commonName := fmt.Sprintf("%s.%s.svc", name, namespace)
	certProvider := Provider{
		client:     client,
		commonName: commonName,
	}
	return &certProvider
}

// SyncSecret syncs the secret containing the certificates to the given name and namespace.
func (cp *Provider) SyncSecret(ctx context.Context, name, namespace string) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
	key := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}
	if err := cp.client.Get(ctx, key, secret); err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		if err := cp.client.Create(ctx, secret); err != nil {
			if errors.IsAlreadyExists(err) {
				return err
			}
			return fmt.Errorf("failed to create secret: %v", err)
		}
	}

	if len(secret.Data[common.CAKeyPem]) == 0 ||
		len(secret.Data[common.CACertPem]) == 0 ||
		len(secret.Data[common.ServerCertPem]) == 0 ||
		len(secret.Data[common.ServerKeyPem]) == 0 {
		return cp.generateAndUpdateSecret(ctx, secret)
	}

	if err := cp.parseSecret(secret); err != nil {
		return err
	}

	pool := x509.NewCertPool()
	pool.AddCert(cp.caCert)
	if _, err := cp.serverCert.Verify(x509.VerifyOptions{
		DNSName:     cp.commonName,
		Roots:       pool,
		CurrentTime: time.Now().AddDate(0, 0, 180),
	}); err != nil {
		return cp.generateAndUpdateSecret(ctx, secret)
	}

	return nil
}

// CAKey returns the PEM-encoded CA private key.
func (cp *Provider) CAKey() ([]byte, error) {
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
func (cp *Provider) CACert() ([]byte, error) {
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
func (cp *Provider) ServerKey() ([]byte, error) {
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
func (cp *Provider) ServerCert() ([]byte, error) {
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
func (cp *Provider) TLSConfig() (*tls.Config, error) {
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
func (cp *Provider) WriteFile(path, certName, keyName string) error {
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

func (cp *Provider) Generate() error {
	// Generate CA private caKey
	caKey, err := NewPrivateKey()
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
	serverKey, err := NewPrivateKey()
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
	serverCert, err := NewSignedServerCert(serverCfg, caKey, caCert, serverKey)
	if err != nil {
		return fmt.Errorf("failed to generate signed server certificate: %v", err)
	}

	cp.caKey = caKey
	cp.caCert = caCert
	cp.serverKey = serverKey
	cp.serverCert = serverCert
	return nil
}

func (cp *Provider) parseSecret(secret *corev1.Secret) error {
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

func (cp *Provider) updateSecret(ctx context.Context, secret *corev1.Secret) error {
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
	if err := cp.client.Update(ctx, secret); err != nil {
		return err
	}
	return nil
}

func (cp *Provider) generateAndUpdateSecret(ctx context.Context, secret *corev1.Secret) error {
	if err := cp.Generate(); err != nil {
		return fmt.Errorf("failed to generate certificate: %v", err)
	}
	if err := cp.updateSecret(ctx, secret); err != nil {
		return err
	}
	return nil
}
