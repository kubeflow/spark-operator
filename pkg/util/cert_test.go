package util

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"testing"
	"time"

	"k8s.io/client-go/util/cert"
)

func TestNewPrivateKey(t *testing.T) {
	_, err := NewPrivateKey()
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

	caKey, _ := rsa.GenerateKey(rand.Reader, RSAKeySize)
	caCert := &x509.Certificate{}
	serverKey, _ := rsa.GenerateKey(rand.Reader, RSAKeySize)

	serverCert, err := NewSignedServerCert(cfg, caKey, caCert, serverKey)
	if err != nil {
		t.Errorf("failed to generate signed server certificate: %v", err)
	}

	if serverCert == nil {
		t.Error("server certificate is nil")
	}
}
