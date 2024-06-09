package util

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
)

const (
	RSAKeySize = 2048
)

func NewPrivateKey() (*rsa.PrivateKey, error) {
	key, err := rsa.GenerateKey(rand.Reader, RSAKeySize)
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
