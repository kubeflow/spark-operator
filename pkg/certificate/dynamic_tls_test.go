/*
Copyright 2026 The Kubeflow authors.

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
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDynamicTLSConfig(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	cert, key := generateServingPair(t, "webhook.default.svc", nil)
	require.NoError(t, os.WriteFile(certPath, cert, 0o600))
	require.NoError(t, os.WriteFile(keyPath, key, 0o600))

	dynamicTLS, err := NewDynamicTLSConfig(context.Background(), certPath, keyPath, time.Second, time.Millisecond, &tls.Config{MinVersion: tls.VersionTLS13})
	require.NoError(t, err)

	config, err := dynamicTLS.GetConfigForClient(nil)
	require.NoError(t, err)
	assert.Equal(t, uint16(tls.VersionTLS13), config.MinVersion)
	require.Len(t, config.Certificates, 1)
	assert.Equal(t, cert, pemCertificateBytes(t, config.Certificates[0].Certificate))

	certificate, err := dynamicTLS.GetCertificate(nil)
	require.NoError(t, err)
	assert.Equal(t, config.Certificates[0].Certificate, certificate.Certificate)
	assert.False(t, dynamicTLS.NeedLeaderElection())
}

func TestDynamicTLSConfigServedLeaf(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	cert, key := generateServingPair(t, "webhook.default.svc", nil)
	require.NoError(t, os.WriteFile(certPath, cert, 0o600))
	require.NoError(t, os.WriteFile(keyPath, key, 0o600))

	dynamicTLS, err := NewDynamicTLSConfig(context.Background(), certPath, keyPath, time.Second, time.Millisecond, &tls.Config{})
	require.NoError(t, err)

	leaf, err := dynamicTLS.ServedLeaf()
	require.NoError(t, err)
	require.NotNil(t, leaf)
	assert.Equal(t, "webhook.default.svc", leaf.Subject.CommonName)

	// ServedLeaf returns the parsed currently-served DER, and *DynamicTLSConfig
	// satisfies the interlock accessor the filesystem CA source depends on.
	served, err := dynamicTLS.GetCertificate(nil)
	require.NoError(t, err)
	require.NotEmpty(t, served.Certificate)
	assert.Equal(t, served.Certificate[0], leaf.Raw)

	var _ servedLeafAccessor = dynamicTLS
}

func TestDynamicTLSConfigReloadsAndRetainsLastKnownGood(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	certA, keyA := generateServingPair(t, "webhook.default.svc", nil)
	certB, keyB := generateServingPair(t, "webhook.default.svc", nil)
	require.NoError(t, os.WriteFile(certPath, certA, 0o600))
	require.NoError(t, os.WriteFile(keyPath, keyA, 0o600))

	dynamicTLS, err := NewDynamicTLSConfig(context.Background(), certPath, keyPath, time.Second, time.Millisecond, &tls.Config{})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(certPath, certB, 0o600))
	require.NoError(t, os.WriteFile(keyPath, keyB, 0o600))
	require.NoError(t, dynamicTLS.reload(context.Background()))

	certificate, err := dynamicTLS.GetCertificate(nil)
	require.NoError(t, err)
	assert.Equal(t, certB, pemCertificateBytes(t, certificate.Certificate))

	require.NoError(t, os.WriteFile(certPath, []byte("malformed"), 0o600))
	require.Error(t, dynamicTLS.reload(context.Background()))
	certificate, err = dynamicTLS.GetCertificate(nil)
	require.NoError(t, err)
	assert.Equal(t, certB, pemCertificateBytes(t, certificate.Certificate))
}

func TestDynamicTLSConfigReloadsCertificateChainOnlyChange(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	leaf, key := generateServingPair(t, "webhook.default.svc", nil)
	intermediate, _ := generateServingPair(t, "intermediate", nil)
	require.NoError(t, os.WriteFile(certPath, leaf, 0o600))
	require.NoError(t, os.WriteFile(keyPath, key, 0o600))

	dynamicTLS, err := NewDynamicTLSConfig(context.Background(), certPath, keyPath, time.Second, time.Millisecond, &tls.Config{})
	require.NoError(t, err)
	chain := append(bytes.Clone(leaf), intermediate...)
	require.NoError(t, os.WriteFile(certPath, chain, 0o600))
	require.NoError(t, dynamicTLS.reload(context.Background()))

	certificate, err := dynamicTLS.GetCertificate(nil)
	require.NoError(t, err)
	assert.Equal(t, chain, pemCertificateBytes(t, certificate.Certificate))
}

func TestDynamicTLSConfigRetainsLastKnownGoodForInvalidReplacements(t *testing.T) {
	cert, key := generateServingPair(t, "webhook.default.svc", nil)
	_, mismatchedKey := generateServingPair(t, "webhook.default.svc", nil)

	tests := []struct {
		name    string
		cert    []byte
		key     []byte
		omitKey bool
	}{
		{name: "empty certificate", cert: []byte{}, key: key},
		{name: "malformed certificate", cert: []byte("malformed"), key: key},
		{name: "mismatched key", cert: cert, key: mismatchedKey},
		{name: "unreadable key path", cert: cert, omitKey: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			certPath := filepath.Join(dir, "tls.crt")
			keyPath := filepath.Join(dir, "tls.key")
			require.NoError(t, os.WriteFile(certPath, cert, 0o600))
			require.NoError(t, os.WriteFile(keyPath, key, 0o600))
			dynamicTLS, err := NewDynamicTLSConfig(context.Background(), certPath, keyPath, time.Second, time.Millisecond, &tls.Config{})
			require.NoError(t, err)

			require.NoError(t, os.WriteFile(certPath, tt.cert, 0o600))
			if tt.omitKey {
				require.NoError(t, os.Remove(keyPath))
			} else {
				require.NoError(t, os.WriteFile(keyPath, tt.key, 0o600))
			}
			require.Error(t, dynamicTLS.reload(context.Background()))
			certificate, err := dynamicTLS.GetCertificate(nil)
			require.NoError(t, err)
			assert.Equal(t, cert, pemCertificateBytes(t, certificate.Certificate))
		})
	}
}

func TestDynamicTLSConfigWatchesValidReplacement(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	certA, keyA := generateServingPair(t, "webhook.default.svc", nil)
	certB, keyB := generateServingPair(t, "webhook.default.svc", nil)
	require.NoError(t, os.WriteFile(certPath, certA, 0o600))
	require.NoError(t, os.WriteFile(keyPath, keyA, 0o600))

	dynamicTLS, err := NewDynamicTLSConfig(context.Background(), certPath, keyPath, time.Second, time.Millisecond, &tls.Config{})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- dynamicTLS.Start(ctx) }()
	defer cancel()

	require.Eventually(t, func() bool {
		certificate, err := dynamicTLS.GetCertificate(nil)
		return err == nil && bytes.Equal(certA, pemCertificateBytes(t, certificate.Certificate))
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, os.WriteFile(certPath, certB, 0o600))
	require.NoError(t, os.WriteFile(keyPath, keyB, 0o600))
	require.Eventually(t, func() bool {
		certificate, err := dynamicTLS.GetCertificate(nil)
		return err == nil && bytes.Equal(certB, pemCertificateBytes(t, certificate.Certificate))
	}, 5*time.Second, 10*time.Millisecond)
	cancel()
	require.NoError(t, <-done)
}

func TestDynamicTLSConfigUnchangedReloadIsNoOp(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	cert, key := generateServingPair(t, "webhook.default.svc", nil)
	require.NoError(t, os.WriteFile(certPath, cert, 0o600))
	require.NoError(t, os.WriteFile(keyPath, key, 0o600))

	dynamicTLS, err := NewDynamicTLSConfig(context.Background(), certPath, keyPath, time.Second, time.Millisecond, &tls.Config{})
	require.NoError(t, err)
	require.NoError(t, dynamicTLS.reload(context.Background()))
	certificate, err := dynamicTLS.GetCertificate(nil)
	require.NoError(t, err)
	assert.Equal(t, cert, pemCertificateBytes(t, certificate.Certificate))
}

func TestDynamicTLSConfigIterationOneDoesNotRejectSemanticInvalidity(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name     string
		template *x509.Certificate
	}{
		{
			name: "expired",
			template: &x509.Certificate{
				SerialNumber: big.NewInt(1),
				DNSNames:     []string{"webhook.default.svc"},
				NotBefore:    now.Add(-2 * time.Hour),
				NotAfter:     now.Add(-time.Hour),
				ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			},
		},
		{
			name: "wrong SAN",
			template: &x509.Certificate{
				SerialNumber: big.NewInt(2),
				DNSNames:     []string{"other.default.svc"},
				NotBefore:    now.Add(-time.Minute),
				NotAfter:     now.Add(time.Hour),
				ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			},
		},
		{
			name: "wrong EKU",
			template: &x509.Certificate{
				SerialNumber: big.NewInt(3),
				DNSNames:     []string{"webhook.default.svc"},
				NotBefore:    now.Add(-time.Minute),
				NotAfter:     now.Add(time.Hour),
				ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			},
		},
		{
			name: "untrusted self-signed issuer",
			template: &x509.Certificate{
				SerialNumber: big.NewInt(4),
				DNSNames:     []string{"webhook.default.svc"},
				NotBefore:    now.Add(-time.Minute),
				NotAfter:     now.Add(time.Hour),
				ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			certPath := filepath.Join(dir, "tls.crt")
			keyPath := filepath.Join(dir, "tls.key")
			cert, key := generateServingPairFromTemplate(t, tt.template, nil)
			require.NoError(t, os.WriteFile(certPath, cert, 0o600))
			require.NoError(t, os.WriteFile(keyPath, key, 0o600))

			dynamicTLS, err := NewDynamicTLSConfig(context.Background(), certPath, keyPath, time.Second, time.Millisecond, &tls.Config{})
			require.NoError(t, err)
			certificate, err := dynamicTLS.GetCertificate(nil)
			require.NoError(t, err)
			assert.Equal(t, cert, pemCertificateBytes(t, certificate.Certificate))
		})
	}
}

func TestDynamicTLSConfigStopsOnCancellation(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	cert, key := generateServingPair(t, "webhook.default.svc", nil)
	require.NoError(t, os.WriteFile(certPath, cert, 0o600))
	require.NoError(t, os.WriteFile(keyPath, key, 0o600))

	dynamicTLS, err := NewDynamicTLSConfig(context.Background(), certPath, keyPath, time.Second, time.Millisecond, &tls.Config{})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- dynamicTLS.Start(ctx) }()
	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("dynamic TLS runnable did not stop after cancellation")
	}
}

func TestDynamicTLSConfigDelayedBootstrapAndLiveHandshakeReload(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	certA, keyA := generateServingPair(t, "localhost", nil)
	certB, keyB := generateServingPair(t, "localhost", nil)
	writeErr := make(chan error, 1)
	go func() {
		time.Sleep(30 * time.Millisecond)
		if err := os.WriteFile(certPath, certA, 0o600); err != nil {
			writeErr <- err
			return
		}
		writeErr <- os.WriteFile(keyPath, keyA, 0o600)
	}()

	dynamicTLS, err := NewDynamicTLSConfig(context.Background(), certPath, keyPath, time.Second, 5*time.Millisecond, &tls.Config{MinVersion: tls.VersionTLS12})
	require.NoError(t, err)
	require.NoError(t, <-writeErr)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- dynamicTLS.Start(ctx) }()
	defer func() {
		cancel()
		require.NoError(t, <-done)
	}()

	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		MinVersion:         tls.VersionTLS12,
		GetConfigForClient: dynamicTLS.GetConfigForClient,
		GetCertificate:     dynamicTLS.GetCertificate,
	})
	require.NoError(t, err)
	defer listener.Close()
	serverDone := make(chan error, 2)
	go serveTLSConnections(listener, serverDone, 2)

	assertHandshakeSerial(t, listener.Addr().String(), certA)
	require.NoError(t, os.WriteFile(certPath, certB, 0o600))
	require.NoError(t, os.WriteFile(keyPath, keyB, 0o600))
	require.Eventually(t, func() bool {
		certificate, err := dynamicTLS.GetCertificate(nil)
		return err == nil && bytes.Equal(certB, pemCertificateBytes(t, certificate.Certificate))
	}, 5*time.Second, 10*time.Millisecond)
	assertHandshakeSerial(t, listener.Addr().String(), certB)
	require.NoError(t, <-serverDone)
}

func serveTLSConnections(listener net.Listener, done chan<- error, count int) {
	for range count {
		connection, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		_, err = io.Copy(io.Discard, connection)
		closeErr := connection.Close()
		if err != nil {
			done <- err
			return
		}
		if closeErr != nil {
			done <- closeErr
			return
		}
	}
	done <- nil
}

func assertHandshakeSerial(t *testing.T, address string, certPEM []byte) {
	t.Helper()
	block, _ := pem.Decode(certPEM)
	require.NotNil(t, block)
	want, err := x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)

	connection, err := tls.Dial("tcp", address, &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true, // The assertion below pins the expected test certificate.
	})
	require.NoError(t, err)
	defer connection.Close()
	require.NotEmpty(t, connection.ConnectionState().PeerCertificates)
	assert.Equal(t, want.SerialNumber, connection.ConnectionState().PeerCertificates[0].SerialNumber)
}

func pemCertificateBytes(t *testing.T, certificates [][]byte) []byte {
	t.Helper()
	var result []byte
	for _, certificate := range certificates {
		result = append(result, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate})...)
	}
	return result
}
