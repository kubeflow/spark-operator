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
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
)

func TestAcquireDynamicServingContent(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	wantCert, wantKey := generateServingPair(t, "webhook.default.svc", nil)

	writeErr := make(chan error, 1)
	go func() {
		time.Sleep(30 * time.Millisecond)
		if err := os.WriteFile(certPath, wantCert, 0o600); err != nil {
			writeErr <- err
			return
		}
		writeErr <- os.WriteFile(keyPath, wantKey, 0o600)
	}()

	provider, err := acquireDynamicServingContent(context.Background(), certPath, keyPath, time.Second, 5*time.Millisecond)
	require.NoError(t, err)
	require.NoError(t, <-writeErr)
	gotCert, gotKey := provider.CurrentCertKeyContent()
	assert.Equal(t, wantCert, gotCert)
	assert.Equal(t, wantKey, gotKey)
}

func TestAcquireDynamicServingContentTimesOutWithoutExposingInput(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	require.NoError(t, os.WriteFile(certPath, []byte("private-certificate-input"), 0o600))
	require.NoError(t, os.WriteFile(keyPath, []byte("private-key-input"), 0o600))

	_, err := acquireDynamicServingContent(context.Background(), certPath, keyPath, 30*time.Millisecond, 5*time.Millisecond)
	require.Error(t, err)
	assert.ErrorContains(t, err, certPath)
	assert.ErrorContains(t, err, keyPath)
	assert.NotContains(t, err.Error(), "private-certificate-input")
	assert.NotContains(t, err.Error(), "private-key-input")
}

func TestAcquireDynamicServingContentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()

	_, err := acquireDynamicServingContent(ctx, "/missing/tls.crt", "/missing/tls.key", time.Minute, time.Second)
	require.ErrorIs(t, err, context.Canceled)
	assert.Less(t, time.Since(started), 100*time.Millisecond)
}

func TestAcquireDynamicServingContentRetriesPartialGenerations(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	cert, key := generateServingPair(t, "webhook.default.svc", nil)
	_, mismatchedKey := generateServingPair(t, "webhook.default.svc", nil)
	require.NoError(t, os.WriteFile(certPath, cert, 0o600))

	writeErr := make(chan error, 1)
	go func() {
		if err := os.WriteFile(keyPath, []byte("partial"), 0o600); err != nil {
			writeErr <- err
			return
		}
		time.Sleep(20 * time.Millisecond)
		if err := os.WriteFile(keyPath, mismatchedKey, 0o600); err != nil {
			writeErr <- err
			return
		}
		time.Sleep(20 * time.Millisecond)
		writeErr <- os.WriteFile(keyPath, key, 0o600)
	}()

	provider, err := acquireDynamicServingContent(context.Background(), certPath, keyPath, time.Second, 5*time.Millisecond)
	require.NoError(t, err)
	require.NoError(t, <-writeErr)
	gotCert, gotKey := provider.CurrentCertKeyContent()
	assert.Equal(t, cert, gotCert)
	assert.Equal(t, key, gotKey)
}

func TestDynamicServingContentRetainsAcceptedGeneration(t *testing.T) {
	dir := t.TempDir()
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	cert, key := generateServingPair(t, "webhook.default.svc", nil)
	require.NoError(t, os.WriteFile(certPath, cert, 0o600))
	require.NoError(t, os.WriteFile(keyPath, key, 0o600))

	provider, err := dynamiccertificates.NewDynamicServingContentFromFiles("test", certPath, keyPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(certPath, []byte("malformed"), 0o600))
	require.Error(t, provider.RunOnce(context.Background()))
	gotCert, gotKey := provider.CurrentCertKeyContent()
	assert.Equal(t, cert, gotCert)
	assert.Equal(t, key, gotKey)
}

func generateServingPair(t *testing.T, dnsName string, signer *rsa.PrivateKey) ([]byte, []byte) {
	t.Helper()
	now := time.Now()
	return generateServingPairFromTemplate(t, &x509.Certificate{
		SerialNumber: big.NewInt(now.UnixNano()),
		Subject:      pkix.Name{CommonName: dnsName},
		DNSNames:     []string{dnsName},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}, signer)
}

func generateServingPairFromTemplate(t *testing.T, template *x509.Certificate, signer *rsa.PrivateKey) ([]byte, []byte) {
	t.Helper()
	key := signer
	if key == nil {
		var err error
		key, err = rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	_, err = tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)
	return certPEM, keyPEM
}
