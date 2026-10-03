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
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
)

type fakeServedLeaf struct {
	leaf *x509.Certificate
	err  error
}

func (f fakeServedLeaf) ServedLeaf() (*x509.Certificate, error) {
	return f.leaf, f.err
}

func newTestCA(t *testing.T, cn string) (*x509.Certificate, *rsa.PrivateKey, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}
	return cert, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func newTestLeaf(t *testing.T, cn string, ca *x509.Certificate, caKey *rsa.PrivateKey) *x509.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano() + 1),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{cn},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create leaf certificate: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse leaf certificate: %v", err)
	}
	return leaf
}

func newFileSource(t *testing.T, path string, leaf servedLeafAccessor) *FilesystemCABundleSource {
	t.Helper()
	return &FilesystemCABundleSource{
		path:         path,
		syncInterval: time.Second,
		servedLeaf:   leaf,
		logger:       logr.Discard(),
	}
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %q: %v", path, err)
	}
}

func TestCanonicalizeCABundleOrderDedupeAndReject(t *testing.T) {
	_, _, caA := newTestCA(t, "ca-a")
	_, _, caB := newTestCA(t, "ca-b")

	// Re-encode caA with an extra PEM header to prove dedupe is by DER, not bytes.
	blockA, _ := pem.Decode(caA)
	caAWithHeader := pem.EncodeToMemory(&pem.Block{
		Type:    "CERTIFICATE",
		Headers: map[string]string{"X-Note": "dup"},
		Bytes:   blockA.Bytes,
	})

	input := bytes.Join([][]byte{
		[]byte("# leading comment ignored by pem.Decode\n"),
		caB,
		caA,
		caAWithHeader, // exact-duplicate DER, different PEM framing
	}, nil)

	got, err := canonicalizeCABundle(input)
	if err != nil {
		t.Fatalf("canonicalize: %v", err)
	}

	// Expect caB then caA, each once, re-encoded without headers, in input order.
	want := append(append([]byte{}, caB...), caA...)
	if !bytes.Equal(got, want) {
		t.Fatalf("canonical bundle mismatch:\n got=%q\nwant=%q", got, want)
	}

	// Canonicalization is idempotent.
	again, err := canonicalizeCABundle(got)
	if err != nil {
		t.Fatalf("re-canonicalize: %v", err)
	}
	if !bytes.Equal(again, got) {
		t.Fatal("canonicalize is not idempotent")
	}

	// No CERTIFICATE blocks is an error.
	if _, err := canonicalizeCABundle([]byte("not a certificate")); err == nil {
		t.Fatal("expected error for input with no CERTIFICATE blocks")
	}

	// An unparseable CERTIFICATE block is an error.
	garbage := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("not-der")})
	if _, err := canonicalizeCABundle(garbage); err == nil {
		t.Fatal("expected error for unparseable CERTIFICATE block")
	}
}

func TestFilesystemCASourceInterlockAcceptAndReject(t *testing.T) {
	caCert, caKey, caPEM := newTestCA(t, "trusted-ca")
	leaf := newTestLeaf(t, "webhook.svc", caCert, caKey)
	_, _, otherPEM := newTestCA(t, "unrelated-ca")

	dir := t.TempDir()
	path := filepath.Join(dir, "ca.crt")

	// Accept: bundle anchors the served leaf; extra unrelated root is harmless.
	writeFile(t, path, append(append([]byte{}, caPEM...), otherPEM...))
	source := newFileSource(t, path, fakeServedLeaf{leaf: leaf})
	changed, err := source.refresh()
	if err != nil {
		t.Fatalf("expected anchoring bundle to commit: %v", err)
	}
	if !changed {
		t.Fatal("expected first commit to report a change")
	}
	got, err := source.CACert()
	if err != nil {
		t.Fatalf("CACert after commit: %v", err)
	}
	wantCanonical, err := canonicalizeCABundle(append(append([]byte{}, caPEM...), otherPEM...))
	if err != nil {
		t.Fatalf("canonicalize expected: %v", err)
	}
	if !bytes.Equal(got, wantCanonical) {
		t.Fatal("CACert did not return the committed canonical bundle")
	}

	// Reject: a bundle that does not anchor the served leaf retains LKG.
	rejectSource := newFileSource(t, path, fakeServedLeaf{leaf: leaf})
	if _, err := rejectSource.refresh(); err != nil {
		t.Fatalf("prime last-known-good: %v", err)
	}
	writeFile(t, path, otherPEM)
	if _, err := rejectSource.refresh(); err == nil {
		t.Fatal("expected interlock to reject a non-anchoring bundle")
	}
	retained, err := rejectSource.CACert()
	if err != nil {
		t.Fatalf("CACert after reject: %v", err)
	}
	if !bytes.Equal(retained, wantCanonical) {
		t.Fatal("interlock rejection did not retain the last-known-good bundle")
	}
}

func TestFilesystemCASourceRetainsLastKnownGoodOnBadInput(t *testing.T) {
	caCert, caKey, caPEM := newTestCA(t, "trusted-ca")
	leaf := newTestLeaf(t, "webhook.svc", caCert, caKey)

	dir := t.TempDir()
	path := filepath.Join(dir, "ca.crt")
	writeFile(t, path, caPEM)
	source := newFileSource(t, path, fakeServedLeaf{leaf: leaf})
	if _, err := source.refresh(); err != nil {
		t.Fatalf("initial commit: %v", err)
	}
	good, err := source.CACert()
	if err != nil {
		t.Fatalf("CACert: %v", err)
	}

	// Unparseable content: retain LKG.
	writeFile(t, path, []byte("garbage not pem"))
	if _, err := source.refresh(); err == nil {
		t.Fatal("expected error on unparseable file")
	}
	if cur, _ := source.CACert(); !bytes.Equal(cur, good) {
		t.Fatal("did not retain LKG after bad parse")
	}

	// Unreadable file: retain LKG.
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove file: %v", err)
	}
	if _, err := source.refresh(); err == nil {
		t.Fatal("expected error on unreadable file")
	}
	if cur, _ := source.CACert(); !bytes.Equal(cur, good) {
		t.Fatal("did not retain LKG after read error")
	}
}

func TestFilesystemCASourceBroadcastsOnlyOnChange(t *testing.T) {
	caCert, caKey, caPEM := newTestCA(t, "trusted-ca")
	leaf := newTestLeaf(t, "webhook.svc", caCert, caKey)
	_, _, otherPEM := newTestCA(t, "extra-ca")

	dir := t.TempDir()
	path := filepath.Join(dir, "ca.crt")
	writeFile(t, path, caPEM)

	source := newFileSource(t, path, fakeServedLeaf{leaf: leaf})
	sink := source.RegisterSink()

	// First commit broadcasts.
	if _, err := source.refresh(); err != nil {
		t.Fatalf("initial commit: %v", err)
	}
	if !drainedOne(sink) {
		t.Fatal("expected a broadcast on first commit")
	}

	// Identical content: no change, no broadcast.
	if changed, err := source.refresh(); err != nil || changed {
		t.Fatalf("expected no-op refresh, changed=%v err=%v", changed, err)
	}
	if drainedOne(sink) {
		t.Fatal("expected no broadcast when nothing changed")
	}

	// Adding an anchoring-preserving root changes the bundle and broadcasts.
	writeFile(t, path, append(append([]byte{}, caPEM...), otherPEM...))
	if changed, err := source.refresh(); err != nil || !changed {
		t.Fatalf("expected change, changed=%v err=%v", changed, err)
	}
	if !drainedOne(sink) {
		t.Fatal("expected a broadcast when the bundle changed")
	}
}

func TestFilesystemCASourceCACertCopyAndAvailability(t *testing.T) {
	source := newFileSource(t, "unused", fakeServedLeaf{})
	if _, err := source.CACert(); err == nil {
		t.Fatal("expected CACert error before any commit")
	}
	if source.NeedLeaderElection() {
		t.Fatal("filesystem CA source must not need leader election")
	}

	source.committedTrust = []byte("committed-bundle")
	first, err := source.CACert()
	if err != nil {
		t.Fatalf("CACert: %v", err)
	}
	first[0] = 'X' // mutate the returned copy
	second, err := source.CACert()
	if err != nil {
		t.Fatalf("CACert: %v", err)
	}
	if !bytes.Equal(second, []byte("committed-bundle")) {
		t.Fatal("CACert returned a view into internal state, not a copy")
	}
}

func TestNewFilesystemCABundleSourceBootstrap(t *testing.T) {
	caCert, caKey, caPEM := newTestCA(t, "trusted-ca")
	leaf := newTestLeaf(t, "webhook.svc", caCert, caKey)

	dir := t.TempDir()
	path := filepath.Join(dir, "ca.crt")

	// Success: a good file commits within the bootstrap budget.
	writeFile(t, path, caPEM)
	source, err := NewFilesystemCABundleSource(t.Context(), path, time.Second, time.Second, 10*time.Millisecond, fakeServedLeaf{leaf: leaf})
	if err != nil {
		t.Fatalf("bootstrap with good file: %v", err)
	}
	if _, err := source.CACert(); err != nil {
		t.Fatalf("expected committed bundle after bootstrap: %v", err)
	}

	// Timeout: a bundle that never anchors the served leaf fails within budget.
	writeFile(t, path, []byte("not a certificate"))
	if _, err := NewFilesystemCABundleSource(t.Context(), path, time.Second, 100*time.Millisecond, 10*time.Millisecond, fakeServedLeaf{leaf: leaf}); err == nil {
		t.Fatal("expected bootstrap to time out on a never-valid file")
	}

	// Argument validation.
	if _, err := NewFilesystemCABundleSource(t.Context(), "", time.Second, time.Second, time.Second, fakeServedLeaf{leaf: leaf}); err == nil {
		t.Fatal("expected error for empty path")
	}
	if _, err := NewFilesystemCABundleSource(t.Context(), path, 0, time.Second, time.Second, fakeServedLeaf{leaf: leaf}); err == nil {
		t.Fatal("expected error for non-positive sync interval")
	}
	if _, err := NewFilesystemCABundleSource(t.Context(), path, time.Second, time.Second, time.Second, nil); err == nil {
		t.Fatal("expected error for nil served leaf accessor")
	}
}

// drainedOne reports whether exactly one signal was pending on the sink.
func drainedOne[T any](sink <-chan T) bool {
	select {
	case <-sink:
		return true
	default:
		return false
	}
}
