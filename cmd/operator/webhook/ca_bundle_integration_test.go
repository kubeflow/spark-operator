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

package webhook

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/kubeflow/spark-operator/v2/internal/controller/mutatingwebhookconfiguration"
	"github.com/kubeflow/spark-operator/v2/internal/controller/validatingwebhookconfiguration"
	"github.com/kubeflow/spark-operator/v2/pkg/certificate"
	operatorscheme "github.com/kubeflow/spark-operator/v2/pkg/scheme"
)

// fakeServedLeaf structurally satisfies the (unexported) servedLeafAccessor
// interface of the certificate package: ServedLeaf is exported, so a type
// declared here is assignable to that interface even though it cannot be named.
type fakeServedLeaf struct {
	leaf *x509.Certificate
}

func (f fakeServedLeaf) ServedLeaf() (*x509.Certificate, error) {
	return f.leaf, nil
}

// TestFilesystemCABundlePublicationIntegration is the end-to-end proof for the
// operator-owned filesystem caBundle path (KEP-3165 Phase 3 v1). It stands up a
// real API server (envtest), both named admission configurations, a
// FilesystemCABundleSource over a temp CA file, and both webhook-configuration
// reconcilers wired through a manager in filesystem+operator mode, then asserts:
//
//	(1) both configurations' caBundle populate to the committed bundle;
//	(2) readyz flips from not-ready to ready as convergence completes;
//	(3) rotating the CA file (adding a second root) converges both configs;
//	(4) a bad file retains the last-known-good bundle with no patch storm;
//	(5) a follower source refreshes its local trust without publishing.
func TestFilesystemCABundlePublicationIntegration(t *testing.T) {
	assetsDir := filepath.Join("..", "..", "..", "bin", "k8s", fmt.Sprintf("1.35.0-%s-%s", runtime.GOOS, runtime.GOARCH))
	if _, err := os.Stat(assetsDir); err != nil {
		t.Skipf("envtest binaries not found at %s: %v", assetsDir, err)
	}

	// Silence controller-runtime logging (and avoid the deferred "SetLogger was
	// never called" warning) for the duration of the test.
	ctrl.SetLogger(logr.Discard())

	testEnv := &envtest.Environment{BinaryAssetsDirectory: assetsDir}
	cfg, err := testEnv.Start()
	require.NoError(t, err)
	defer func() { require.NoError(t, testEnv.Stop()) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	directClient, err := client.New(cfg, client.Options{Scheme: operatorscheme.WebhookScheme})
	require.NoError(t, err)

	// --- trust material: CA1 + a leaf issued by CA1 (the served leaf), and a
	// second unrelated root CA2 used later to prove rotation converges. ---
	tempDir := t.TempDir()
	leaderFile := filepath.Join(tempDir, "ca.crt")
	followerFile := filepath.Join(tempDir, "follower-ca.crt")

	ca1PEM, ca1Cert, ca1Key := generateCACertPEM(t, "spark-operator-test-ca-1")
	ca2PEM, _, _ := generateCACertPEM(t, "spark-operator-test-ca-2")
	leaf1 := issueLeaf(t, ca1Cert, ca1Key, "spark-webhook.spark-operator.svc")

	require.NoError(t, os.WriteFile(leaderFile, ca1PEM, 0o600))

	// --- both named admission configurations start with empty caBundles. ---
	require.NoError(t, directClient.Create(ctx, newMutatingConfig(testMutatingName)))
	require.NoError(t, directClient.Create(ctx, newValidatingConfig(testValidatingName)))

	// --- the source performs a bounded bootstrap refresh at construction, so a
	// committed bundle exists before the reconcilers run. ---
	const syncInterval = 50 * time.Millisecond
	source, err := certificate.NewFilesystemCABundleSource(ctx, leaderFile, syncInterval, 15*time.Second, syncInterval, fakeServedLeaf{leaf: leaf1})
	require.NoError(t, err)

	desired1, err := source.CACert()
	require.NoError(t, err)
	require.NotEmpty(t, desired1)

	// helpers over the live API.
	getMutating := func() *admissionregistrationv1.MutatingWebhookConfiguration {
		obj := &admissionregistrationv1.MutatingWebhookConfiguration{}
		require.NoError(t, directClient.Get(ctx, types.NamespacedName{Name: testMutatingName}, obj))
		return obj
	}
	getValidating := func() *admissionregistrationv1.ValidatingWebhookConfiguration {
		obj := &admissionregistrationv1.ValidatingWebhookConfiguration{}
		require.NoError(t, directClient.Get(ctx, types.NamespacedName{Name: testValidatingName}, obj))
		return obj
	}
	converged := func(desired []byte) bool {
		if mutatingWebhookConfigurationConverged(ctx, directClient, testMutatingName, desired) != nil {
			return false
		}
		return validatingWebhookConfigurationConverged(ctx, directClient, testValidatingName, desired) == nil
	}

	// readyz composed exactly as start.go composes it in filesystem+operator mode.
	readyz := caBundleReadinessChecker(directClient, source, testMutatingName, testValidatingName)
	readyzErr := func() error { return readyz(httptest.NewRequest(http.MethodGet, "/readyz", nil)) }

	// (2a) before the reconcilers publish, the configs carry empty caBundles, so
	// readiness must fail closed even though the source already has a bundle.
	require.Error(t, readyzErr(), "readyz must be not-ready before the caBundle is published")

	// --- wire both reconcilers through a manager in filesystem+operator mode. ---
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:         operatorscheme.WebhookScheme,
		LeaderElection: false,
		Metrics:        metricsserver.Options{BindAddress: "0"},
	})
	require.NoError(t, err)

	require.NoError(t, mutatingwebhookconfiguration.
		NewReconciler(mgr.GetClient(), source, testMutatingName).
		WithCABundleEventChannel(source.RegisterSink()).
		SetupWithManager(mgr, controller.Options{}))
	require.NoError(t, validatingwebhookconfiguration.
		NewReconciler(mgr.GetClient(), source, testValidatingName).
		WithCABundleEventChannel(source.RegisterSink()).
		SetupWithManager(mgr, controller.Options{}))
	require.NoError(t, mgr.Add(source))

	mgrCtx, mgrCancel := context.WithCancel(ctx)
	started := make(chan error, 1)
	go func() { started <- mgr.Start(mgrCtx) }()
	defer func() {
		mgrCancel()
		<-started
	}()

	// (1) both configurations converge to the committed bundle, and (2b) readyz
	// flips to ready once convergence completes.
	require.Eventually(t, func() bool { return converged(desired1) }, 30*time.Second, 100*time.Millisecond,
		"both admission configurations should carry the committed caBundle")
	require.Eventually(t, func() bool { return readyzErr() == nil }, 30*time.Second, 100*time.Millisecond,
		"readyz should become ready after convergence")

	// (3) rotate the CA file to CA1+CA2 (leaf1 still anchored by CA1); the source
	// commits the new bundle and both configs converge to it.
	require.NoError(t, os.WriteFile(leaderFile, concatPEM(ca1PEM, ca2PEM), 0o600))
	require.Eventually(t, func() bool {
		cur, err := source.CACert()
		return err == nil && !bytes.Equal(cur, desired1)
	}, 10*time.Second, syncInterval, "source should commit the rotated bundle")

	desired2, err := source.CACert()
	require.NoError(t, err)
	require.False(t, bytes.Equal(desired1, desired2))
	require.Eventually(t, func() bool { return converged(desired2) }, 30*time.Second, 100*time.Millisecond,
		"both admission configurations should converge to the rotated caBundle")
	require.NoError(t, readyzErr(), "readyz should stay ready after rotation converges")

	// (4) a bad file retains the last-known-good bundle: the source keeps
	// desired2, the configs are not touched (no patch storm), and readyz stays
	// ready.
	mutRV := getMutating().ResourceVersion
	valRV := getValidating().ResourceVersion
	require.NoError(t, os.WriteFile(leaderFile, []byte("this is not a PEM certificate"), 0o600))
	time.Sleep(10 * syncInterval) // give the poller many chances to (wrongly) act

	cur, err := source.CACert()
	require.NoError(t, err)
	require.True(t, bytes.Equal(cur, desired2), "last-known-good bundle must be retained on bad input")
	require.True(t, converged(desired2), "configs must still carry the last-known-good bundle")
	assert.Equal(t, mutRV, getMutating().ResourceVersion, "mutating config must not be re-patched on bad input")
	assert.Equal(t, valRV, getValidating().ResourceVersion, "validating config must not be re-patched on bad input")
	require.NoError(t, readyzErr(), "readyz should remain ready on last-known-good")

	// (5) a follower source (no reconcilers, its own file) refreshes its local
	// trust on rotation but publishes nothing: it is not leader-elected, and the
	// leader's configs are untouched by the follower's file change.
	require.NoError(t, os.WriteFile(followerFile, ca1PEM, 0o600))
	follower, err := certificate.NewFilesystemCABundleSource(ctx, followerFile, syncInterval, 15*time.Second, syncInterval, fakeServedLeaf{leaf: leaf1})
	require.NoError(t, err)
	require.False(t, follower.NeedLeaderElection(), "every replica refreshes regardless of leadership")

	followerInitial, err := follower.CACert()
	require.NoError(t, err)

	followerCtx, followerCancel := context.WithCancel(ctx)
	defer followerCancel()
	go func() { _ = follower.Start(followerCtx) }()

	leaderMutRV := getMutating().ResourceVersion
	leaderValRV := getValidating().ResourceVersion

	require.NoError(t, os.WriteFile(followerFile, concatPEM(ca1PEM, ca2PEM), 0o600))
	require.Eventually(t, func() bool {
		cur, err := follower.CACert()
		return err == nil && !bytes.Equal(cur, followerInitial)
	}, 10*time.Second, syncInterval, "follower should refresh its local trust on rotation")

	assert.Equal(t, leaderMutRV, getMutating().ResourceVersion, "follower must not publish to the mutating config")
	assert.Equal(t, leaderValRV, getValidating().ResourceVersion, "follower must not publish to the validating config")
}

// generateCACertPEM returns a self-signed CA certificate as PEM alongside its
// parsed form and signing key, for issuing leaves and building trust bundles.
func generateCACertPEM(t *testing.T, commonName string) ([]byte, *x509.Certificate, *rsa.PrivateKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber:          randomSerial(t),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), cert, key
}

// issueLeaf issues an end-entity certificate signed by the given CA.
func issueLeaf(t *testing.T, ca *x509.Certificate, caKey *rsa.PrivateKey, dnsName string) *x509.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tmpl := &x509.Certificate{
		SerialNumber: randomSerial(t),
		Subject:      pkix.Name{CommonName: dnsName},
		DNSNames:     []string{dnsName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)

	return leaf
}

func randomSerial(t *testing.T) *big.Int {
	t.Helper()
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	require.NoError(t, err)
	return serial
}

// concatPEM concatenates PEM blocks; each block already carries a trailing
// newline, so the result is a valid multi-certificate bundle.
func concatPEM(pems ...[]byte) []byte {
	var out []byte
	for _, p := range pems {
		out = append(out, p...)
	}
	return out
}

func newMutatingConfig(name string) *admissionregistrationv1.MutatingWebhookConfiguration {
	return &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Webhooks: []admissionregistrationv1.MutatingWebhook{
			{
				Name:                    "w1.spark-operator.io",
				ClientConfig:            admissionregistrationv1.WebhookClientConfig{URL: ptr.To("https://example.com/mutate-1")},
				SideEffects:             ptr.To(admissionregistrationv1.SideEffectClassNone),
				AdmissionReviewVersions: []string{"v1"},
			},
			{
				Name:                    "w2.spark-operator.io",
				ClientConfig:            admissionregistrationv1.WebhookClientConfig{URL: ptr.To("https://example.com/mutate-2")},
				SideEffects:             ptr.To(admissionregistrationv1.SideEffectClassNone),
				AdmissionReviewVersions: []string{"v1"},
			},
		},
	}
}

func newValidatingConfig(name string) *admissionregistrationv1.ValidatingWebhookConfiguration {
	return &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{
			{
				Name:                    "w1.spark-operator.io",
				ClientConfig:            admissionregistrationv1.WebhookClientConfig{URL: ptr.To("https://example.com/validate-1")},
				SideEffects:             ptr.To(admissionregistrationv1.SideEffectClassNone),
				AdmissionReviewVersions: []string{"v1"},
			},
			{
				Name:                    "w2.spark-operator.io",
				ClientConfig:            admissionregistrationv1.WebhookClientConfig{URL: ptr.To("https://example.com/validate-2")},
				SideEffects:             ptr.To(admissionregistrationv1.SideEffectClassNone),
				AdmissionReviewVersions: []string{"v1"},
			},
		},
	}
}
