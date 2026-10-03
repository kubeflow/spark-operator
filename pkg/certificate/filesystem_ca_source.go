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
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/util/wait"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

// caBundleRefreshJitterFactor spreads the periodic file read across replicas so
// they do not all stat the mounted CA file on the same tick.
const caBundleRefreshJitterFactor = 0.1

// servedLeafAccessor supplies the currently-served webhook leaf certificate for
// the CA<->leaf interlock. *DynamicTLSConfig satisfies it.
type servedLeafAccessor interface {
	ServedLeaf() (*x509.Certificate, error)
}

// FilesystemCABundleSource reads a CA bundle from a mounted file on an interval,
// minimally validates it, and exposes the last committed bundle to the webhook
// configuration reconcilers and to readiness. It is a CABundleSource and a
// (non-leader-elected) controller-runtime Runnable: every replica refreshes for
// its own readiness, while only the leader's reconcilers publish.
//
// v1 validation floor (Option A): a candidate is committed only when it parses
// to at least one CERTIFICATE block, canonicalizes (input order, exact-duplicate
// DER removed), and the currently-served leaf verifies against a pool built from
// it. Any failure retains the last-known-good bundle and retries on the next
// tick. Semantic hardening (IsCA/expiry/SAN/EKU/issuer, intermediate-chain
// interlock, read/count bounds, published-vs-desired trust tracking) is v2.
type FilesystemCABundleSource struct {
	path         string
	syncInterval time.Duration
	servedLeaf   servedLeafAccessor
	logger       logr.Logger

	// mu guards committedTrust, the last-known-good canonical PEM bundle.
	mu             sync.RWMutex
	committedTrust []byte

	// sinksMu guards sinks, the per-reconciler channels notified on change.
	sinksMu sync.Mutex
	sinks   []chan event.GenericEvent
}

var (
	_ CABundleSource = (*FilesystemCABundleSource)(nil)
)

// NewFilesystemCABundleSource constructs a source and performs one bounded
// bootstrap refresh so a ready source has a committed bundle before serving.
// The bootstrap mirrors the serving-cert acquisition retry loop and is bounded
// by waitTimeout; it fails if no valid bundle is committed within that budget.
func NewFilesystemCABundleSource(
	ctx context.Context,
	path string,
	syncInterval time.Duration,
	waitTimeout time.Duration,
	retryInterval time.Duration,
	servedLeaf servedLeafAccessor,
) (*FilesystemCABundleSource, error) {
	if path == "" {
		return nil, fmt.Errorf("CA bundle path must be set")
	}
	if syncInterval <= 0 {
		return nil, fmt.Errorf("CA bundle sync interval must be positive")
	}
	if servedLeaf == nil {
		return nil, fmt.Errorf("served leaf accessor must be set")
	}

	source := &FilesystemCABundleSource{
		path:         path,
		syncInterval: syncInterval,
		servedLeaf:   servedLeaf,
		logger:       ctrl.Log.WithName("filesystem-ca-bundle-source"),
	}
	if err := source.bootstrap(ctx, waitTimeout, retryInterval); err != nil {
		return nil, err
	}
	return source, nil
}

// bootstrap blocks until the first valid bundle is committed or waitTimeout
// elapses, retrying on retryInterval. It never publishes garbage: a bad file
// simply keeps the loop retrying until the deadline.
func (s *FilesystemCABundleSource) bootstrap(ctx context.Context, waitTimeout, retryInterval time.Duration) error {
	if waitTimeout <= 0 {
		return fmt.Errorf("CA bundle wait timeout must be positive")
	}
	if retryInterval <= 0 {
		return fmt.Errorf("CA bundle retry interval must be positive")
	}

	waitCtx, cancel := context.WithTimeout(ctx, waitTimeout)
	defer cancel()

	var lastErr error
	for {
		if err := waitCtx.Err(); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			return fmt.Errorf("timed out waiting for CA bundle %q: %w", s.path, lastErr)
		}

		if _, err := s.refresh(); err != nil {
			lastErr = err
		} else if s.hasCommitted() {
			return nil
		}

		timer := time.NewTimer(retryInterval)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			if errors.Is(waitCtx.Err(), context.Canceled) {
				return waitCtx.Err()
			}
			return fmt.Errorf("timed out waiting for CA bundle %q: %w", s.path, lastErr)
		case <-timer.C:
		}
	}
}

// Start runs the periodic refresh until the context is cancelled. It is a
// controller-runtime Runnable.
func (s *FilesystemCABundleSource) Start(ctx context.Context) error {
	wait.JitterUntilWithContext(ctx, s.refreshOnce, s.syncInterval, caBundleRefreshJitterFactor, true)
	return nil
}

// NeedLeaderElection reports that every replica refreshes independently so its
// own readiness reflects the mounted file, regardless of leadership.
func (*FilesystemCABundleSource) NeedLeaderElection() bool {
	return false
}

// CACert returns a copy of the last committed canonical bundle (never the raw
// file bytes), or an error when no bundle has been committed yet.
func (s *FilesystemCABundleSource) CACert() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.committedTrust) == 0 {
		return nil, fmt.Errorf("CA bundle is not available")
	}
	out := make([]byte, len(s.committedTrust))
	copy(out, s.committedTrust)
	return out, nil
}

// RegisterSink returns a buffered channel that receives an event whenever the
// committed bundle changes. Call it during setup, before Start, once per
// reconciler that should reconcile on CA change.
func (s *FilesystemCABundleSource) RegisterSink() <-chan event.GenericEvent {
	s.sinksMu.Lock()
	defer s.sinksMu.Unlock()
	sink := make(chan event.GenericEvent, 1)
	s.sinks = append(s.sinks, sink)
	return sink
}

// refreshOnce runs one refresh, logging and retaining the last-known-good bundle
// on any failure. It matches the signature wait.JitterUntilWithContext expects.
func (s *FilesystemCABundleSource) refreshOnce(context.Context) {
	if _, err := s.refresh(); err != nil {
		s.logger.Error(err, "Retaining last-known-good CA bundle after refresh failure", "path", s.path)
	}
}

// refresh reads, validates, and (on change) commits the CA bundle. It returns
// whether the committed bundle changed. On any error the committed bundle is
// left untouched.
func (s *FilesystemCABundleSource) refresh() (bool, error) {
	raw, err := os.ReadFile(s.path)
	if err != nil {
		return false, fmt.Errorf("read CA bundle %q: %w", s.path, err)
	}

	candidate, err := canonicalizeCABundle(raw)
	if err != nil {
		return false, fmt.Errorf("parse CA bundle %q: %w", s.path, err)
	}

	if err := s.verifyServedLeaf(candidate); err != nil {
		return false, fmt.Errorf("CA bundle %q does not anchor the served leaf: %w", s.path, err)
	}

	s.mu.Lock()
	if bytes.Equal(s.committedTrust, candidate) {
		s.mu.Unlock()
		return false, nil
	}
	s.committedTrust = candidate
	s.mu.Unlock()

	s.broadcast()
	s.logger.Info("Committed CA bundle", "path", s.path, "bytes", len(candidate))
	return true, nil
}

// verifyServedLeaf enforces the CA<->leaf interlock: the currently-served leaf
// must verify against a pool built from the candidate bundle. This is the v1
// floor; it assumes the served leaf is issued directly by a CA in the bundle
// (intermediate-chain interlock and semantic checks are v2). EKU is intentionally
// not constrained here.
func (s *FilesystemCABundleSource) verifyServedLeaf(bundle []byte) error {
	leaf, err := s.servedLeaf.ServedLeaf()
	if err != nil {
		return fmt.Errorf("get served leaf: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(bundle) {
		return fmt.Errorf("no usable CA certificates in bundle")
	}
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
	}); err != nil {
		return fmt.Errorf("served leaf does not verify against bundle: %w", err)
	}
	return nil
}

// broadcast performs a non-blocking send to every registered sink. Sinks are
// buffered size 1, so a full sink already has a pending signal and the send is
// coalesced.
func (s *FilesystemCABundleSource) broadcast() {
	s.sinksMu.Lock()
	defer s.sinksMu.Unlock()
	for _, sink := range s.sinks {
		select {
		case sink <- event.GenericEvent{}:
		default:
		}
	}
}

func (s *FilesystemCABundleSource) hasCommitted() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.committedTrust) > 0
}

// canonicalizeCABundle keeps only CERTIFICATE PEM blocks, requires each to parse
// as an X.509 certificate, drops exact-duplicate DER while preserving input
// order, and re-encodes to a normalized PEM so byte-equality comparison across
// reads is stable. It errors when no CERTIFICATE block is present.
func canonicalizeCABundle(raw []byte) ([]byte, error) {
	var out bytes.Buffer
	seen := make(map[string]struct{})
	count := 0

	rest := raw
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		if _, err := x509.ParseCertificate(block.Bytes); err != nil {
			return nil, fmt.Errorf("bundle contains an unparseable CERTIFICATE block: %w", err)
		}
		key := string(block.Bytes)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		if err := pem.Encode(&out, &pem.Block{Type: "CERTIFICATE", Bytes: block.Bytes}); err != nil {
			return nil, fmt.Errorf("re-encode certificate: %w", err)
		}
		count++
	}

	if count == 0 {
		return nil, fmt.Errorf("no CERTIFICATE blocks found")
	}
	return out.Bytes(), nil
}
