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

// Package agent implements the spark-recovery-agent sidecar for fenced,
// progress-preserving restarts (FencedRestart feature gate).
//
// The agent runs next to the Spark driver container and:
//
//   - restores the latest committed progress marker onto the shared volume
//     before serving traffic, so the driver can read it at startup;
//   - serves a localhost HTTP API through which the application commits
//     progress markers (epoch-guarded writes to the state store);
//   - records heartbeats (monotonic sequence bumps) so the operator can
//     detect partitioned nodes faster than kubelet reporting;
//   - exits with a distinct code the moment the store reports it fenced,
//     taking the stale driver's protection visibly offline.
//
// Exit codes: 0 on graceful shutdown, 64 when fenced.
package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	ctrl "sigs.k8s.io/controller-runtime"
	logzap "sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/statestore"
)

const (
	// ExitCodeFenced signals that this agent's epoch was fenced by the
	// operator: a newer run of the application exists.
	ExitCodeFenced = 64

	defaultHeartbeatInterval = 10 * time.Second
	defaultSnapshotMaxBytes  = int64(1 << 20) // 1 MiB
	shutdownGracePeriod      = 5 * time.Second
)

var logger = ctrl.Log.WithName("recovery-agent")

// config is assembled from the environment injected by the pod webhook.
type config struct {
	key               statestore.JobKey
	epoch             int64
	restoreEpoch      *int64
	storeAddress      string
	storePassword     string
	heartbeatInterval time.Duration
	snapshotMaxBytes  int64
	listenAddr        string
	snapshotDir       string
}

func configFromEnv() (config, error) {
	cfg := config{
		key: statestore.JobKey{
			Namespace: os.Getenv(common.EnvRecoveryJobNamespace),
			Name:      os.Getenv(common.EnvRecoveryJobName),
		},
		storeAddress:      os.Getenv(common.EnvRecoveryStoreAddress),
		storePassword:     os.Getenv(common.EnvRecoveryStorePassword),
		heartbeatInterval: defaultHeartbeatInterval,
		snapshotMaxBytes:  defaultSnapshotMaxBytes,
		listenAddr:        fmt.Sprintf(":%d", common.RecoveryAgentPort),
		snapshotDir:       common.RecoveryVolumeMountPath,
	}
	if cfg.key.Namespace == "" || cfg.key.Name == "" {
		return cfg, fmt.Errorf("%s and %s must be set", common.EnvRecoveryJobNamespace, common.EnvRecoveryJobName)
	}
	if cfg.storeAddress == "" {
		return cfg, fmt.Errorf("%s must be set", common.EnvRecoveryStoreAddress)
	}
	epoch, err := strconv.ParseInt(os.Getenv(common.EnvRecoveryEpoch), 10, 64)
	if err != nil {
		return cfg, fmt.Errorf("invalid %s: %w", common.EnvRecoveryEpoch, err)
	}
	cfg.epoch = epoch
	if v := os.Getenv(common.EnvRecoveryRestoreEpoch); v != "" {
		restoreEpoch, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return cfg, fmt.Errorf("invalid %s: %w", common.EnvRecoveryRestoreEpoch, err)
		}
		cfg.restoreEpoch = &restoreEpoch
	}
	if v := os.Getenv(common.EnvRecoveryHeartbeatIntvl); v != "" {
		interval, err := time.ParseDuration(v)
		if err != nil {
			return cfg, fmt.Errorf("invalid %s: %w", common.EnvRecoveryHeartbeatIntvl, err)
		}
		cfg.heartbeatInterval = interval
	}
	if v := os.Getenv(common.EnvRecoverySnapshotMaxSz); v != "" {
		maxBytes, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return cfg, fmt.Errorf("invalid %s: %w", common.EnvRecoverySnapshotMaxSz, err)
		}
		cfg.snapshotMaxBytes = maxBytes
	}
	return cfg, nil
}

// NewStartCommand creates the `agent start` command.
func NewStartCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "start",
		Short: "Start the Spark recovery agent",
		RunE: func(_ *cobra.Command, _ []string) error {
			ctrl.SetLogger(logzap.New())
			cfg, err := configFromEnv()
			if err != nil {
				return err
			}
			store := statestore.NewRedisStore(statestore.RedisOptions{
				Address:  cfg.storeAddress,
				Password: cfg.storePassword,
			})
			defer func() { _ = store.Close() }()
			return run(cfg, store)
		},
	}
}

func run(cfg config, store statestore.StateStore) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	agent := &recoveryAgent{cfg: cfg, store: store}

	if err := agent.restore(ctx); err != nil {
		return fmt.Errorf("failed to restore progress marker: %w", err)
	}

	server := &http.Server{
		Addr:              cfg.listenAddr,
		Handler:           agent.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() {
		logger.Info("Recovery agent serving", "addr", cfg.listenAddr, "epoch", cfg.epoch)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	fenced := make(chan struct{})
	go agent.heartbeatLoop(ctx, fenced)

	select {
	case <-ctx.Done():
		logger.Info("Shutting down recovery agent")
	case err := <-serverErr:
		return fmt.Errorf("recovery agent server failed: %w", err)
	case <-fenced:
		logger.Info("Epoch was fenced by the operator; exiting", "epoch", cfg.epoch, "exitCode", ExitCodeFenced)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGracePeriod)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		os.Exit(ExitCodeFenced)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGracePeriod)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}

type recoveryAgent struct {
	cfg   config
	store statestore.StateStore

	mu       sync.RWMutex
	restored []byte
	isFenced bool
}

// restore fetches the marker committed before this epoch, writes it to the
// shared volume for file-based consumers, and keeps it in memory for the
// HTTP API. Run before the server accepts traffic so the driver never
// observes a partially restored state.
func (a *recoveryAgent) restore(ctx context.Context) error {
	if a.cfg.restoreEpoch == nil {
		logger.Info("No progress marker to restore; starting fresh", "epoch", a.cfg.epoch)
		return nil
	}
	snapshot, err := a.store.LatestSnapshot(ctx, a.cfg.key, *a.cfg.restoreEpoch)
	if err != nil {
		if errors.Is(err, statestore.ErrNotFound) {
			logger.Info("Restore epoch set but no marker found; starting fresh",
				"restoreEpoch", *a.cfg.restoreEpoch)
			return nil
		}
		return err
	}
	path := filepath.Join(a.cfg.snapshotDir, common.RecoverySnapshotFileName)
	if err := os.WriteFile(path, snapshot.Data, 0o644); err != nil {
		return fmt.Errorf("failed to write restored marker to %s: %w", path, err)
	}
	a.mu.Lock()
	a.restored = snapshot.Data
	a.mu.Unlock()
	logger.Info("Restored progress marker", "fromEpoch", snapshot.Epoch, "bytes", len(snapshot.Data), "path", path)
	return nil
}

func (a *recoveryAgent) heartbeatLoop(ctx context.Context, fenced chan<- struct{}) {
	ticker := time.NewTicker(a.cfg.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := a.store.Heartbeat(ctx, a.cfg.key, a.cfg.epoch)
			switch {
			case err == nil:
			case errors.Is(err, statestore.ErrFenced):
				a.mu.Lock()
				a.isFenced = true
				a.mu.Unlock()
				close(fenced)
				return
			default:
				// Store outages must not kill a healthy driver: log and retry
				// on the next tick. The operator observes the stalled
				// heartbeat and applies its own grace period.
				logger.Error(err, "Failed to record heartbeat; will retry")
			}
		}
	}
}

func (a *recoveryAgent) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", a.handleHealthz)
	mux.HandleFunc("/v1/snapshot", a.handleSnapshot)
	return mux
}

func (a *recoveryAgent) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	a.mu.RLock()
	fenced := a.isFenced
	a.mu.RUnlock()
	if fenced {
		http.Error(w, "fenced", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handleSnapshot implements the progress-marker API:
//
//	GET  /v1/snapshot -> the marker restored at boot (404 if none)
//	PUT  /v1/snapshot -> commit the request body as this epoch's marker
func (a *recoveryAgent) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		a.mu.RLock()
		restored := a.restored
		a.mu.RUnlock()
		if restored == nil {
			http.Error(w, "no progress marker was restored", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(restored)

	case http.MethodPut, http.MethodPost:
		body, err := io.ReadAll(io.LimitReader(r.Body, a.cfg.snapshotMaxBytes+1))
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to read body: %v", err), http.StatusBadRequest)
			return
		}
		if int64(len(body)) > a.cfg.snapshotMaxBytes {
			http.Error(w, fmt.Sprintf("marker exceeds the %d byte limit", a.cfg.snapshotMaxBytes), http.StatusRequestEntityTooLarge)
			return
		}
		err = a.store.PutSnapshot(r.Context(), a.cfg.key, a.cfg.epoch, body)
		switch {
		case err == nil:
			logger.Info("Committed progress marker", "epoch", a.cfg.epoch, "bytes", len(body))
			w.WriteHeader(http.StatusNoContent)
		case errors.Is(err, statestore.ErrFenced):
			// A newer epoch exists; this driver must stop committing.
			http.Error(w, "fenced: a newer run of this application exists", http.StatusConflict)
		default:
			http.Error(w, fmt.Sprintf("failed to commit marker: %v", err), http.StatusBadGateway)
		}

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
