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

// Package statestore provides the fencing and progress-marker backend for
// fenced, progress-preserving restarts (FencedRestart feature gate).
//
// The contract is deliberately small and backend-neutral:
//
//   - The fencing epoch is a monotonic integer, only ever changed by
//     AdvanceEpoch, and only by the operator.
//   - Every write performed on behalf of a driver carries the epoch that
//     driver was born with; implementations MUST reject writes whose epoch
//     is no longer current, atomically with the check (ErrFenced).
//   - Heartbeats are monotonic sequence numbers judged by the READER on its
//     own clock. No store-side TTLs and no cross-machine wall-clock
//     comparisons are part of the contract, so object stores without an
//     expiry primitive can implement it in a follow-up.
package statestore

import (
	"context"
	"errors"
	"time"
)

// Sentinel errors, checked with errors.Is.
var (
	// ErrFenced is returned when a write is rejected because its epoch is no
	// longer the current fencing epoch.
	ErrFenced = errors.New("statestore: epoch is fenced")

	// ErrNotFound is returned when the requested record does not exist.
	ErrNotFound = errors.New("statestore: not found")
)

// JobKey identifies one SparkApplication in the store.
type JobKey struct {
	// Namespace of the SparkApplication.
	Namespace string
	// Name of the SparkApplication.
	Name string
}

// HeartbeatRecord is a liveness beat written by the recovery agent.
// Liveness is judged by the reader: a driver is suspect when Seq has not
// advanced for the configured heartbeatTTL on the reader's own clock.
type HeartbeatRecord struct {
	Epoch int64
	Seq   int64
}

// Snapshot is a committed progress marker.
type Snapshot struct {
	Epoch int64
	Data  []byte
}

// StateStore is the fencing and progress-marker backend. Implementations
// must be safe for concurrent use.
type StateStore interface {
	// CurrentEpoch returns the fencing epoch for the job, creating it at 0
	// if absent.
	CurrentEpoch(ctx context.Context, key JobKey) (int64, error)

	// AdvanceEpoch atomically increments and returns the new epoch. After it
	// returns e, no write guarded by any epoch < e can ever succeed.
	AdvanceEpoch(ctx context.Context, key JobKey) (int64, error)

	// PutSnapshot stores data as the progress marker for (key, epoch) if and
	// only if epoch is still the current fencing epoch; otherwise ErrFenced.
	// The epoch check and the write are atomic.
	PutSnapshot(ctx context.Context, key JobKey, epoch int64, data []byte) error

	// LatestSnapshot returns the most recent snapshot with epoch <= before,
	// or ErrNotFound if none exists.
	LatestSnapshot(ctx context.Context, key JobKey, before int64) (Snapshot, error)

	// Heartbeat records a liveness beat (monotonic sequence bump) if and only
	// if epoch is still current; otherwise ErrFenced.
	Heartbeat(ctx context.Context, key JobKey, epoch int64) error

	// LastHeartbeat returns the most recent heartbeat record, or ErrNotFound.
	LastHeartbeat(ctx context.Context, key JobKey) (HeartbeatRecord, error)

	// PruneSnapshots removes snapshots older than the keepLast most recent
	// epochs that have snapshots.
	PruneSnapshots(ctx context.Context, key JobKey, keepLast int) error

	// Ping verifies connectivity.
	Ping(ctx context.Context) error

	// Close releases resources held by the store.
	Close() error
}

// DefaultDialTimeout bounds individual store operations issued by the
// operator; callers should also pass context deadlines.
const DefaultDialTimeout = 5 * time.Second
