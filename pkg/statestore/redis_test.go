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

package statestore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/kubeflow/spark-operator/v2/pkg/statestore"
)

func newTestStore(t *testing.T) statestore.StateStore {
	t.Helper()
	mr := miniredis.RunT(t)
	store := statestore.NewRedisStore(statestore.RedisOptions{Address: mr.Addr()})
	t.Cleanup(func() { _ = store.Close() })
	return store
}

var testKey = statestore.JobKey{Namespace: "default", Name: "spark-pi"}

func TestCurrentEpochInitializesToZero(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	epoch, err := store.CurrentEpoch(ctx, testKey)
	if err != nil {
		t.Fatalf("CurrentEpoch: %v", err)
	}
	if epoch != 0 {
		t.Fatalf("expected initial epoch 0, got %d", epoch)
	}

	// Idempotent: a second call must not re-initialize.
	if _, err := store.AdvanceEpoch(ctx, testKey); err != nil {
		t.Fatalf("AdvanceEpoch: %v", err)
	}
	epoch, err = store.CurrentEpoch(ctx, testKey)
	if err != nil {
		t.Fatalf("CurrentEpoch: %v", err)
	}
	if epoch != 1 {
		t.Fatalf("expected epoch 1 after advance, got %d", epoch)
	}
}

func TestAdvanceEpochIsMonotonic(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	var last int64
	for i := 1; i <= 5; i++ {
		epoch, err := store.AdvanceEpoch(ctx, testKey)
		if err != nil {
			t.Fatalf("AdvanceEpoch: %v", err)
		}
		if epoch <= last {
			t.Fatalf("epoch went backwards: %d after %d", epoch, last)
		}
		last = epoch
	}
}

func TestPutSnapshotRejectsFencedEpoch(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	if _, err := store.CurrentEpoch(ctx, testKey); err != nil {
		t.Fatalf("CurrentEpoch: %v", err)
	}

	// Epoch 0 is current: write must succeed.
	if err := store.PutSnapshot(ctx, testKey, 0, []byte("marker-0")); err != nil {
		t.Fatalf("PutSnapshot at current epoch: %v", err)
	}

	// Fence to epoch 1. The stale writer (epoch 0) must be rejected.
	if _, err := store.AdvanceEpoch(ctx, testKey); err != nil {
		t.Fatalf("AdvanceEpoch: %v", err)
	}
	err := store.PutSnapshot(ctx, testKey, 0, []byte("stale"))
	if !errors.Is(err, statestore.ErrFenced) {
		t.Fatalf("expected ErrFenced for stale epoch, got %v", err)
	}

	// The fenced write must not have replaced the committed marker.
	snap, err := store.LatestSnapshot(ctx, testKey, 1)
	if err != nil {
		t.Fatalf("LatestSnapshot: %v", err)
	}
	if string(snap.Data) != "marker-0" || snap.Epoch != 0 {
		t.Fatalf("expected marker-0@0, got %q@%d", snap.Data, snap.Epoch)
	}
}

func TestLatestSnapshotHonorsBefore(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	if _, err := store.CurrentEpoch(ctx, testKey); err != nil {
		t.Fatalf("CurrentEpoch: %v", err)
	}
	if err := store.PutSnapshot(ctx, testKey, 0, []byte("m0")); err != nil {
		t.Fatalf("PutSnapshot: %v", err)
	}
	if _, err := store.AdvanceEpoch(ctx, testKey); err != nil {
		t.Fatalf("AdvanceEpoch: %v", err)
	}
	if err := store.PutSnapshot(ctx, testKey, 1, []byte("m1")); err != nil {
		t.Fatalf("PutSnapshot: %v", err)
	}

	snap, err := store.LatestSnapshot(ctx, testKey, 1)
	if err != nil {
		t.Fatalf("LatestSnapshot: %v", err)
	}
	if snap.Epoch != 1 || string(snap.Data) != "m1" {
		t.Fatalf("expected m1@1, got %q@%d", snap.Data, snap.Epoch)
	}

	snap, err = store.LatestSnapshot(ctx, testKey, 0)
	if err != nil {
		t.Fatalf("LatestSnapshot(before=0): %v", err)
	}
	if snap.Epoch != 0 || string(snap.Data) != "m0" {
		t.Fatalf("expected m0@0, got %q@%d", snap.Data, snap.Epoch)
	}

	_, err = store.LatestSnapshot(ctx, statestore.JobKey{Namespace: "default", Name: "other"}, 10)
	if !errors.Is(err, statestore.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for unknown job, got %v", err)
	}
}

func TestHeartbeatSequenceAdvancesAndFences(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	if _, err := store.CurrentEpoch(ctx, testKey); err != nil {
		t.Fatalf("CurrentEpoch: %v", err)
	}

	if _, err := store.LastHeartbeat(ctx, testKey); !errors.Is(err, statestore.ErrNotFound) {
		t.Fatalf("expected ErrNotFound before first heartbeat")
	}

	if err := store.Heartbeat(ctx, testKey, 0); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if err := store.Heartbeat(ctx, testKey, 0); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	hb, err := store.LastHeartbeat(ctx, testKey)
	if err != nil {
		t.Fatalf("LastHeartbeat: %v", err)
	}
	if hb.Epoch != 0 || hb.Seq != 2 {
		t.Fatalf("expected epoch 0 seq 2, got %+v", hb)
	}

	// After fencing, the old driver's heartbeats must be rejected.
	if _, err := store.AdvanceEpoch(ctx, testKey); err != nil {
		t.Fatalf("AdvanceEpoch: %v", err)
	}
	if err := store.Heartbeat(ctx, testKey, 0); !errors.Is(err, statestore.ErrFenced) {
		t.Fatalf("expected ErrFenced for stale heartbeat, got %v", err)
	}
}

func TestPruneSnapshotsKeepsMostRecent(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()

	if _, err := store.CurrentEpoch(ctx, testKey); err != nil {
		t.Fatalf("CurrentEpoch: %v", err)
	}
	for epoch := int64(0); epoch < 5; epoch++ {
		if err := store.PutSnapshot(ctx, testKey, epoch, []byte{byte('a' + epoch)}); err != nil {
			t.Fatalf("PutSnapshot@%d: %v", epoch, err)
		}
		if _, err := store.AdvanceEpoch(ctx, testKey); err != nil {
			t.Fatalf("AdvanceEpoch: %v", err)
		}
	}

	if err := store.PruneSnapshots(ctx, testKey, 2); err != nil {
		t.Fatalf("PruneSnapshots: %v", err)
	}

	// Epochs 3 and 4 survive; 2 and older are gone.
	snap, err := store.LatestSnapshot(ctx, testKey, 4)
	if err != nil || snap.Epoch != 4 {
		t.Fatalf("expected snapshot@4 to survive, got %+v err=%v", snap, err)
	}
	snap, err = store.LatestSnapshot(ctx, testKey, 3)
	if err != nil || snap.Epoch != 3 {
		t.Fatalf("expected snapshot@3 to survive, got %+v err=%v", snap, err)
	}
	if _, err := store.LatestSnapshot(ctx, testKey, 2); !errors.Is(err, statestore.ErrNotFound) {
		t.Fatalf("expected snapshots <=2 to be pruned, got %v", err)
	}
}

func TestParseProfiles(t *testing.T) {
	registry, err := statestore.ParseProfiles("default=redis:redis.ns.svc:6379,alt=redis:other:6380")
	if err != nil {
		t.Fatalf("ParseProfiles: %v", err)
	}
	if !registry.Has("default") || !registry.Has("alt") {
		t.Fatalf("expected both profiles present")
	}
	p, _ := registry.Get("default")
	if p.Address != "redis.ns.svc:6379" || p.Type != "redis" {
		t.Fatalf("unexpected profile: %+v", p)
	}

	if _, err := statestore.ParseProfiles("bad-entry"); err == nil {
		t.Fatal("expected error for malformed entry")
	}
	if _, err := statestore.ParseProfiles("name=s3:bucket"); err == nil {
		t.Fatal("expected error for unsupported type")
	}
	empty, err := statestore.ParseProfiles("")
	if err != nil {
		t.Fatalf("ParseProfiles(empty): %v", err)
	}
	if empty.Has("default") {
		t.Fatal("empty registry should have no profiles")
	}
}
