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

package statestore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// Key layout (all keys carry the configurable prefix, default "sparkoperator"):
//
//	<prefix>/fencing/<namespace>/<name>              -> integer epoch
//	<prefix>/heartbeat/<namespace>/<name>            -> hash {epoch, seq}
//	<prefix>/snapshot/<namespace>/<name>/<epoch>     -> marker bytes
//	<prefix>/snapshots/<namespace>/<name>            -> zset of epochs with snapshots
//
// All guarded mutations run as Lua scripts so the check-epoch-then-write pair
// is a single atomic step. Without this there is a TOCTOU hole: the agent
// could read epoch 3, the operator could fence to 4, and the stale write
// would still land.

const fencedReply = "FENCED"

// putSnapshotScript: KEYS[1]=fencing, KEYS[2]=snapshot, KEYS[3]=snapshot zset
// ARGV[1]=expected epoch, ARGV[2]=payload.
var putSnapshotScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if current == false or tonumber(current) ~= tonumber(ARGV[1]) then
  return redis.error_reply('FENCED')
end
redis.call('SET', KEYS[2], ARGV[2])
redis.call('ZADD', KEYS[3], tonumber(ARGV[1]), ARGV[1])
return redis.status_reply('OK')
`)

// heartbeatScript: KEYS[1]=fencing, KEYS[2]=heartbeat hash
// ARGV[1]=expected epoch.
var heartbeatScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if current == false or tonumber(current) ~= tonumber(ARGV[1]) then
  return redis.error_reply('FENCED')
end
redis.call('HSET', KEYS[2], 'epoch', ARGV[1])
redis.call('HINCRBY', KEYS[2], 'seq', 1)
return redis.status_reply('OK')
`)

// RedisStore implements StateStore backed by Redis.
type RedisStore struct {
	client redis.UniversalClient
	prefix string
}

var _ StateStore = &RedisStore{}

// RedisOptions configures a RedisStore.
type RedisOptions struct {
	// Address is the host:port of the Redis endpoint.
	Address string
	// Password is optional.
	Password string
	// KeyPrefix defaults to "sparkoperator".
	KeyPrefix string
}

// NewRedisStore creates a RedisStore.
func NewRedisStore(opts RedisOptions) *RedisStore {
	prefix := opts.KeyPrefix
	if prefix == "" {
		prefix = "sparkoperator"
	}
	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:       []string{opts.Address},
		Password:    opts.Password,
		DialTimeout: DefaultDialTimeout,
	})
	return &RedisStore{client: client, prefix: prefix}
}

func (s *RedisStore) fencingKey(key JobKey) string {
	return fmt.Sprintf("%s/fencing/%s/%s", s.prefix, key.Namespace, key.Name)
}

func (s *RedisStore) heartbeatKey(key JobKey) string {
	return fmt.Sprintf("%s/heartbeat/%s/%s", s.prefix, key.Namespace, key.Name)
}

func (s *RedisStore) snapshotKey(key JobKey, epoch int64) string {
	return fmt.Sprintf("%s/snapshot/%s/%s/%d", s.prefix, key.Namespace, key.Name, epoch)
}

func (s *RedisStore) snapshotIndexKey(key JobKey) string {
	return fmt.Sprintf("%s/snapshots/%s/%s", s.prefix, key.Namespace, key.Name)
}

// CurrentEpoch implements StateStore.
func (s *RedisStore) CurrentEpoch(ctx context.Context, key JobKey) (int64, error) {
	// SETNX initializes the epoch to 0 exactly once; GET reads it back.
	if err := s.client.SetNX(ctx, s.fencingKey(key), 0, 0).Err(); err != nil {
		return 0, fmt.Errorf("failed to initialize fencing epoch: %w", err)
	}
	val, err := s.client.Get(ctx, s.fencingKey(key)).Int64()
	if err != nil {
		return 0, fmt.Errorf("failed to read fencing epoch: %w", err)
	}
	return val, nil
}

// AdvanceEpoch implements StateStore.
func (s *RedisStore) AdvanceEpoch(ctx context.Context, key JobKey) (int64, error) {
	val, err := s.client.Incr(ctx, s.fencingKey(key)).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to advance fencing epoch: %w", err)
	}
	return val, nil
}

// PutSnapshot implements StateStore.
func (s *RedisStore) PutSnapshot(ctx context.Context, key JobKey, epoch int64, data []byte) error {
	err := putSnapshotScript.Run(ctx, s.client,
		[]string{s.fencingKey(key), s.snapshotKey(key, epoch), s.snapshotIndexKey(key)},
		epoch, data,
	).Err()
	if err != nil {
		if isFencedErr(err) {
			return ErrFenced
		}
		return fmt.Errorf("failed to put snapshot: %w", err)
	}
	return nil
}

// LatestSnapshot implements StateStore.
func (s *RedisStore) LatestSnapshot(ctx context.Context, key JobKey, before int64) (Snapshot, error) {
	epochs, err := s.client.ZRevRangeByScore(ctx, s.snapshotIndexKey(key), &redis.ZRangeBy{
		Min:   "-inf",
		Max:   strconv.FormatInt(before, 10),
		Count: 1,
	}).Result()
	if err != nil {
		return Snapshot{}, fmt.Errorf("failed to list snapshots: %w", err)
	}
	if len(epochs) == 0 {
		return Snapshot{}, ErrNotFound
	}
	epoch, err := strconv.ParseInt(epochs[0], 10, 64)
	if err != nil {
		return Snapshot{}, fmt.Errorf("corrupt snapshot index entry %q: %w", epochs[0], err)
	}
	data, err := s.client.Get(ctx, s.snapshotKey(key, epoch)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return Snapshot{}, ErrNotFound
		}
		return Snapshot{}, fmt.Errorf("failed to read snapshot: %w", err)
	}
	return Snapshot{Epoch: epoch, Data: data}, nil
}

// Heartbeat implements StateStore.
func (s *RedisStore) Heartbeat(ctx context.Context, key JobKey, epoch int64) error {
	err := heartbeatScript.Run(ctx, s.client,
		[]string{s.fencingKey(key), s.heartbeatKey(key)},
		epoch,
	).Err()
	if err != nil {
		if isFencedErr(err) {
			return ErrFenced
		}
		return fmt.Errorf("failed to record heartbeat: %w", err)
	}
	return nil
}

// LastHeartbeat implements StateStore.
func (s *RedisStore) LastHeartbeat(ctx context.Context, key JobKey) (HeartbeatRecord, error) {
	vals, err := s.client.HGetAll(ctx, s.heartbeatKey(key)).Result()
	if err != nil {
		return HeartbeatRecord{}, fmt.Errorf("failed to read heartbeat: %w", err)
	}
	if len(vals) == 0 {
		return HeartbeatRecord{}, ErrNotFound
	}
	epoch, err := strconv.ParseInt(vals["epoch"], 10, 64)
	if err != nil {
		return HeartbeatRecord{}, fmt.Errorf("corrupt heartbeat epoch %q: %w", vals["epoch"], err)
	}
	seq, err := strconv.ParseInt(vals["seq"], 10, 64)
	if err != nil {
		return HeartbeatRecord{}, fmt.Errorf("corrupt heartbeat seq %q: %w", vals["seq"], err)
	}
	return HeartbeatRecord{Epoch: epoch, Seq: seq}, nil
}

// PruneSnapshots implements StateStore.
func (s *RedisStore) PruneSnapshots(ctx context.Context, key JobKey, keepLast int) error {
	if keepLast < 1 {
		keepLast = 1
	}
	epochs, err := s.client.ZRevRange(ctx, s.snapshotIndexKey(key), int64(keepLast), -1).Result()
	if err != nil {
		return fmt.Errorf("failed to list snapshots for pruning: %w", err)
	}
	for _, e := range epochs {
		epoch, err := strconv.ParseInt(e, 10, 64)
		if err != nil {
			continue
		}
		if err := s.client.Del(ctx, s.snapshotKey(key, epoch)).Err(); err != nil {
			return fmt.Errorf("failed to prune snapshot for epoch %d: %w", epoch, err)
		}
		if err := s.client.ZRem(ctx, s.snapshotIndexKey(key), e).Err(); err != nil {
			return fmt.Errorf("failed to prune snapshot index for epoch %d: %w", epoch, err)
		}
	}
	return nil
}

// Ping implements StateStore.
func (s *RedisStore) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

// Close implements StateStore.
func (s *RedisStore) Close() error {
	return s.client.Close()
}

func isFencedErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), fencedReply)
}
