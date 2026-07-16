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
	"fmt"
	"os"
	"strings"
	"sync"
)

// Profile is a named state-store configuration provided by the operator
// administrator. Applications reference profiles by name via
// spec.restartPolicy.recovery.storeProfile, keeping store endpoints and
// credentials out of application manifests.
type Profile struct {
	Name    string
	Type    string // only "redis" today
	Address string
	// Password is resolved from the environment variable
	// RECOVERY_STORE_<NAME>_PASSWORD (uppercased, dashes to underscores),
	// so credentials never appear in flags or manifests.
	Password string
}

// ProfileRegistry holds the parsed --recovery-store-profiles configuration.
type ProfileRegistry struct {
	mu       sync.Mutex
	profiles map[string]Profile
	stores   map[string]StateStore
}

// ParseProfiles parses the --recovery-store-profiles flag value. Format:
//
//	name=type:address[,name=type:address...]
//
// e.g. "default=redis:redis.spark-operator.svc:6379".
func ParseProfiles(flagValue string) (*ProfileRegistry, error) {
	registry := &ProfileRegistry{
		profiles: make(map[string]Profile),
		stores:   make(map[string]StateStore),
	}
	if strings.TrimSpace(flagValue) == "" {
		return registry, nil
	}
	for _, entry := range strings.Split(flagValue, ",") {
		name, rest, found := strings.Cut(strings.TrimSpace(entry), "=")
		if !found || name == "" {
			return nil, fmt.Errorf("invalid store profile entry %q: expected name=type:address", entry)
		}
		storeType, address, found := strings.Cut(rest, ":")
		if !found || address == "" {
			return nil, fmt.Errorf("invalid store profile entry %q: expected name=type:address", entry)
		}
		if storeType != "redis" {
			return nil, fmt.Errorf("unsupported store profile type %q in entry %q: only \"redis\" is supported", storeType, entry)
		}
		envKey := fmt.Sprintf("RECOVERY_STORE_%s_PASSWORD", strings.ToUpper(strings.ReplaceAll(name, "-", "_")))
		registry.profiles[name] = Profile{
			Name:     name,
			Type:     storeType,
			Address:  address,
			Password: os.Getenv(envKey),
		}
	}
	return registry, nil
}

// Has reports whether a profile with the given name exists.
func (r *ProfileRegistry) Has(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.profiles[name]
	return ok
}

// Get returns the profile with the given name.
func (r *ProfileRegistry) Get(name string) (Profile, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.profiles[name]
	return p, ok
}

// Store returns a cached StateStore for the named profile, creating it on
// first use.
func (r *ProfileRegistry) Store(name string) (StateStore, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if store, ok := r.stores[name]; ok {
		return store, nil
	}
	profile, ok := r.profiles[name]
	if !ok {
		return nil, fmt.Errorf("unknown recovery store profile %q", name)
	}
	store := NewRedisStore(RedisOptions{
		Address:  profile.Address,
		Password: profile.Password,
	})
	r.stores[name] = store
	return store, nil
}
