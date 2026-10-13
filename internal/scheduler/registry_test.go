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

package scheduler

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

// fakeScheduler is a minimal Interface implementation used to exercise the registry.
type fakeScheduler struct {
	name string
}

func (f *fakeScheduler) Name() string {
	return f.name
}

func (f *fakeScheduler) ShouldSchedule(_ *v1beta2.SparkApplication) bool {
	return true
}

func (f *fakeScheduler) Schedule(_ *v1beta2.SparkApplication) error {
	return nil
}

func (f *fakeScheduler) Cleanup(_ *v1beta2.SparkApplication) error {
	return nil
}

func newFakeFactory(name string) Factory {
	return func(_ Config) (Interface, error) {
		return &fakeScheduler{name: name}, nil
	}
}

// newTestRegistry returns an isolated registry so tests never touch the package
// level singleton.
func newTestRegistry() *Registry {
	return &Registry{factories: make(map[string]Factory)}
}

func TestGetRegistry(t *testing.T) {
	original := registry
	t.Cleanup(func() { registry = original })

	registry = nil

	first := GetRegistry()
	require.NotNil(t, first)
	require.NotNil(t, first.factories)
	assert.Empty(t, first.factories)

	second := GetRegistry()
	assert.Same(t, first, second)
}

func TestRegistryRegister(t *testing.T) {
	r := newTestRegistry()

	require.NoError(t, r.Register("volcano", newFakeFactory("volcano")))
	require.NoError(t, r.Register("yunikorn", newFakeFactory("yunikorn")))
	assert.Len(t, r.factories, 2)
}

func TestRegistryRegisterDuplicate(t *testing.T) {
	r := newTestRegistry()

	require.NoError(t, r.Register("volcano", newFakeFactory("volcano")))

	err := r.Register("volcano", newFakeFactory("volcano"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already registered")

	// The original registration is left intact.
	assert.Len(t, r.factories, 1)
}

func TestRegistryGetScheduler(t *testing.T) {
	r := newTestRegistry()
	require.NoError(t, r.Register("volcano", newFakeFactory("volcano")))

	scheduler, err := r.GetScheduler("volcano", nil)
	require.NoError(t, err)
	require.NotNil(t, scheduler)
	assert.Equal(t, "volcano", scheduler.Name())
}

func TestRegistryGetSchedulerNotFound(t *testing.T) {
	r := newTestRegistry()
	require.NoError(t, r.Register("volcano", newFakeFactory("volcano")))

	scheduler, err := r.GetScheduler("yunikorn", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
	assert.Nil(t, scheduler)
}

func TestRegistryGetSchedulerFactoryError(t *testing.T) {
	r := newTestRegistry()
	factoryErr := errors.New("failed to create scheduler")

	require.NoError(t, r.Register("broken", func(_ Config) (Interface, error) {
		return nil, factoryErr
	}))

	scheduler, err := r.GetScheduler("broken", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, factoryErr)
	assert.Nil(t, scheduler)
}

func TestRegistryGetSchedulerPassesConfig(t *testing.T) {
	type testConfig struct {
		value string
	}

	r := newTestRegistry()
	want := &testConfig{value: "expected"}

	var got Config
	require.NoError(t, r.Register("config-capture", func(config Config) (Interface, error) {
		got = config
		return &fakeScheduler{name: "config-capture"}, nil
	}))

	_, err := r.GetScheduler("config-capture", want)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestRegistryGetRegisteredSchedulerNames(t *testing.T) {
	r := newTestRegistry()
	assert.Empty(t, r.GetRegisteredSchedulerNames())

	names := []string{"volcano", "yunikorn", "kube-scheduler"}
	for _, name := range names {
		require.NoError(t, r.Register(name, newFakeFactory(name)))
	}

	assert.ElementsMatch(t, names, r.GetRegisteredSchedulerNames())
}
