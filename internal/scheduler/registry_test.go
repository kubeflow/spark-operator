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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

type fakeScheduler struct {
	name string
}

func (f *fakeScheduler) Name() string                                      { return f.name }
func (f *fakeScheduler) ShouldSchedule(app *v1beta2.SparkApplication) bool { return true }
func (f *fakeScheduler) Schedule(app *v1beta2.SparkApplication) error      { return nil }
func (f *fakeScheduler) Cleanup(app *v1beta2.SparkApplication) error       { return nil }

func newFakeFactory(name string) Factory {
	return func(config Config) (Interface, error) {
		return &fakeScheduler{name: name}, nil
	}
}

func newTestRegistry() *Registry {
	return &Registry{factories: make(map[string]Factory)}
}

func TestRegistryRegisterAndGetScheduler(t *testing.T) {
	r := newTestRegistry()

	err := r.Register("foo", newFakeFactory("foo"))
	require.NoError(t, err)

	sched, err := r.GetScheduler("foo", nil)
	require.NoError(t, err)
	assert.Equal(t, "foo", sched.Name())
}

func TestRegistryRegisterDuplicateNameErrors(t *testing.T) {
	r := newTestRegistry()

	require.NoError(t, r.Register("foo", newFakeFactory("foo")))
	err := r.Register("foo", newFakeFactory("foo-again"))
	require.Error(t, err)

	sched, err := r.GetScheduler("foo", nil)
	require.NoError(t, err)
	assert.Equal(t, "foo", sched.Name(), "the original factory must not be overwritten")
}

func TestRegistryGetSchedulerNotFoundErrors(t *testing.T) {
	r := newTestRegistry()

	_, err := r.GetScheduler("missing", nil)
	require.Error(t, err)
}

func TestRegistryGetRegisteredSchedulerNames(t *testing.T) {
	r := newTestRegistry()

	assert.Empty(t, r.GetRegisteredSchedulerNames())

	require.NoError(t, r.Register("foo", newFakeFactory("foo")))
	require.NoError(t, r.Register("bar", newFakeFactory("bar")))

	assert.ElementsMatch(t, []string{"foo", "bar"}, r.GetRegisteredSchedulerNames())
}

func TestGetRegistryReturnsSingleton(t *testing.T) {
	first := GetRegistry()
	require.NotNil(t, first)

	second := GetRegistry()
	assert.Same(t, first, second)
}
