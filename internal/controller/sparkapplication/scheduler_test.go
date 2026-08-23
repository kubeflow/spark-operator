/*
Copyright 2024 The Kubeflow authors.

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

package sparkapplication

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/internal/scheduler"
	"github.com/kubeflow/spark-operator/v2/internal/scheduler/workload"
)

func TestShouldDoBatchScheduling(t *testing.T) {
	ctx := context.Background()
	// Create test app
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "default",
		},
		Spec: v1beta2.SparkApplicationSpec{
			BatchScheduler: ptr.To(workload.SchedulerName),
		},
	}

	t.Run("workload scheduler selected when registered", func(t *testing.T) {
		// Create an isolated registry for this test
		testRegistry := scheduler.NewRegistry()
		err := testRegistry.Register(workload.SchedulerName, func(config scheduler.Config) (scheduler.Interface, error) {
			return &mockScheduler{name: workload.SchedulerName}, nil
		})
		require.NoError(t, err)
		reconciler := &Reconciler{
			manager:  &fakeManager{},
			registry: testRegistry,
			options: Options{
				DefaultBatchScheduler: "",
			},
		}
		needScheduling, sched, err := reconciler.shouldDoBatchScheduling(ctx, app)
		assert.NoError(t, err)
		assert.True(t, needScheduling)
		assert.NotNil(t, sched)
		assert.Equal(t, workload.SchedulerName, sched.Name())
	})

	t.Run("no registry returns false", func(t *testing.T) {
		reconciler := &Reconciler{
			registry: nil,
		}
		needScheduling, sched, err := reconciler.shouldDoBatchScheduling(ctx, app)
		assert.NoError(t, err)
		assert.False(t, needScheduling)
		assert.Nil(t, sched)
	})

	t.Run("unknown scheduler returns error", func(t *testing.T) {
		testRegistry := scheduler.NewRegistry()
		app := &v1beta2.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "default",
			},
			Spec: v1beta2.SparkApplicationSpec{
				BatchScheduler: ptr.To("unknown-scheduler"),
			},
		}
		reconciler := &Reconciler{
			registry: testRegistry,
			options:  Options{},
		}
		needScheduling, sched, err := reconciler.shouldDoBatchScheduling(ctx, app)
		assert.Error(t, err)
		assert.False(t, needScheduling)
		assert.Nil(t, sched)
	})

	t.Run("empty scheduler name returns false", func(t *testing.T) {
		testRegistry := scheduler.NewRegistry()
		app := &v1beta2.SparkApplication{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-app",
				Namespace: "default",
			},
			Spec: v1beta2.SparkApplicationSpec{
				BatchScheduler: ptr.To(""),
			},
		}
		reconciler := &Reconciler{
			registry: testRegistry,
			options: Options{
				DefaultBatchScheduler: "",
			},
		}
		needScheduling, sched, err := reconciler.shouldDoBatchScheduling(ctx, app)
		assert.NoError(t, err)
		assert.False(t, needScheduling)
		assert.Nil(t, sched)
	})
}

// mockScheduler is a simple mock for testing
type mockScheduler struct {
	name string
}

func (m *mockScheduler) Name() string {
	return m.name
}

func (m *mockScheduler) ShouldSchedule(_ *v1beta2.SparkApplication) bool {
	return true
}

func (m *mockScheduler) Schedule(_ *v1beta2.SparkApplication) error {
	return nil
}

func (m *mockScheduler) Cleanup(_ *v1beta2.SparkApplication) error {
	return nil
}

// fakeManager implements the minimal Manager interface needed for testing
type fakeManager struct {
	ctrl.Manager
}

func (f *fakeManager) GetConfig() *rest.Config {
	return &rest.Config{}
}

func (f *fakeManager) GetClient() client.Client {
	return nil
}
