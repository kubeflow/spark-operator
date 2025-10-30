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

package kubescheduler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
)

func TestFactoryWithValidConfig(t *testing.T) {
	scheme := newTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	cfg := &Config{SchedulerName: Name, Client: fakeClient}
	sch, err := Factory(cfg)

	require.NoError(t, err)
	assert.Equal(t, Name, sch.Name())
}

func TestFactoryWithInvalidConfig(t *testing.T) {
	_, err := Factory(struct{}{})
	require.Error(t, err)
}

func TestSchedulerName(t *testing.T) {
	sch, _ := newTestScheduler(t)
	assert.Equal(t, Name, sch.Name())
}

func TestShouldScheduleAlwaysTrue(t *testing.T) {
	sch, _ := newTestScheduler(t)
	app := newTestSparkApplication()

	assert.True(t, sch.ShouldSchedule(app))
}

func TestScheduleCreatesPodGroupAndLabelsApp(t *testing.T) {
	sch, cl := newTestScheduler(t)
	app := newTestSparkApplication()

	err := sch.Schedule(app)
	require.NoError(t, err)

	assert.Equal(t, getPodGroupName(app), app.Labels[schedulingv1alpha1.PodGroupLabel])

	created := &schedulingv1alpha1.PodGroup{}
	err = cl.Get(context.Background(), types.NamespacedName{Namespace: app.Namespace, Name: getPodGroupName(app)}, created)
	require.NoError(t, err)

	assert.Equal(t, int32(1), created.Spec.MinMember)
	assertResourceListEqual(t, created.Spec.MinResources, expectedMinResources(app))
	require.Len(t, created.OwnerReferences, 1)
	assert.Equal(t, app.Name, created.OwnerReferences[0].Name)
	assert.NotNil(t, created.OwnerReferences[0].Controller)
	assert.True(t, *created.OwnerReferences[0].Controller)
}

func TestScheduleUpdatesExistingPodGroup(t *testing.T) {
	app := newTestSparkApplication()
	existing := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:            getPodGroupName(app),
			Namespace:       app.Namespace,
			ResourceVersion: "1",
			Labels:          map[string]string{"existing": "label"},
		},
		Spec: schedulingv1alpha1.PodGroupSpec{
			MinMember: 5,
			MinResources: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("100Mi"),
			},
		},
	}

	sch, cl := newTestScheduler(t, existing)
	app.Labels = map[string]string{"preserve": "me"}

	err := sch.Schedule(app)
	require.NoError(t, err)

	updated := &schedulingv1alpha1.PodGroup{}
	err = cl.Get(context.Background(), types.NamespacedName{Namespace: app.Namespace, Name: getPodGroupName(app)}, updated)
	require.NoError(t, err)

	assert.Equal(t, int32(1), updated.Spec.MinMember)
	assertResourceListEqual(t, updated.Spec.MinResources, expectedMinResources(app))
	require.Len(t, updated.OwnerReferences, 1)
	assert.Equal(t, app.Name, updated.OwnerReferences[0].Name)
	assert.Equal(t, "me", app.Labels["preserve"])
	assert.Equal(t, getPodGroupName(app), app.Labels[schedulingv1alpha1.PodGroupLabel])
}

func TestCleanupDeletesPodGroup(t *testing.T) {
	app := newTestSparkApplication()
	existing := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getPodGroupName(app),
			Namespace: app.Namespace,
		},
	}

	sch, cl := newTestScheduler(t, existing)

	err := sch.Cleanup(app)
	require.NoError(t, err)

	err = cl.Get(context.Background(), types.NamespacedName{Namespace: app.Namespace, Name: getPodGroupName(app)}, &schedulingv1alpha1.PodGroup{})
	require.Error(t, err)
	assert.True(t, client.IgnoreNotFound(err) == nil)
}

func TestCleanupIgnoresNotFound(t *testing.T) {
	sch, _ := newTestScheduler(t)
	err := sch.Cleanup(newTestSparkApplication())
	assert.NoError(t, err)
}

func newTestScheduler(t *testing.T, objs ...client.Object) (*Scheduler, client.Client) {
	t.Helper()

	scheme := newTestScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	return &Scheduler{name: Name, client: cl}, cl
}

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, v1beta2.AddToScheme(scheme))
	require.NoError(t, schedulingv1alpha1.AddToScheme(scheme))
	return scheme
}

func newTestSparkApplication() *v1beta2.SparkApplication {
	return &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app",
			Namespace: "test-ns",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Type:         v1beta2.SparkApplicationTypeScala,
			SparkVersion: "3.5.0",
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores:          ptr.To[int32](1),
					Memory:         util.StringPtr("1Gi"),
					MemoryOverhead: util.StringPtr("256Mi"),
				},
			},
			Executor: v1beta2.ExecutorSpec{
				Instances: ptr.To[int32](2),
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores:          ptr.To[int32](1),
					Memory:         util.StringPtr("2Gi"),
					MemoryOverhead: util.StringPtr("512Mi"),
				},
			},
		},
	}
}

func expectedMinResources(app *v1beta2.SparkApplication) corev1.ResourceList {
	return util.SumResourceList([]corev1.ResourceList{util.GetDriverRequestResource(app), util.GetExecutorRequestResource(app)})
}

func assertResourceListEqual(t *testing.T, actual, expected corev1.ResourceList) {
	t.Helper()

	assert.Equal(t, len(expected), len(actual))
	for name, exp := range expected {
		got, ok := actual[name]
		if assert.Truef(t, ok, "missing resource %s", name) {
			assert.Zerof(t, exp.Cmp(got), "resource %s mismatch: want %s, got %s", name, exp.String(), got.String())
		}
	}
}
