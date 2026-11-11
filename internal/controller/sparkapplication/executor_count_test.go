/*
Copyright The Kubeflow Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sparkapplication

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

const executorCountSubmissionID = "sub-1"

func executorPodWithPhase(appName string, id int, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-exec-%d", appName, id),
			Namespace: "default",
			Labels: map[string]string{
				common.LabelSparkAppName:    appName,
				common.LabelSparkRole:       common.SparkRoleExecutor,
				common.LabelSparkExecutorID: fmt.Sprintf("%d", id),
				common.LabelSubmissionID:    executorCountSubmissionID,
			},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
}

func readyExecutorPod(appName string, id int) *corev1.Pod {
	pod := executorPodWithPhase(appName, id, corev1.PodRunning)
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	return pod
}

func newExecutorCountReconciler(t *testing.T, maxTracked int, pods ...*corev1.Pod) *Reconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	objs := make([]client.Object, 0, len(pods))
	for _, pod := range pods {
		objs = append(objs, pod)
	}
	return &Reconciler{
		client:   fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build(),
		recorder: events.NewFakeRecorder(100),
		options:  Options{MaxTrackedExecutorPerApp: maxTracked},
	}
}

func runningApp(appName string) *v1beta2.SparkApplication {
	return &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{Name: appName, Namespace: "default"},
		Status: v1beta2.SparkApplicationStatus{
			SubmissionID: executorCountSubmissionID,
			AppState:     v1beta2.ApplicationState{State: v1beta2.ApplicationStateRunning},
		},
	}
}

func TestUpdateExecutorCountsCountsActiveTerminatingAndReadyExecutors(t *testing.T) {
	deleting := readyExecutorPod("app", 3)
	deleting.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
	deleting.Finalizers = []string{"test"} // the fake client keeps an object being deleted only with a finalizer
	notReady := executorPodWithPhase("app", 2, corev1.PodRunning)
	notReady.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
	otherSubmission := readyExecutorPod("app", 8)
	otherSubmission.Name = "app-old-exec-8"
	otherSubmission.Labels[common.LabelSubmissionID] = "sub-0"
	r := newExecutorCountReconciler(t, 1000,
		readyExecutorPod("app", 1),
		notReady,
		deleting,
		executorPodWithPhase("app", 4, corev1.PodPending), // e.g. held by a scheduling gate
		executorPodWithPhase("app", 5, corev1.PodSucceeded),
		executorPodWithPhase("app", 6, corev1.PodFailed),
		otherSubmission,
		readyExecutorPod("other", 9),
	)
	app := runningApp("app")

	require.NoError(t, r.updateExecutorCounts(context.Background(), app))

	assert.Equal(t, &v1beta2.ExecutorStatus{
		SubmissionID:  executorCountSubmissionID,
		Active:        3,
		Terminating:   1,
		Ready:         1,
		MaxExecutorID: 6,
	}, app.Status.Executors)
}

func TestUpdateExecutorCountsDoesNotDependOnExecutorState(t *testing.T) {
	// MaxTrackedExecutorPerApp limits status.executorState only, not the counts
	r := newExecutorCountReconciler(t, 1,
		readyExecutorPod("app", 1),
		readyExecutorPod("app", 2),
		readyExecutorPod("app", 3),
		executorPodWithPhase("app", 4, corev1.PodPending),
	)
	app := runningApp("app")

	require.NoError(t, r.updateExecutorState(context.Background(), app))
	require.NoError(t, r.updateExecutorCounts(context.Background(), app))

	assert.Len(t, app.Status.ExecutorState, 1)
	assert.Equal(t, &v1beta2.ExecutorStatus{
		SubmissionID:  executorCountSubmissionID,
		Active:        4,
		Ready:         3,
		MaxExecutorID: 4,
	}, app.Status.Executors)
}

func TestUpdateExecutorCountsLeavesThemUnsetWithoutExecutors(t *testing.T) {
	r := newExecutorCountReconciler(t, 1000)
	app := runningApp("app")

	require.NoError(t, r.updateExecutorCounts(context.Background(), app))

	assert.Nil(t, app.Status.Executors)
}

func TestUpdateExecutorCountsDropToZeroAndKeepTheHighestExecutorID(t *testing.T) {
	r := newExecutorCountReconciler(t, 1000, readyExecutorPod("app", 2))
	app := runningApp("app")
	app.Status.Executors = &v1beta2.ExecutorStatus{SubmissionID: executorCountSubmissionID, Active: 5, Ready: 4, MaxExecutorID: 7}

	require.NoError(t, r.updateExecutorCounts(context.Background(), app))
	assert.Equal(t, &v1beta2.ExecutorStatus{
		SubmissionID:  executorCountSubmissionID,
		Active:        1,
		Ready:         1,
		MaxExecutorID: 7,
	}, app.Status.Executors)

	// Once every executor is gone, the counts drop to zero, while the highest executor ID is kept
	r = newExecutorCountReconciler(t, 1000)
	require.NoError(t, r.updateExecutorCounts(context.Background(), app))
	assert.Equal(t, &v1beta2.ExecutorStatus{SubmissionID: executorCountSubmissionID, MaxExecutorID: 7}, app.Status.Executors)
}

func TestUpdateExecutorCountsStartOverForANewSubmission(t *testing.T) {
	previous := &v1beta2.ExecutorStatus{SubmissionID: "sub-0", Active: 5, Ready: 4, MaxExecutorID: 9}

	// The counts and the highest executor ID of the previous submission are not carried over
	r := newExecutorCountReconciler(t, 1000, readyExecutorPod("app", 1))
	app := runningApp("app")
	app.Status.Executors = previous.DeepCopy()
	require.NoError(t, r.updateExecutorCounts(context.Background(), app))
	assert.Equal(t, &v1beta2.ExecutorStatus{
		SubmissionID:  executorCountSubmissionID,
		Active:        1,
		Ready:         1,
		MaxExecutorID: 1,
	}, app.Status.Executors)

	// Until the new submission has an executor pod, there are no counts
	r = newExecutorCountReconciler(t, 1000)
	app = runningApp("app")
	app.Status.Executors = previous.DeepCopy()
	require.NoError(t, r.updateExecutorCounts(context.Background(), app))
	assert.Nil(t, app.Status.Executors)
}

// newDeletingExecutorReconciler serves app, its driver pod and one executor pod being deleted,
// as while the executors of a suspended or rerun application are released.
func newDeletingExecutorReconciler(t *testing.T, app *v1beta2.SparkApplication) (*Reconciler, client.Client, *corev1.Pod) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, v1beta2.AddToScheme(scheme))
	driver := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: app.Status.DriverInfo.PodName, Namespace: app.Namespace}}
	executor := readyExecutorPod(app.Name, 2)
	executor.Finalizers = []string{"test"} // the fake client keeps an object being deleted only with a finalizer
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta2.SparkApplication{}).
		WithObjects(app, driver, executor).
		Build()
	require.NoError(t, c.Delete(context.Background(), executor))
	r := &Reconciler{client: c, recorder: events.NewFakeRecorder(100), options: Options{MaxTrackedExecutorPerApp: 1000}}
	return r, c, executor
}

func TestExecutorCountsFollowTheExecutorsWhileTheyAreReleased(t *testing.T) {
	for name, tc := range map[string]struct {
		state     v1beta2.ApplicationStateType
		reconcile func(*Reconciler, context.Context, ctrl.Request) (ctrl.Result, error)
	}{
		"suspended":     {state: v1beta2.ApplicationStateSuspended, reconcile: (*Reconciler).reconcileSuspendedSparkApplication},
		"pending rerun": {state: v1beta2.ApplicationStatePendingRerun, reconcile: (*Reconciler).reconcilePendingRerunSparkApplication},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			app := runningApp("app")
			app.Spec.Suspend = ptr.To(true)
			app.Status.AppState.State = tc.state
			app.Status.DriverInfo.PodName = "app-driver"
			app.Status.Executors = &v1beta2.ExecutorStatus{SubmissionID: executorCountSubmissionID, Active: 3, Ready: 3, MaxExecutorID: 3}
			r, c, executor := newDeletingExecutorReconciler(t, app)
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(app)}

			_, err := tc.reconcile(r, ctx, req)
			require.NoError(t, err)
			got := &v1beta2.SparkApplication{}
			require.NoError(t, c.Get(ctx, req.NamespacedName, got))
			assert.Equal(t, &v1beta2.ExecutorStatus{
				SubmissionID:  executorCountSubmissionID,
				Terminating:   1,
				MaxExecutorID: 3,
			}, got.Status.Executors, "counts while the executor is being deleted")

			// Once the executor is gone, the counts are zero, and the highest executor ID is kept
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(executor), executor))
			executor.Finalizers = nil
			require.NoError(t, c.Update(ctx, executor))
			_, err = tc.reconcile(r, ctx, req)
			require.NoError(t, err)
			require.NoError(t, c.Get(ctx, req.NamespacedName, got))
			assert.Equal(t, &v1beta2.ExecutorStatus{SubmissionID: executorCountSubmissionID, MaxExecutorID: 3}, got.Status.Executors,
				"counts once the executor is gone")
		})
	}
}
