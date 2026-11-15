/*
Copyright 2026 The Kubeflow Authors.

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
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

func newEventTestApp(state v1beta2.ApplicationStateType) *v1beta2.SparkApplication {
	return &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{Name: "spark-pi", Namespace: "default"},
		Status: v1beta2.SparkApplicationStatus{
			AppState: v1beta2.ApplicationState{State: state, ErrorMessage: "boom"},
		},
	}
}

// FakeRecorder renders "<type> <reason> " + fmt.Sprintf(note, args...), the same note formatting as the real recorder.
func recordedEvent(t *testing.T, rec *events.FakeRecorder) string {
	t.Helper()
	select {
	case e := <-rec.Events:
		return e
	default:
		t.Fatal("no event recorded")
		return ""
	}
}

func TestRecordSparkApplicationEvent(t *testing.T) {
	tests := []struct {
		state v1beta2.ApplicationStateType
		want  string
	}{
		{v1beta2.ApplicationStateNew, "Normal SparkApplicationAdded SparkApplication spark-pi was added, enqueuing it for submission"},
		{v1beta2.ApplicationStateSubmitted, "Normal SparkApplicationSubmitted SparkApplication spark-pi was submitted successfully"},
		{v1beta2.ApplicationStateFailedSubmission, "Warning SparkApplicationSubmissionFailed failed to submit SparkApplication spark-pi: boom"},
		{v1beta2.ApplicationStateCompleted, "Normal SparkApplicationCompleted SparkApplication spark-pi completed"},
		{v1beta2.ApplicationStateFailed, "Warning SparkApplicationFailed SparkApplication spark-pi failed: boom"},
		{v1beta2.ApplicationStatePendingRerun, "Warning SparkApplicationPendingRerun SparkApplication spark-pi is pending rerun"},
		{v1beta2.ApplicationStateSuspending, "Warning SparkApplicationSuspending SparkApplication spark-pi is suspending"},
		{v1beta2.ApplicationStateSuspended, "Warning SparkApplicationSuspended SparkApplication spark-pi is suspended"},
		{v1beta2.ApplicationStateResuming, "Warning SparkApplicationResuming SparkApplication spark-pi is resuming"},
	}
	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			rec := events.NewFakeRecorder(1)
			r := &Reconciler{recorder: rec}
			r.recordSparkApplicationEvent(newEventTestApp(tt.state))
			assert.Equal(t, tt.want, recordedEvent(t, rec))
		})
	}
}

func TestRecordDriverEvent(t *testing.T) {
	tests := []struct {
		state v1beta2.DriverState
		want  string
	}{
		{v1beta2.DriverStatePending, "Normal SparkDriverPending Driver spark-pi-driver is pending"},
		{v1beta2.DriverStateRunning, "Normal SparkDriverRunning Driver spark-pi-driver is running"},
		{v1beta2.DriverStateCompleted, "Normal SparkDriverCompleted Driver spark-pi-driver completed"},
		{v1beta2.DriverStateFailed, "Warning SparkDriverFailed Driver spark-pi-driver failed"},
		{v1beta2.DriverStateUnknown, "Warning SparkDriverUnknown Driver spark-pi-driver in unknown state"},
	}
	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			rec := events.NewFakeRecorder(1)
			r := &Reconciler{recorder: rec}
			r.recordDriverEvent(newEventTestApp(v1beta2.ApplicationStateRunning), tt.state, "spark-pi-driver")
			assert.Equal(t, tt.want, recordedEvent(t, rec))
		})
	}
}

func TestRecordExecutorEvent(t *testing.T) {
	tests := []struct {
		state v1beta2.ExecutorState
		args  []any
		want  string
	}{
		{v1beta2.ExecutorStatePending, []any{"spark-pi-exec-1"}, "Normal SparkExecutorPending Executor spark-pi-exec-1 is pending"},
		{v1beta2.ExecutorStateRunning, []any{"spark-pi-exec-1"}, "Normal SparkExecutorRunning Executor spark-pi-exec-1 is running"},
		{v1beta2.ExecutorStateCompleted, []any{"spark-pi-exec-1"}, "Normal SparkExecutorCompleted Executor spark-pi-exec-1 completed"},
		{
			v1beta2.ExecutorStateFailed,
			[]any{"spark-pi-exec-1", int32(137), "OOMKilled", "evicted"},
			"Warning SparkExecutorFailed Executor spark-pi-exec-1 failed with ExitCode: 137, Reason: OOMKilled, Pod Message: evicted",
		},
		{v1beta2.ExecutorStateUnknown, []any{"spark-pi-exec-1"}, "Warning SparkExecutorUnknown Executor spark-pi-exec-1 in unknown state"},
	}
	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			rec := events.NewFakeRecorder(1)
			r := &Reconciler{recorder: rec}
			r.recordExecutorEvent(newEventTestApp(v1beta2.ApplicationStateRunning), tt.state, tt.args...)
			assert.Equal(t, tt.want, recordedEvent(t, rec))
		})
	}
}
