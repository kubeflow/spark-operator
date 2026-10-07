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

package scheduledsparkapplication

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

func TestShouldStartNextRun(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := v1beta2.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name      string
		policy    v1beta2.ConcurrencyPolicy
		lastState v1beta2.ApplicationStateType
		want      bool
	}{
		{name: "unset policy defaults to Allow while last run is running", policy: "", lastState: v1beta2.ApplicationStateRunning, want: true},
		{name: "unset policy defaults to Allow after last run completed", policy: "", lastState: v1beta2.ApplicationStateCompleted, want: true},
		{name: "Allow while last run is running", policy: v1beta2.ConcurrencyAllow, lastState: v1beta2.ApplicationStateRunning, want: true},
		{name: "Forbid while last run is running", policy: v1beta2.ConcurrencyForbid, lastState: v1beta2.ApplicationStateRunning, want: false},
		{name: "Forbid after last run completed", policy: v1beta2.ConcurrencyForbid, lastState: v1beta2.ApplicationStateCompleted, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lastRun := &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-scheduled-1",
					Namespace:         "default",
					Labels:            map[string]string{common.LabelScheduledSparkAppName: "test-scheduled"},
					CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour)),
				},
				Status: v1beta2.SparkApplicationStatus{AppState: v1beta2.ApplicationState{State: tt.lastState}},
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(lastRun).Build()
			r := NewReconciler(scheme, c, nil, clock.RealClock{}, Options{})
			scheduledApp := &v1beta2.ScheduledSparkApplication{
				ObjectMeta: metav1.ObjectMeta{Name: "test-scheduled", Namespace: "default"},
				Spec:       v1beta2.ScheduledSparkApplicationSpec{ConcurrencyPolicy: tt.policy},
			}

			got, err := r.shouldStartNextRun(scheduledApp)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("shouldStartNextRun() = %v, want %v", got, tt.want)
			}
		})
	}
}
