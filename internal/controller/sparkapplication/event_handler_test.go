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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

// TestEventHandlersEnqueueWithoutRateLimiting checks that a burst of informer events queues the
// SparkApplication once and does not raise its rate limiter backoff, which would delay the next reconcile.
func TestEventHandlersEnqueueWithoutRateLimiting(t *testing.T) {
	ctx := context.TODO()
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{Name: "test-app", Namespace: "default"},
	}
	oldPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-app-exec-1",
			Namespace: app.Namespace,
			Labels:    map[string]string{common.LabelSparkAppName: app.Name},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	newPod := oldPod.DeepCopy()
	newPod.Status.Phase = corev1.PodSucceeded
	podHandler := NewSparkPodEventHandler(nil, nil)
	appHandler := NewSparkApplicationEventHandler(nil)

	tests := []struct {
		name   string
		handle func(workqueue.TypedRateLimitingInterface[ctrl.Request])
	}{
		{"pod create", func(q workqueue.TypedRateLimitingInterface[ctrl.Request]) {
			podHandler.Create(ctx, event.CreateEvent{Object: newPod}, q)
		}},
		{"pod update", func(q workqueue.TypedRateLimitingInterface[ctrl.Request]) {
			podHandler.Update(ctx, event.UpdateEvent{ObjectOld: oldPod, ObjectNew: newPod}, q)
		}},
		{"pod delete", func(q workqueue.TypedRateLimitingInterface[ctrl.Request]) {
			podHandler.Delete(ctx, event.DeleteEvent{Object: newPod}, q)
		}},
		{"pod generic", func(q workqueue.TypedRateLimitingInterface[ctrl.Request]) {
			podHandler.Generic(ctx, event.GenericEvent{Object: newPod}, q)
		}},
		{"app create", func(q workqueue.TypedRateLimitingInterface[ctrl.Request]) {
			appHandler.Create(ctx, event.CreateEvent{Object: app}, q)
		}},
		{"app update", func(q workqueue.TypedRateLimitingInterface[ctrl.Request]) {
			appHandler.Update(ctx, event.UpdateEvent{ObjectOld: app, ObjectNew: app}, q)
		}},
		{"app delete", func(q workqueue.TypedRateLimitingInterface[ctrl.Request]) {
			appHandler.Delete(ctx, event.DeleteEvent{Object: app}, q)
		}},
		{"app generic", func(q workqueue.TypedRateLimitingInterface[ctrl.Request]) {
			appHandler.Generic(ctx, event.GenericEvent{Object: app}, q)
		}},
	}

	want := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: app.Namespace, Name: app.Name}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[ctrl.Request]())
			defer queue.ShutDown()

			for range 20 {
				tc.handle(queue)
			}

			if got := queue.NumRequeues(want); got != 0 {
				t.Errorf("NumRequeues() = %d, want 0", got)
			}
			if got := queue.Len(); got != 1 {
				t.Fatalf("Len() = %d, want 1", got)
			}
			if got, _ := queue.Get(); got != want {
				t.Errorf("Get() = %v, want %v", got, want)
			}
		})
	}
}
