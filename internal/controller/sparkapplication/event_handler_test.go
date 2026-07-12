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

package sparkapplication

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

// queueLenEventually polls Len() briefly: AddRateLimited schedules the item
// through the rate limiter's delaying queue rather than adding it
// synchronously, so it is not necessarily visible immediately.
func queueLenEventually(t *testing.T, queue workqueue.TypedRateLimitingInterface[ctrl.Request]) int {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		if l := queue.Len(); l > 0 || time.Now().After(deadline) {
			return l
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func driverPodWithDriverContainerState(state *corev1.ContainerStateTerminated) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "resumable-etl-driver",
			Namespace: "default",
			Labels: map[string]string{
				common.LabelSparkRole:    common.SparkRoleDriver,
				common.LabelSparkAppName: "resumable-etl",
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: common.SparkDriverContainerName,
					State: corev1.ContainerState{
						Terminated: state,
					},
				},
				// A sidecar (e.g. the FencedRestart recovery agent) that
				// keeps running after the driver container exits, holding
				// the pod at PodRunning.
				{Name: "spark-recovery-agent"},
			},
		},
	}
}

// A driver container terminating while a sidecar keeps the pod at
// PodRunning must still enqueue a reconcile: without this, a
// recovery-enabled application's completion is never observed. See
// internal/webhook/recovery_sidecar.go for the sidecar this guards against.
func TestSparkPodEventHandlerUpdateEnqueuesOnDriverContainerTermination(t *testing.T) {
	handler := NewSparkPodEventHandler(nil, nil)
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[ctrl.Request]())

	oldPod := driverPodWithDriverContainerState(nil)
	newPod := driverPodWithDriverContainerState(&corev1.ContainerStateTerminated{ExitCode: 0})

	handler.Update(context.Background(), event.UpdateEvent{ObjectOld: oldPod, ObjectNew: newPod}, queue)

	if got := queueLenEventually(t, queue); got != 1 {
		t.Fatalf("expected the pod update to enqueue a reconcile despite an unchanged pod phase, got queue length %d", got)
	}
}

func TestSparkPodEventHandlerUpdateSkipsWhenNothingRelevantChanged(t *testing.T) {
	handler := NewSparkPodEventHandler(nil, nil)
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[ctrl.Request]())

	oldPod := driverPodWithDriverContainerState(nil)
	newPod := driverPodWithDriverContainerState(nil)

	handler.Update(context.Background(), event.UpdateEvent{ObjectOld: oldPod, ObjectNew: newPod}, queue)

	if got := queue.Len(); got != 0 {
		t.Fatalf("expected no reconcile when neither phase nor driver container state changed, got queue length %d", got)
	}
}
