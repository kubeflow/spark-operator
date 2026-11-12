/*
Copyright 2026 The Kubeflow authors.

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

package validatingwebhookconfiguration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func TestEventHandlerEnqueuesNormalEventsWithoutRateLimiting(t *testing.T) {
	handler := NewEventHandler()
	tests := []struct {
		name   string
		object string
		handle func(workqueue.TypedRateLimitingInterface[ctrl.Request])
	}{
		{
			name:   "create",
			object: "created",
			handle: func(queue workqueue.TypedRateLimitingInterface[ctrl.Request]) {
				handler.Create(context.Background(), event.CreateEvent{Object: &admissionregistrationv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "created"}}}, queue)
			},
		},
		{
			name:   "update",
			object: "updated",
			handle: func(queue workqueue.TypedRateLimitingInterface[ctrl.Request]) {
				handler.Update(context.Background(), event.UpdateEvent{
					ObjectOld: &admissionregistrationv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "updated", ResourceVersion: "1"}},
					ObjectNew: &admissionregistrationv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "updated", ResourceVersion: "2"}},
				}, queue)
			},
		},
		{
			name:   "delete",
			object: "deleted",
			handle: func(queue workqueue.TypedRateLimitingInterface[ctrl.Request]) {
				handler.Delete(context.Background(), event.DeleteEvent{Object: &admissionregistrationv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "deleted"}}}, queue)
			},
		},
		{
			name:   "generic",
			object: "generic",
			handle: func(queue workqueue.TypedRateLimitingInterface[ctrl.Request]) {
				handler.Generic(context.Background(), event.GenericEvent{Object: &admissionregistrationv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "generic"}}}, queue)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			queue := workqueue.NewTypedRateLimitingQueue(
				workqueue.NewTypedItemExponentialFailureRateLimiter[ctrl.Request](time.Millisecond, time.Second),
			)
			defer queue.ShutDown()

			test.handle(queue)
			require.Eventually(t, func() bool { return queue.Len() == 1 }, time.Second, time.Millisecond)

			request := ctrl.Request{NamespacedName: types.NamespacedName{Name: test.object}}
			assert.Zero(t, queue.NumRequeues(request))
			item, shutdown := queue.Get()
			require.False(t, shutdown)
			assert.Equal(t, request, item)
			queue.Done(item)
		})
	}
}
