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

package scheduledsparkapplication

import (
	"context"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	"github.com/kubeflow/spark-operator/api/v1beta2"
)

// EventHandler handles events for ScheduledSparkApplication.
type EventHandler struct {
}

// EventHandler implements handler.EventHandler.
var _ handler.EventHandler = &EventHandler{}

// NewEventHandler creates a new EventHandler instance
func NewEventHandler() *EventHandler {
	return &EventHandler{}
}

// Create implements handler.EventHandler.
func (s *EventHandler) Create(ctx context.Context, event event.CreateEvent, queue workqueue.TypedRateLimitingInterface[ctrl.Request]) {
	app, ok := event.Object.(*v1beta2.ScheduledSparkApplication)
	if !ok {
		return
	}

	logger.V(1).Info("ScheduledSparkApplication created", "name", app.Name, "namespace", app.Namespace)
	queue.AddRateLimited(ctrl.Request{NamespacedName: types.NamespacedName{Name: app.Name, Namespace: app.Namespace}})
}

// Update implements handler.EventHandler.
func (s *EventHandler) Update(ctx context.Context, event event.UpdateEvent, queue workqueue.TypedRateLimitingInterface[ctrl.Request]) {
	oldApp, ok := event.ObjectOld.(*v1beta2.ScheduledSparkApplication)
	if !ok {
		return
	}

	logger.V(1).Info("ScheduledSparkApplication updated", "name", oldApp.Name, "namespace", oldApp.Namespace)
	queue.AddRateLimited(ctrl.Request{NamespacedName: types.NamespacedName{Name: oldApp.Name, Namespace: oldApp.Namespace}})
}

// Delete implements handler.EventHandler.
func (s *EventHandler) Delete(ctx context.Context, event event.DeleteEvent, queue workqueue.TypedRateLimitingInterface[ctrl.Request]) {
	app, ok := event.Object.(*v1beta2.ScheduledSparkApplication)
	if !ok {
		return
	}

	logger.V(1).Info("ScheduledSparkApplication deleted", "name", app.Name, "namespace", app.Namespace)
	queue.AddRateLimited(ctrl.Request{NamespacedName: types.NamespacedName{Name: app.Name, Namespace: app.Namespace}})
}

// Generic implements handler.EventHandler.
func (s *EventHandler) Generic(ctx context.Context, event event.GenericEvent, queue workqueue.TypedRateLimitingInterface[ctrl.Request]) {
	app, ok := event.Object.(*v1beta2.ScheduledSparkApplication)
	if !ok {
		return
	}

	logger.V(1).Info("ScheduledSparkApplication generic event", "name", app.Name, "namespace", app.Namespace)
	queue.AddRateLimited(ctrl.Request{NamespacedName: types.NamespacedName{Name: app.Name, Namespace: app.Namespace}})
}
