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

package mutatingwebhookconfiguration

import (
	"context"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
)

// EventHandler handles MutatingWebhookConfiguration events.
type EventHandler struct{}

var _ handler.EventHandler = &EventHandler{}

// NewEventHandler creates a new MutatingWebhookConfigurationEventHandler instance.
func NewEventHandler() *EventHandler {
	return &EventHandler{}
}

// Create implements handler.EventHandler.
func (h *EventHandler) Create(ctx context.Context, event event.CreateEvent, queue workqueue.TypedRateLimitingInterface[ctrl.Request]) {
	mwc, ok := event.Object.(*admissionregistrationv1.MutatingWebhookConfiguration)
	if !ok {
		return
	}
	logger.Info("MutatingWebhookConfiguration created", "name", mwc.Name)
	key := types.NamespacedName{
		Namespace: mwc.Namespace,
		Name:      mwc.Name,
	}
	queue.AddRateLimited(ctrl.Request{NamespacedName: key})
}

// Update implements handler.EventHandler.
func (h *EventHandler) Update(ctx context.Context, event event.UpdateEvent, queue workqueue.TypedRateLimitingInterface[ctrl.Request]) {
	oldWebhook, ok := event.ObjectOld.(*admissionregistrationv1.MutatingWebhookConfiguration)
	if !ok {
		return
	}
	newWebhook, ok := event.ObjectNew.(*admissionregistrationv1.MutatingWebhookConfiguration)
	if !ok {
		return
	}
	if newWebhook.ResourceVersion == oldWebhook.ResourceVersion {
		return
	}

	logger.Info("MutatingWebhookConfiguration updated", "name", newWebhook.Name, "namespace", newWebhook.Namespace)
	key := types.NamespacedName{
		Namespace: newWebhook.Namespace,
		Name:      newWebhook.Name,
	}
	queue.AddRateLimited(ctrl.Request{NamespacedName: key})
}

// Delete implements handler.EventHandler.
func (h *EventHandler) Delete(ctx context.Context, event event.DeleteEvent, queue workqueue.TypedRateLimitingInterface[ctrl.Request]) {
	mwc, ok := event.Object.(*admissionregistrationv1.MutatingWebhookConfiguration)
	if !ok {
		return
	}
	logger.Info("MutatingWebhookConfiguration deleted", "name", mwc.Name, "namespace", mwc.Namespace)
	key := types.NamespacedName{
		Namespace: mwc.Namespace,
		Name:      mwc.Name,
	}
	queue.AddRateLimited(ctrl.Request{NamespacedName: key})
}

// Generic implements handler.EventHandler.
func (h *EventHandler) Generic(ctx context.Context, event event.GenericEvent, queue workqueue.TypedRateLimitingInterface[ctrl.Request]) {
	mwc, ok := event.Object.(*admissionregistrationv1.MutatingWebhookConfiguration)
	if !ok {
		return
	}
	logger.Info("MutatingWebhookConfiguration generic event", "name", mwc.Name, "namespace", mwc.Namespace)
	key := types.NamespacedName{
		Namespace: mwc.Namespace,
		Name:      mwc.Name,
	}
	queue.AddRateLimited(ctrl.Request{NamespacedName: key})
}
