/*
Copyright 2024 The kubeflow authors.

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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
)

// SparkPodEventHandler watches Spark pods and update the SparkApplication objects accordingly.
type SparkPodEventHandler struct {
	cache cache.Cache
}

var _ handler.EventHandler = &SparkPodEventHandler{}

// NewSparkPodEventHandler creates a new sparkPodEventHandler instance.
func NewSparkPodEventHandler(cache cache.Cache) *SparkPodEventHandler {
	handler := &SparkPodEventHandler{
		cache: cache,
	}
	return handler
}

// Create implements handler.EventHandler.
func (h *SparkPodEventHandler) Create(ctx context.Context, event event.CreateEvent, queue workqueue.RateLimitingInterface) {
	pod := event.Object.(*corev1.Pod)
	logger.V(1).Info("Spark pod created", "name", pod.Name, "namespace", pod.Namespace, "phase", pod.Status.Phase)
	h.enqueueSparkAppForUpdate(ctx, pod, queue)
}

// Update implements handler.EventHandler.
func (h *SparkPodEventHandler) Update(ctx context.Context, event event.UpdateEvent, queue workqueue.RateLimitingInterface) {
	oldPod := event.ObjectOld.(*corev1.Pod)
	newPod := event.ObjectNew.(*corev1.Pod)
	if newPod.ResourceVersion == oldPod.ResourceVersion {
		return
	}
	logger.V(1).Info("Spark pod updated", "name", newPod.Name, "namespace", newPod.Namespace, "oldPhase", oldPod.Status.Phase, "newPhase", newPod.Status.Phase)
	h.enqueueSparkAppForUpdate(ctx, newPod, queue)
}

// Delete implements handler.EventHandler.
func (h *SparkPodEventHandler) Delete(ctx context.Context, event event.DeleteEvent, queue workqueue.RateLimitingInterface) {
	pod := event.Object.(*corev1.Pod)
	logger.V(1).Info("Spark pod deleted", "name", pod.Name, "namespace", pod.Namespace, "phase", pod.Status.Phase)
	h.enqueueSparkAppForUpdate(ctx, pod, queue)
}

// Generic implements handler.EventHandler.
func (h *SparkPodEventHandler) Generic(ctx context.Context, event event.GenericEvent, queue workqueue.RateLimitingInterface) {
	pod := event.Object.(*corev1.Pod)
	logger.V(1).Info("Spark pod generic event ", "name", pod.Name, "namespace", pod.Namespace, "phase", pod.Status.Phase)
	h.enqueueSparkAppForUpdate(ctx, pod, queue)
}

func (h *SparkPodEventHandler) enqueueSparkAppForUpdate(ctx context.Context, pod *corev1.Pod, queue workqueue.RateLimitingInterface) {
	name, ok := util.GetAppName(pod)
	if !ok {
		return
	}
	namespace := pod.Namespace
	key := types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}

	app := v1beta2.SparkApplication{}

	if submissionID, ok := pod.Labels[common.LabelSubmissionID]; ok {
		if err := h.cache.Get(ctx, key, &app); err != nil {
			return
		}
		if app.Status.SubmissionID != submissionID {
			return
		}
	}

	queue.AddRateLimited(ctrl.Request{NamespacedName: key})
}

// EventHandler watches SparkApplication events.
type EventHandler struct {
}

var _ handler.EventHandler = &EventHandler{}

// NewSparkApplicationEventHandler creates a new SparkApplicationEventHandler instance.
func NewSparkApplicationEventHandler() *EventHandler {
	return &EventHandler{}
}

// Create implements handler.EventHandler.
func (h *EventHandler) Create(ctx context.Context, event event.CreateEvent, queue workqueue.RateLimitingInterface) {
	app, ok := event.Object.(*v1beta2.SparkApplication)
	if !ok {
		return
	}

	logger.V(1).Info("SparkApplication created", "name", app.Name, "namespace", app.Namespace, "state", app.Status.AppState.State)
	queue.AddRateLimited(ctrl.Request{NamespacedName: types.NamespacedName{Name: app.Name, Namespace: app.Namespace}})
}

// Update implements handler.EventHandler.
func (h *EventHandler) Update(ctx context.Context, event event.UpdateEvent, queue workqueue.RateLimitingInterface) {
	oldApp, ok := event.ObjectOld.(*v1beta2.SparkApplication)
	if !ok {
		return
	}

	newApp, ok := event.ObjectNew.(*v1beta2.SparkApplication)
	if !ok {
		return
	}

	logger.V(1).Info("SparkApplication updated", "name", oldApp.Name, "namespace", oldApp.Namespace, "oldState", oldApp.Status.AppState.State, "newState", newApp.Status.AppState.State)
	queue.AddRateLimited(ctrl.Request{NamespacedName: types.NamespacedName{Name: newApp.Name, Namespace: newApp.Namespace}})
}

// Delete implements handler.EventHandler.
func (h *EventHandler) Delete(ctx context.Context, event event.DeleteEvent, queue workqueue.RateLimitingInterface) {
	app, ok := event.Object.(*v1beta2.SparkApplication)
	if !ok {
		return
	}

	logger.V(1).Info("SparkApplication deleted", "name", app.Name, "namespace", app.Namespace, "state", app.Status.AppState.State)
	queue.AddRateLimited(ctrl.Request{NamespacedName: types.NamespacedName{Name: app.Name, Namespace: app.Namespace}})
}

// Generic implements handler.EventHandler.
func (h *EventHandler) Generic(ctx context.Context, event event.GenericEvent, queue workqueue.RateLimitingInterface) {
	app, ok := event.Object.(*v1beta2.SparkApplication)
	if !ok {
		return
	}

	logger.V(1).Info("SparkApplication generic event", "name", app.Name, "namespace", app.Namespace, "state", app.Status.AppState.State)
	queue.AddRateLimited(ctrl.Request{NamespacedName: types.NamespacedName{Name: app.Name, Namespace: app.Namespace}})
}
