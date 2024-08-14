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
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/util"
)

// sparkPodEventFilter filters Spark pod events.
type sparkPodEventFilter struct {
	namespaces map[string]bool
}

// sparkPodEventFilter implements the predicate.Predicate interface.
var _ predicate.Predicate = &sparkPodEventFilter{}

// newSparkPodEventFilter creates a new SparkPodEventFilter instance.
func newSparkPodEventFilter(namespaces []string) *sparkPodEventFilter {
	nsMap := make(map[string]bool)
	if len(namespaces) == 0 {
		nsMap[metav1.NamespaceAll] = true
	} else {
		for _, ns := range namespaces {
			nsMap[ns] = true
		}
	}

	return &sparkPodEventFilter{
		namespaces: nsMap,
	}
}

// Create implements predicate.Predicate.
func (f *sparkPodEventFilter) Create(e event.CreateEvent) bool {
	pod, ok := e.Object.(*corev1.Pod)
	if !ok {
		return false
	}

	return f.filter(pod)
}

// Update implements predicate.Predicate.
func (f *sparkPodEventFilter) Update(e event.UpdateEvent) bool {
	oldPod, ok := e.ObjectOld.(*corev1.Pod)
	if !ok {
		return false
	}

	newPod, ok := e.ObjectNew.(*corev1.Pod)
	if !ok {
		return false
	}

	if newPod.Status.Phase == oldPod.Status.Phase {
		return false
	}

	return f.filter(newPod)
}

// Delete implements predicate.Predicate.
func (f *sparkPodEventFilter) Delete(e event.DeleteEvent) bool {
	pod, ok := e.Object.(*corev1.Pod)
	if !ok {
		return false
	}

	return f.filter(pod)
}

// Generic implements predicate.Predicate.
func (f *sparkPodEventFilter) Generic(e event.GenericEvent) bool {
	pod, ok := e.Object.(*corev1.Pod)
	if !ok {
		return false
	}

	return f.filter(pod)
}

func (f *sparkPodEventFilter) filter(pod *corev1.Pod) bool {
	if !util.IsLaunchedBySparkOperator(pod) {
		return false
	}

	return f.namespaces[metav1.NamespaceAll] || f.namespaces[pod.Namespace]
}

type EventFilter struct {
	client     client.Client
	recorder   record.EventRecorder
	namespaces map[string]bool
}

var _ predicate.Predicate = &EventFilter{}

func NewSparkApplicationEventFilter(client client.Client, recorder record.EventRecorder, namespaces []string) *EventFilter {
	nsMap := make(map[string]bool)
	if len(namespaces) == 0 {
		nsMap[metav1.NamespaceAll] = true
	} else {
		for _, ns := range namespaces {
			nsMap[ns] = true
		}
	}

	return &EventFilter{
		client:     client,
		recorder:   recorder,
		namespaces: nsMap,
	}
}

// Create implements predicate.Predicate.
func (f *EventFilter) Create(e event.CreateEvent) bool {
	app, ok := e.Object.(*v1beta2.SparkApplication)
	if !ok {
		return false
	}

	return f.filter(app)
}

// Update implements predicate.Predicate.
func (f *EventFilter) Update(e event.UpdateEvent) bool {
	oldApp, ok := e.ObjectOld.(*v1beta2.SparkApplication)
	if !ok {
		return false
	}

	newApp, ok := e.ObjectNew.(*v1beta2.SparkApplication)
	if !ok {
		return false
	}

	if !f.filter(newApp) {
		return false
	}

	if oldApp.ResourceVersion == newApp.ResourceVersion && !util.IsExpired(newApp) && !util.ShouldRetry(newApp) {
		return false
	}

	// The spec has changed. This is currently best effort as we can potentially miss updates
	// and end up in an inconsistent state.
	if !equality.Semantic.DeepEqual(oldApp.Spec, newApp.Spec) {
		// Force-set the application status to Invalidating which handles clean-up and application re-run.
		newApp.Status.AppState.State = v1beta2.ApplicationStateInvalidating
		logger.Info("Updating SparkApplication status", "name", newApp.Name, "namespace", newApp.Namespace, " oldState", oldApp.Status.AppState.State, "newState", newApp.Status.AppState.State)
		if err := f.client.Status().Update(context.TODO(), newApp); err != nil {
			logger.Error(err, "Failed to update application status", "application", newApp.Name)
			f.recorder.Eventf(
				newApp,
				corev1.EventTypeWarning,
				"SparkApplicationSpecUpdateFailed",
				"Failed to update spec for SparkApplication %s: %v",
				newApp.Name,
				err,
			)
			return false
		}
	}

	return true
}

// Delete implements predicate.Predicate.
func (f *EventFilter) Delete(e event.DeleteEvent) bool {
	app, ok := e.Object.(*v1beta2.SparkApplication)
	if !ok {
		return false
	}

	return f.filter(app)
}

// Generic implements predicate.Predicate.
func (f *EventFilter) Generic(e event.GenericEvent) bool {
	app, ok := e.Object.(*v1beta2.SparkApplication)
	if !ok {
		return false
	}

	return f.filter(app)
}

func (f *EventFilter) filter(app *v1beta2.SparkApplication) bool {
	return f.namespaces[metav1.NamespaceAll] || f.namespaces[app.Namespace]
}
