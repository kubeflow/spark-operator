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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/kubeflow/spark-operator/api/v1beta2"
)

// EventFilter filters out ScheduledSparkApplication events.
type EventFilter struct {
	namespaces map[string]bool
}

// EventHandler handles ScheduledSparkApplication events.
var _ predicate.Predicate = &EventFilter{}

// NewEventFilter creates a new EventFilter instance.
func NewEventFilter(namespaces []string) *EventFilter {
	nsMap := make(map[string]bool)
	if len(namespaces) == 0 {
		nsMap[metav1.NamespaceAll] = true
	} else {
		for _, ns := range namespaces {
			nsMap[ns] = true
		}
	}

	return &EventFilter{
		namespaces: nsMap,
	}
}

// Create implements predicate.Predicate.
func (f *EventFilter) Create(e event.CreateEvent) bool {
	app, ok := e.Object.(*v1beta2.ScheduledSparkApplication)
	if !ok {
		return false
	}
	return f.filter(app)
}

// Update implements predicate.Predicate.
func (f *EventFilter) Update(e event.UpdateEvent) bool {
	newApp, ok := e.ObjectNew.(*v1beta2.ScheduledSparkApplication)
	if !ok {
		return false
	}

	return f.filter(newApp)
}

// Delete implements predicate.Predicate.
func (f *EventFilter) Delete(_ event.DeleteEvent) bool {
	return false
}

// Generic implements predicate.Predicate.
func (f *EventFilter) Generic(e event.GenericEvent) bool {
	app, ok := e.Object.(*v1beta2.ScheduledSparkApplication)
	if !ok {
		return false
	}
	return f.filter(app)
}

func (f *EventFilter) filter(app *v1beta2.ScheduledSparkApplication) bool {
	return f.namespaces[metav1.NamespaceAll] || f.namespaces[app.Namespace]
}
