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

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/go-logr/logr"
	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// EventFilter filters out ScheduledSparkApplication events.
type EventFilter struct {
	client           client.Client
	namespaceMatcher *util.NamespaceMatcher
	logger           logr.Logger
}

// EventHandler handles ScheduledSparkApplication events.
var _ predicate.Predicate = &EventFilter{}

// NewEventFilter creates a new EventFilter instance.
func NewEventFilter(client client.Client, namespaces []string, namespaceSelector string) (*EventFilter, error) {
	matcher, err := util.NewNamespaceMatcher(namespaces, namespaceSelector)
	if err != nil {
		return nil, err
	}

	return &EventFilter{
		client:           client,
		namespaceMatcher: matcher,
		logger:           log.Log.WithName("scheduled-spark-application-event-filter"),
	}, nil
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
	// Check if namespace matches using the matcher
	matched, err := f.namespaceMatcher.MatchesWithClient(context.TODO(), f.client, app.Namespace)
	if err != nil {
		f.logger.Error(err, "failed to check namespace match", "namespace", app.Namespace, "app", app.Name)
		return false
	}

	return matched
}
