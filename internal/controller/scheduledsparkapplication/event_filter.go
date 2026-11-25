/*
Copyright 2024 The Kubeflow Authors.

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

package scheduledsparkapplication

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
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

	if !f.filter(newApp) {
		return false
	}

	oldApp, ok := e.ObjectOld.(*v1beta2.ScheduledSparkApplication)
	if ok && newApp.Status.ScheduleState == v1beta2.ScheduleStateScheduled &&
		(oldApp.Spec.Schedule != newApp.Spec.Schedule || oldApp.Spec.TimeZone != newApp.Spec.TimeZone) {
		f.logger.Info("Schedule changed, recomputing next run", "name", newApp.Name, "namespace", newApp.Namespace,
			"oldSchedule", oldApp.Spec.Schedule, "newSchedule", newApp.Spec.Schedule,
			"oldTimeZone", oldApp.Spec.TimeZone, "newTimeZone", newApp.Spec.TimeZone)
		if err := f.resetNextRun(newApp); err != nil {
			f.logger.Error(err, "Failed to reset ScheduledSparkApplication status after schedule change", "name", newApp.Name, "namespace", newApp.Namespace)
		}
	}

	return true
}

// resetNextRun sends a Scheduled app back to New so that status.nextRun is recomputed from the edited schedule.
func (f *EventFilter) resetNextRun(edited *v1beta2.ScheduledSparkApplication) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		app := &v1beta2.ScheduledSparkApplication{}
		if err := f.client.Get(context.TODO(), client.ObjectKeyFromObject(edited), app); err != nil {
			return err
		}
		// A later edit gets its own event, and any other state already recomputes nextRun.
		if app.Status.ScheduleState != v1beta2.ScheduleStateScheduled ||
			app.Spec.Schedule != edited.Spec.Schedule || app.Spec.TimeZone != edited.Spec.TimeZone {
			return nil
		}
		app.Status.ScheduleState = v1beta2.ScheduleStateNew
		app.Status.NextRun = metav1.Time{}
		return f.client.Status().Update(context.TODO(), app)
	})
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
