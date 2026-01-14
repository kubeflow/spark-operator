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
	"reflect"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/go-logr/logr"
	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/features"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// sparkPodEventFilter filters Spark pod events.
type sparkPodEventFilter struct {
	client           client.Client
	namespaceMatcher *util.NamespaceMatcher
	logger           logr.Logger
}

// sparkPodEventFilter implements the predicate.Predicate interface.
var _ predicate.Predicate = &sparkPodEventFilter{}

// newSparkPodEventFilter creates a new SparkPodEventFilter instance.
func newSparkPodEventFilter(client client.Client, namespaces []string, namespaceSelector string) (*sparkPodEventFilter, error) {
	matcher, err := util.NewNamespaceMatcher(namespaces, namespaceSelector)
	if err != nil {
		return nil, err
	}

	return &sparkPodEventFilter{
		client:           client,
		namespaceMatcher: matcher,
		logger:           log.Log.WithName("spark-pod-event-filter"),
	}, nil
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

	// Check if namespace matches using the matcher
	matched, err := f.namespaceMatcher.MatchesWithClient(context.TODO(), f.client, pod.Namespace)
	if err != nil {
		f.logger.Error(err, "failed to check namespace match", "namespace", pod.Namespace, "pod", pod.Name)
		return false
	}

	return matched
}

type EventFilter struct {
	client           client.Client
	recorder         record.EventRecorder
	namespaceMatcher *util.NamespaceMatcher
	logger           logr.Logger
}

var _ predicate.Predicate = &EventFilter{}

func NewSparkApplicationEventFilter(client client.Client, recorder record.EventRecorder, namespaces []string, namespaceSelector string) (*EventFilter, error) {
	matcher, err := util.NewNamespaceMatcher(namespaces, namespaceSelector)
	if err != nil {
		return nil, err
	}

	return &EventFilter{
		client:           client,
		recorder:         recorder,
		namespaceMatcher: matcher,
		logger:           log.Log.WithName("spark-application-event-filter"),
	}, nil
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

	// The spec has changed except for Spec.Suspend.
	// This is currently best effort as we can potentially miss updates and end up in an inconsistent state.
	if !equality.Semantic.DeepEqual(oldApp.Spec, newApp.Spec) {

		// Only Spec.Suspend can be updated without any action
		oldAppCopy := oldApp.DeepCopy()
		oldAppCopy.Spec.Suspend = newApp.Spec.Suspend
		if equality.Semantic.DeepEqual(oldAppCopy.Spec, newApp.Spec) {
			return true
		}

		// Check if only webhook-patched fields changed (requires PartialRestart feature gate).
		// These fields are applied by the mutating webhook when new pods are created,
		// so we don't need to trigger a reconcile - the webhook cache will automatically
		// use the new values for any newly created pods.
		if features.Enabled(features.PartialRestart) && f.isWebhookPatchedFieldsOnlyChange(oldApp, newApp) {
			f.logger.Info("Only webhook-patched fields changed, skipping reconcile",
				"name", newApp.Name, "namespace", newApp.Namespace)
			f.recorder.Eventf(
				newApp,
				corev1.EventTypeNormal,
				"SparkApplicationWebhookFieldsUpdated",
				"SparkApplication %s webhook-patched fields updated, new pods will use updated values",
				newApp.Name,
			)
			return false
		}

		// Force-set the application status to Invalidating which handles clean-up and application re-run.
		newApp.Status.AppState.State = v1beta2.ApplicationStateInvalidating
		f.logger.Info("Updating SparkApplication status", "name", newApp.Name, "namespace", newApp.Namespace, " oldState", oldApp.Status.AppState.State, "newState", newApp.Status.AppState.State)
		if err := f.client.Status().Update(context.TODO(), newApp); err != nil {
			f.logger.Error(err, "Failed to update application status", "application", newApp.Name)
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
	// Check if namespace matches using the matcher
	matched, err := f.namespaceMatcher.MatchesWithClient(context.TODO(), f.client, app.Namespace)
	if err != nil {
		f.logger.Error(err, "failed to check namespace match", "namespace", app.Namespace, "app", app.Name)
		return false
	}

	return matched
}

// isWebhookPatchedFieldsOnlyChange checks if the spec changes only involve fields
// that are patched by the mutating webhook when pods are created.
// These fields don't require a full application restart because:
// 1. They don't affect already running pods
// 2. The webhook will automatically apply the new values to any newly created pods
//
// Currently supported webhook-patched fields for Executor:
// - PriorityClassName
// - NodeSelector
// - Tolerations
// - Affinity
// - SchedulerName
//
// Note: Driver field changes still require full restart since the driver pod
// is not recreated during the application lifecycle.
func (f *EventFilter) isWebhookPatchedFieldsOnlyChange(oldApp, newApp *v1beta2.SparkApplication) bool {
	// First check if there are any webhook-patched field changes
	if !hasExecutorWebhookFieldChanges(oldApp, newApp) {
		// No webhook fields changed, so this is not a "webhook-only" change
		return false
	}

	// Create copies to compare non-webhook fields
	oldCopy := oldApp.DeepCopy()
	newCopy := newApp.DeepCopy()

	// Zero out webhook-patched executor fields in both copies
	clearWebhookPatchedExecutorFields(&oldCopy.Spec.Executor)
	clearWebhookPatchedExecutorFields(&newCopy.Spec.Executor)

	// Also zero out Suspend field as it's handled separately
	oldCopy.Spec.Suspend = nil
	newCopy.Spec.Suspend = nil

	// If specs are equal after clearing webhook-patched fields,
	// then only webhook-patched fields changed
	return equality.Semantic.DeepEqual(oldCopy.Spec, newCopy.Spec)
}

// clearWebhookPatchedExecutorFields zeros out the executor fields that are
// patched by the mutating webhook.
func clearWebhookPatchedExecutorFields(executor *v1beta2.ExecutorSpec) {
	executor.PriorityClassName = nil
	executor.NodeSelector = nil
	executor.Tolerations = nil
	executor.Affinity = nil
	executor.SchedulerName = nil
}

// hasExecutorWebhookFieldChanges checks if any webhook-patched executor fields changed.
// This is useful for logging which fields triggered the skip.
func hasExecutorWebhookFieldChanges(oldApp, newApp *v1beta2.SparkApplication) bool {
	oldExec := &oldApp.Spec.Executor
	newExec := &newApp.Spec.Executor

	if !reflect.DeepEqual(oldExec.PriorityClassName, newExec.PriorityClassName) {
		return true
	}
	if !reflect.DeepEqual(oldExec.NodeSelector, newExec.NodeSelector) {
		return true
	}
	if !reflect.DeepEqual(oldExec.Tolerations, newExec.Tolerations) {
		return true
	}
	if !reflect.DeepEqual(oldExec.Affinity, newExec.Affinity) {
		return true
	}
	if !reflect.DeepEqual(oldExec.SchedulerName, newExec.SchedulerName) {
		return true
	}
	return false
}
