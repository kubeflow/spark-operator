/*
Copyright 2018 Google LLC

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
	"fmt"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta1"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/util"
	"github.com/golang/glog"
	"github.com/lyft/flytestdlib/contextutils"
	"github.com/robfig/cron"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/client-go/util/retry"
	"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sort"
	"time"
)

var logger = ctrl.Log.WithName("scheduledsparkapp-controller")

// Add creates a new ScheduledSparkApplication Controller and adds it to the Manager with default RBAC. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager, metricsConfig *util.MetricConfig) error {
	return add(mgr, newReconciler(mgr, metricsConfig))
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	return nil
}

func newReconciler(mgr manager.Manager, metricsConfig *util.MetricConfig) reconcile.Reconciler {
	return &ReconcileScheduledSparkApplication{}
}

type ReconcileScheduledSparkApplication struct {
	client           client.Client
	scheme           *runtime.Scheme
	extensionsClient apiextensionsclient.Interface
	clock            clock.Clock
}

func (r *ReconcileScheduledSparkApplication) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	// Fetch the ScheduledSparkApplication
	ctx := context.Background()
	ctx = contextutils.WithNamespace(ctx, request.Namespace)
	ctx = contextutils.WithAppName(ctx, request.Name)
	ssparkapp := &v1beta1.ScheduledSparkApplication{}
	err := r.client.Get(ctx, request.NamespacedName, ssparkapp)
	if err != nil {
		if errors.IsNotFound(err) {
			// Object not found, return. Created objects are automatically garbage collected.
			// For additional cleanup logic use finalizers.
			logger.Error(err, "Scheduled spark application object not found", "object name", request.NamespacedName)
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		logger.Error(err, "Error reading scheduled spark application", "object name", request.NamespacedName)
		return reconcile.Result{}, err
	}

	if ssparkapp.Spec.Suspend != nil && *ssparkapp.Spec.Suspend {
		return reconcile.Result{}, nil
	}

	status := ssparkapp.Status.DeepCopy()

	schedule, err := cron.ParseStandard(ssparkapp.Spec.Schedule)
	if err != nil {
		logger.Error(err, "failed to parse schedule of ScheduledSparkApplication",
			"schedule", ssparkapp.Spec.Schedule, "namespace", ssparkapp.Namespace, "name", ssparkapp.Name)
		status.ScheduleState = v1beta1.FailedValidationState
		status.Reason = err.Error()
	} else {
		status.ScheduleState = v1beta1.ScheduledState
		now := r.clock.Now()
		nextRunTime := status.NextRun.Time
		if nextRunTime.IsZero() {
			// The first run of the application.
			nextRunTime = schedule.Next(now)
			status.NextRun = metav1.NewTime(nextRunTime)
		}
		if nextRunTime.Before(now) {
			// Check if the condition for starting the next run is satisfied.
			ok, err := r.shouldStartNextRun(ctx, ssparkapp)
			if err != nil {
				return reconcile.Result{}, err
			}
			if ok {
				logger.Info("Next run of ScheduledSparkApplication is due, creating a new SparkApplication instance",
					"namespace", ssparkapp.Namespace, "name", ssparkapp.Name)
				name, err := r.startNextRun(ctx, ssparkapp, now)
				if err != nil {
					return reconcile.Result{}, err
				}
				status.LastRun = metav1.NewTime(now)
				status.NextRun = metav1.NewTime(schedule.Next(status.LastRun.Time))
				status.LastRunName = name
			}
		}

		if err = r.checkAndUpdatePastRuns(ctx, ssparkapp, status); err != nil {
			return reconcile.Result{}, err
		}
	}

	return r.updateScheduledSparkApplicationStatus(ctx, ssparkapp, status)
}

var _ reconcile.Reconciler = &ReconcileScheduledSparkApplication{}

func (r *ReconcileScheduledSparkApplication) createSparkApplication(
	ctx context.Context, scheduledApp *v1beta1.ScheduledSparkApplication, t time.Time) (string, error) {
	app := &v1beta1.SparkApplication{}
	app.Spec = scheduledApp.Spec.Template
	app.Name = fmt.Sprintf("%s-%d", scheduledApp.Name, t.UnixNano())
	app.OwnerReferences = append(app.OwnerReferences, metav1.OwnerReference{
		APIVersion: v1beta1.SchemeGroupVersion.String(),
		Kind:       reflect.TypeOf(v1beta1.ScheduledSparkApplication{}).Name(),
		Name:       scheduledApp.Name,
		UID:        scheduledApp.UID,
	})
	app.ObjectMeta.Labels = make(map[string]string)
	for key, value := range scheduledApp.Labels {
		app.ObjectMeta.Labels[key] = value
	}
	app.ObjectMeta.Labels[config.ScheduledSparkAppNameLabel] = scheduledApp.Name

	err := r.client.Create(ctx, scheduledApp)
	if err != nil {
		return "", err
	}
	return app.Name, nil
}

func (r *ReconcileScheduledSparkApplication) shouldStartNextRun(ctx context.Context, app *v1beta1.ScheduledSparkApplication) (bool, error) {
	sortedApps, err := r.listSparkApplications(ctx, app)
	if err != nil {
		return false, err
	}
	if len(sortedApps.Items) == 0 {
		return true, nil
	}

	// The last run (most recently started) is the first one in the sorted slice.
	lastRun := &sortedApps.Items[0]
	switch app.Spec.ConcurrencyPolicy {
	case v1beta1.ConcurrencyAllow:
		return true, nil
	case v1beta1.ConcurrencyForbid:
		return r.hasLastRunFinished(lastRun), nil
	case v1beta1.ConcurrencyReplace:
		if err := r.killLastRunIfNotFinished(ctx, lastRun); err != nil {
			return false, err
		}
		return true, nil
	}
	return true, nil
}

func (r *ReconcileScheduledSparkApplication) startNextRun(ctx context.Context, app *v1beta1.ScheduledSparkApplication, now time.Time) (string, error) {
	name, err := r.createSparkApplication(ctx, app, now)
	if err != nil {
		glog.Errorf("failed to create a SparkApplication instance for ScheduledSparkApplication %s/%s: %v", app.Namespace, app.Name, err)
		return "", err
	}
	return name, nil
}

func (r *ReconcileScheduledSparkApplication) hasLastRunFinished(app *v1beta1.SparkApplication) bool {
	return app.Status.AppState.State == v1beta1.CompletedState ||
		app.Status.AppState.State == v1beta1.FailedState
}

func (r *ReconcileScheduledSparkApplication) killLastRunIfNotFinished(ctx context.Context, app *v1beta1.SparkApplication) error {
	finished := r.hasLastRunFinished(app)
	if finished {
		return nil
	}

	namespacedName := types.NamespacedName{
		Namespace: app.Namespace,
		Name:      app.Name,
	}
	prevApp := &v1beta1.ScheduledSparkApplication{}
	err := r.client.Get(ctx, namespacedName, prevApp)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	err = r.client.Delete(ctx, prevApp, client.GracePeriodSeconds(0))
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	return nil
}

func (r *ReconcileScheduledSparkApplication) deleteScheduledSparkApplication(
	ctx context.Context, namespace string, name string) error {
	namespacedName := types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}
	ssa := &v1beta1.ScheduledSparkApplication{}
	err := r.client.Get(ctx, namespacedName, ssa)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	err = r.client.Delete(ctx, ssa, client.GracePeriodSeconds(0))
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *ReconcileScheduledSparkApplication) checkAndUpdatePastRuns(
	ctx context.Context,
	app *v1beta1.ScheduledSparkApplication,
	status *v1beta1.ScheduledSparkApplicationStatus) error {
	sortedApps, err := r.listSparkApplications(ctx, app)
	if err != nil {
		return err
	}

	var completedRuns []string
	var failedRuns []string
	for _, a := range sortedApps.Items {
		if a.Status.AppState.State == v1beta1.CompletedState {
			completedRuns = append(completedRuns, a.Name)
		} else if a.Status.AppState.State == v1beta1.FailedState {
			failedRuns = append(failedRuns, a.Name)
		}
	}

	var toDelete []string
	status.PastSuccessfulRunNames, toDelete = bookkeepPastRuns(completedRuns, app.Spec.SuccessfulRunHistoryLimit)
	for _, name := range toDelete {
		err := r.deleteScheduledSparkApplication(ctx, app.Namespace, name)
		if err != nil {
			logger.Error(err, "failed to delete ScheduledSparkApplication", "namespace", app.Namespace, "name", app.Name)
		}
	}
	status.PastFailedRunNames, toDelete = bookkeepPastRuns(failedRuns, app.Spec.FailedRunHistoryLimit)
	for _, name := range toDelete {
		err = r.deleteScheduledSparkApplication(ctx, app.Namespace, name)
		if err != nil {
			logger.Error(err, "failed to delete ScheduledSparkApplication", "namespace", app.Namespace, "name", app.Name)
		}
	}

	return nil
}

func (r *ReconcileScheduledSparkApplication) updateScheduledSparkApplicationStatus(
	ctx context.Context,
	app *v1beta1.ScheduledSparkApplication,
	newStatus *v1beta1.ScheduledSparkApplicationStatus) (reconcile.Result, error) {
	// If the status has not changed, do not perform an update.
	if isStatusEqual(newStatus, &app.Status) {
		return reconcile.Result{}, nil
	}

	toUpdate := app.DeepCopy()
	return reconcile.Result{}, retry.RetryOnConflict(retry.DefaultRetry, func() error {
		toUpdate.Status = *newStatus
		updateErr := r.client.Update(ctx, toUpdate)
		if updateErr == nil {
			return nil
		}

		namespacedName := types.NamespacedName{
			Namespace: toUpdate.Namespace,
			Name:      toUpdate.Name,
		}
		result := &v1beta1.ScheduledSparkApplication{}
		err := r.client.Get(ctx, namespacedName, result)
		if err != nil {
			return err
		}
		toUpdate = result

		return updateErr
	})
}

func (r *ReconcileScheduledSparkApplication) listSparkApplications(ctx context.Context, app *v1beta1.ScheduledSparkApplication) (sparkApps, error) {
	appLabels := make(map[string]string)
	appLabels[config.ScheduledSparkAppNameLabel] = app.Name
	ssaList := &v1beta1.SparkApplicationList{}
	err := r.client.List(ctx, ssaList, client.InNamespace(app.Namespace), client.MatchingLabels(appLabels))
	if err != nil {
		result := &sparkApps{}
		return *result, fmt.Errorf("failed to list SparkApplications: %v", err)
	}
	sortedApps := sparkApps(*ssaList)
	sort.Sort(sortedApps)
	return sortedApps, nil
}

func bookkeepPastRuns(names []string, runLimit *int32) (toKeep []string, toDelete []string) {
	limit := 1
	if runLimit != nil {
		limit = int(*runLimit)
	}

	if len(names) <= limit {
		return names, nil
	}
	toKeep = names[:limit]
	toDelete = names[limit:]
	return
}

func isStatusEqual(newStatus, currentStatus *v1beta1.ScheduledSparkApplicationStatus) bool {
	return newStatus.ScheduleState == currentStatus.ScheduleState &&
		newStatus.LastRun == currentStatus.LastRun &&
		newStatus.NextRun == currentStatus.NextRun &&
		newStatus.LastRunName == currentStatus.LastRunName &&
		reflect.DeepEqual(newStatus.PastSuccessfulRunNames, currentStatus.PastSuccessfulRunNames) &&
		reflect.DeepEqual(newStatus.PastFailedRunNames, currentStatus.PastFailedRunNames) &&
		newStatus.Reason == currentStatus.Reason
}
