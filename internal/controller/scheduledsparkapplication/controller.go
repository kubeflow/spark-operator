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
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	_ "time/tzdata"

	"github.com/robfig/cron/v3"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
)

var (
	logger = log.Log.WithName("")
)

type Options struct {
	Namespaces []string
}

// Reconciler reconciles a ScheduledSparkApplication object
type Reconciler struct {
	scheme   *runtime.Scheme
	client   client.Client
	recorder record.EventRecorder
	clock    clock.Clock
	options  Options
}

var _ reconcile.Reconciler = &Reconciler{}

func NewReconciler(
	scheme *runtime.Scheme,
	client client.Client,
	recorder record.EventRecorder,
	clock clock.Clock,
	options Options,
) *Reconciler {
	return &Reconciler{
		scheme:   scheme,
		client:   client,
		recorder: recorder,
		clock:    clock,
		options:  options,
	}
}

// +kubebuilder:rbac:groups=sparkoperator.k8s.io,resources=scheduledsparkapplications,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sparkoperator.k8s.io,resources=scheduledsparkapplications/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=sparkoperator.k8s.io,resources=scheduledsparkapplications/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the ScheduledSparkApplication object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.18.2/pkg/reconcile
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	key := req.NamespacedName
	oldScheduledApp, err := r.getScheduledSparkApplication(ctx, key)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{Requeue: true}, err
	}
	scheduledApp := oldScheduledApp.DeepCopy()
	logger.Info("Reconciling ScheduledSparkApplication", "name", scheduledApp.Name, "namespace", scheduledApp.Namespace, "state", scheduledApp.Status.ScheduleState)

	if scheduledApp.Spec.Suspend != nil && *scheduledApp.Spec.Suspend {
		return ctrl.Result{}, nil
	}

	timezone := scheduledApp.Spec.TimeZone
	if timezone == "" {
		timezone = "Local"
	} else {
		// Explicitly validate the timezone for a better user experience, but only if it's explicitly specified
		_, err = time.LoadLocation(timezone)
		if err != nil {
			logger.Error(err, "Failed to load timezone location", "name", scheduledApp.Name, "namespace", scheduledApp.Namespace, "timezone", timezone)
			scheduledApp.Status.ScheduleState = v1beta2.ScheduleStateFailedValidation
			scheduledApp.Status.Reason = fmt.Sprintf("Invalid timezone: %v", err)
			if updateErr := r.updateScheduledSparkApplicationStatus(ctx, scheduledApp); updateErr != nil {
				return ctrl.Result{Requeue: true}, updateErr
			}
			return ctrl.Result{}, nil
		}
	}

	// Ensure backwards compatibility if the schedule is relying on internal functionality of robfig/cron
	cronSchedule := scheduledApp.Spec.Schedule
	if !strings.HasPrefix(cronSchedule, "CRON_TZ=") && !strings.HasPrefix(cronSchedule, "TZ=") {
		cronSchedule = fmt.Sprintf("CRON_TZ=%s %s", timezone, cronSchedule)
	}

	schedule, parseErr := cron.ParseStandard(cronSchedule)
	if parseErr != nil {
		logger.Error(err, "Failed to parse schedule of ScheduledSparkApplication", "name", scheduledApp.Name, "namespace", scheduledApp.Namespace, "schedule", scheduledApp.Spec.Schedule)
		scheduledApp.Status.ScheduleState = v1beta2.ScheduleStateFailedValidation
		scheduledApp.Status.Reason = parseErr.Error()
		if updateErr := r.updateScheduledSparkApplicationStatus(ctx, scheduledApp); updateErr != nil {
			return ctrl.Result{Requeue: true}, updateErr
		}
		return ctrl.Result{}, nil
	}

	switch scheduledApp.Status.ScheduleState {
	case v1beta2.ScheduleStateNew:
		now := r.clock.Now()
		oldNextRunTime := scheduledApp.Status.NextRun.Time
		nextRunTime := schedule.Next(now)
		if oldNextRunTime.IsZero() || nextRunTime.Before(oldNextRunTime) {
			scheduledApp.Status.NextRun = metav1.NewTime(nextRunTime)
		}
		scheduledApp.Status.ScheduleState = v1beta2.ScheduleStateScheduled
		if err := r.updateScheduledSparkApplicationStatus(ctx, scheduledApp); err != nil {
			return ctrl.Result{Requeue: true}, err
		}
		return ctrl.Result{RequeueAfter: nextRunTime.Sub(now)}, err
	case v1beta2.ScheduleStateScheduled:
		now := r.clock.Now()
		nextRunTime := scheduledApp.Status.NextRun
		if nextRunTime.IsZero() {
			scheduledApp.Status.NextRun = metav1.NewTime(schedule.Next(now))
			if err := r.updateScheduledSparkApplicationStatus(ctx, scheduledApp); err != nil {
				return ctrl.Result{Requeue: true}, err
			}
			return ctrl.Result{RequeueAfter: schedule.Next(now).Sub(now)}, nil
		}

		if nextRunTime.Time.After(now) {
			return ctrl.Result{RequeueAfter: nextRunTime.Time.Sub(now)}, nil
		}

		ok, err := r.shouldStartNextRun(scheduledApp)
		if err != nil {
			return ctrl.Result{Requeue: true}, err
		}
		if !ok {
			return ctrl.Result{RequeueAfter: schedule.Next(now).Sub(now)}, nil
		}

		logger.Info("Next run of ScheduledSparkApplication is due", "name", scheduledApp.Name, "namespace", scheduledApp.Namespace)
		app, err := r.startNextRun(scheduledApp, now)
		if err != nil {
			logger.Error(err, "Failed to start next run for ScheduledSparkApplication", "name", scheduledApp.Name, "namespace", scheduledApp.Namespace)
			return ctrl.Result{RequeueAfter: schedule.Next(now).Sub(now)}, err
		}

		scheduledApp.Status.LastRun = metav1.NewTime(now)
		scheduledApp.Status.LastRunName = app.Name
		scheduledApp.Status.NextRun = metav1.NewTime(schedule.Next(now))
		if err = r.checkAndUpdatePastRuns(ctx, scheduledApp); err != nil {
			return ctrl.Result{Requeue: true}, err
		}
		if err := r.updateScheduledSparkApplicationStatus(ctx, scheduledApp); err != nil {
			return ctrl.Result{Requeue: true}, err
		}
		return ctrl.Result{RequeueAfter: schedule.Next(now).Sub(now)}, nil
	case v1beta2.ScheduleStateFailedValidation:
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, options controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("scheduled-spark-application-controller").
		Watches(
			&v1beta2.ScheduledSparkApplication{},
			NewEventHandler(),
			builder.WithPredicates(
				NewEventFilter(r.options.Namespaces),
			)).
		WithOptions(options).
		Complete(r)
}

func (r *Reconciler) getScheduledSparkApplication(ctx context.Context, key types.NamespacedName) (*v1beta2.ScheduledSparkApplication, error) {
	app := &v1beta2.ScheduledSparkApplication{}
	if err := r.client.Get(ctx, key, app); err != nil {
		return nil, err
	}
	return app, nil
}

func (r *Reconciler) createSparkApplication(
	scheduledApp *v1beta2.ScheduledSparkApplication,
	t time.Time,
) (*v1beta2.SparkApplication, error) {
	labels := map[string]string{
		common.LabelScheduledSparkAppName: scheduledApp.Name,
	}
	for key, value := range scheduledApp.Labels {
		labels[key] = value
	}
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", scheduledApp.Name, t.UnixNano()),
			Namespace: scheduledApp.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         v1beta2.SchemeGroupVersion.String(),
				Kind:               reflect.TypeOf(v1beta2.ScheduledSparkApplication{}).Name(),
				Name:               scheduledApp.Name,
				UID:                scheduledApp.UID,
				BlockOwnerDeletion: util.BoolPtr(true),
			}},
		},
		Spec: scheduledApp.Spec.Template,
	}
	if err := r.client.Create(context.TODO(), app); err != nil {
		return nil, err
	}
	return app, nil
}

// shouldStartNextRun checks if the next run should be started.
func (r *Reconciler) shouldStartNextRun(scheduledApp *v1beta2.ScheduledSparkApplication) (bool, error) {
	apps, err := r.listSparkApplications(scheduledApp)
	if err != nil {
		return false, err
	}
	if len(apps) == 0 {
		return true, nil
	}

	sortSparkApplicationsInPlace(apps)
	// The last run (most recently started) is the first one in the sorted slice.
	lastRun := apps[0]
	switch scheduledApp.Spec.ConcurrencyPolicy {
	case v1beta2.ConcurrencyAllow:
		return true, nil
	case v1beta2.ConcurrencyForbid:
		return r.hasLastRunFinished(lastRun), nil
	case v1beta2.ConcurrencyReplace:
		if err := r.killLastRunIfNotFinished(lastRun); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (r *Reconciler) startNextRun(scheduledApp *v1beta2.ScheduledSparkApplication, now time.Time) (*v1beta2.SparkApplication, error) {
	app, err := r.createSparkApplication(scheduledApp, now)
	if err != nil {
		return nil, err
	}
	return app, nil
}

func (r *Reconciler) hasLastRunFinished(app *v1beta2.SparkApplication) bool {
	return app.Status.AppState.State == v1beta2.ApplicationStateCompleted ||
		app.Status.AppState.State == v1beta2.ApplicationStateFailed
}

func (r *Reconciler) killLastRunIfNotFinished(app *v1beta2.SparkApplication) error {
	finished := r.hasLastRunFinished(app)
	if finished {
		return nil
	}

	// Delete the SparkApplication object of the last run.
	if err := r.client.Delete(context.TODO(), app, client.GracePeriodSeconds(0)); err != nil {
		return err
	}

	return nil
}

func (r *Reconciler) checkAndUpdatePastRuns(ctx context.Context, scheduledApp *v1beta2.ScheduledSparkApplication) error {
	apps, err := r.listSparkApplications(scheduledApp)
	if err != nil {
		return err
	}

	var completedApps []*v1beta2.SparkApplication
	var failedApps []*v1beta2.SparkApplication
	for _, app := range apps {
		if app.Status.AppState.State == v1beta2.ApplicationStateCompleted {
			completedApps = append(completedApps, app)
		} else if app.Status.AppState.State == v1beta2.ApplicationStateFailed {
			failedApps = append(failedApps, app)
		}
	}

	historyLimit := 1
	if scheduledApp.Spec.SuccessfulRunHistoryLimit != nil {
		historyLimit = int(*scheduledApp.Spec.SuccessfulRunHistoryLimit)
	}

	toKeep, toDelete := bookkeepPastRuns(completedApps, historyLimit)
	scheduledApp.Status.PastSuccessfulRunNames = []string{}
	for _, app := range toKeep {
		scheduledApp.Status.PastSuccessfulRunNames = append(scheduledApp.Status.PastSuccessfulRunNames, app.Name)
	}
	for _, app := range toDelete {
		if err := r.client.Delete(ctx, app, client.GracePeriodSeconds(0)); err != nil {
			return err
		}
	}

	historyLimit = 1
	if scheduledApp.Spec.FailedRunHistoryLimit != nil {
		historyLimit = int(*scheduledApp.Spec.FailedRunHistoryLimit)
	}
	toKeep, toDelete = bookkeepPastRuns(failedApps, historyLimit)
	scheduledApp.Status.PastFailedRunNames = []string{}
	for _, app := range toKeep {
		scheduledApp.Status.PastFailedRunNames = append(scheduledApp.Status.PastFailedRunNames, app.Name)
	}
	for _, app := range toDelete {
		if err := r.client.Delete(ctx, app, client.GracePeriodSeconds(0)); err != nil {
			return err
		}
	}

	return nil
}

func (r *Reconciler) updateScheduledSparkApplicationStatus(ctx context.Context, scheduledApp *v1beta2.ScheduledSparkApplication) error {
	// logger.Info("Updating SchedulingSparkApplication", "name", scheduledApp.Name, "namespace", scheduledApp.Namespace, "status", scheduledApp.Status)
	if err := r.client.Status().Update(ctx, scheduledApp); err != nil {
		return fmt.Errorf("failed to update ScheduledSparkApplication status: %v", err)
	}

	return nil
}

// listSparkApplications lists SparkApplications that are owned by the given ScheduledSparkApplication and sort them by decreasing order of creation timestamp.
func (r *Reconciler) listSparkApplications(app *v1beta2.ScheduledSparkApplication) ([]*v1beta2.SparkApplication, error) {
	set := labels.Set{common.LabelScheduledSparkAppName: app.Name}
	appList := &v1beta2.SparkApplicationList{}
	if err := r.client.List(context.TODO(), appList, client.InNamespace(app.Namespace), client.MatchingLabels(set)); err != nil {
		return nil, fmt.Errorf("failed to list SparkApplications: %v", err)
	}
	apps := []*v1beta2.SparkApplication{}
	for _, item := range appList.Items {
		apps = append(apps, &item)
	}
	return apps, nil
}

// sortSparkApplicationsInPlace sorts the given slice of SparkApplication in place by the decreasing order of creation timestamp.
func sortSparkApplicationsInPlace(apps []*v1beta2.SparkApplication) {
	sort.Slice(apps, func(i, j int) bool {
		return apps[i].CreationTimestamp.After(apps[j].CreationTimestamp.Time)
	})
}

// bookkeepPastRuns bookkeeps the past runs of the given SparkApplication slice.
func bookkeepPastRuns(apps []*v1beta2.SparkApplication, limit int) ([]*v1beta2.SparkApplication, []*v1beta2.SparkApplication) {
	if len(apps) <= limit {
		return apps, nil
	}
	sortSparkApplicationsInPlace(apps)
	toKeep := apps[:limit]
	toDelete := apps[limit:]
	return toKeep, toDelete
}
