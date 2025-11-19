/*
Copyright 2024 The Kubeflow authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in          writing, software
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
	"strconv"
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
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

var (
	logger = log.Log.WithName("")
)

// Controller options
type Options struct {
	Namespaces []string

	// Controller-wide timestamp precision for naming SparkApplications
	// Allowed: nanos, micros, millis, seconds, minutes
	// Default: nanos
	ScheduledSATimestampPrecision string
}

// Reconciler manages ScheduledSparkApplication
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

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	key := req.NamespacedName
	oldScheduled, err := r.getScheduledSparkApplication(ctx, key)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{Requeue: true}, err
	}

	scheduled := oldScheduled.DeepCopy()
	logger.Info("Reconciling ScheduledSparkApplication",
		"name", scheduled.Name,
		"namespace", scheduled.Namespace,
		"state", scheduled.Status.ScheduleState,
	)

	// suspend = true
	if scheduled.Spec.Suspend != nil && *scheduled.Spec.Suspend {
		return ctrl.Result{}, nil
	}

	// timezone validation
	timezone := scheduled.Spec.TimeZone
	if timezone == "" {
		timezone = "Local"
	} else {
		if _, err := time.LoadLocation(timezone); err != nil {
			scheduled.Status.ScheduleState = v1beta2.ScheduleStateFailedValidation
			scheduled.Status.Reason = fmt.Sprintf("Invalid timezone: %v", err)
			_ = r.updateScheduledSparkApplicationStatus(ctx, scheduled)
			return ctrl.Result{}, nil
		}
	}

	// Support CRON_TZ= prefix
	cronExpr := scheduled.Spec.Schedule
	if !strings.HasPrefix(cronExpr, "CRON_TZ=") && !strings.HasPrefix(cronExpr, "TZ=") {
		cronExpr = fmt.Sprintf("CRON_TZ=%s %s", timezone, cronExpr)
	}

	sched, parseErr := cron.ParseStandard(cronExpr)
	if parseErr != nil {
		scheduled.Status.ScheduleState = v1beta2.ScheduleStateFailedValidation
		scheduled.Status.Reason = parseErr.Error()
		_ = r.updateScheduledSparkApplicationStatus(ctx, scheduled)
		return ctrl.Result{}, nil
	}

	switch scheduled.Status.ScheduleState {

	// First-time scheduling
	case v1beta2.ScheduleStateNew:
		now := r.clock.Now()
		next := sched.Next(now)
		scheduled.Status.NextRun = metav1.NewTime(next)
		scheduled.Status.ScheduleState = v1beta2.ScheduleStateScheduled
		_ = r.updateScheduledSparkApplicationStatus(ctx, scheduled)
		return ctrl.Result{RequeueAfter: next.Sub(now)}, nil

	// Standard loop
	case v1beta2.ScheduleStateScheduled:
		now := r.clock.Now()
		if scheduled.Status.NextRun.Time.After(now) {
			return ctrl.Result{RequeueAfter: scheduled.Status.NextRun.Sub(now)}, nil
		}

		ready, err := r.shouldStartNextRun(scheduled)
		if err != nil {
			return ctrl.Result{Requeue: true}, err
		}
		if !ready {
			next := sched.Next(now)
			return ctrl.Result{RequeueAfter: next.Sub(now)}, nil
		}

		app, err := r.startNextRun(scheduled, now)
		if err != nil {
			next := sched.Next(now)
			return ctrl.Result{RequeueAfter: next.Sub(now)}, err
		}

		scheduled.Status.LastRun = metav1.NewTime(now)
		scheduled.Status.LastRunName = app.Name
		scheduled.Status.NextRun = metav1.NewTime(sched.Next(now))

		if err := r.checkAndUpdatePastRuns(ctx, scheduled); err != nil {
			return ctrl.Result{Requeue: true}, err
		}

		_ = r.updateScheduledSparkApplicationStatus(ctx, scheduled)
		return ctrl.Result{RequeueAfter: scheduled.Status.NextRun.Sub(now)}, nil

	case v1beta2.ScheduleStateFailedValidation:
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("scheduled-spark-application-controller").
		Watches(
			&v1beta2.ScheduledSparkApplication{},
			NewEventHandler(),
			builder.WithPredicates(NewEventFilter(r.options.Namespaces)),
		).
		WithOptions(opts).
		Complete(r)
}

func (r *Reconciler) getScheduledSparkApplication(ctx context.Context, key types.NamespacedName) (*v1beta2.ScheduledSparkApplication, error) {
	obj := &v1beta2.ScheduledSparkApplication{}
	if err := r.client.Get(ctx, key, obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func (r *Reconciler) createSparkApplication(scheduled *v1beta2.ScheduledSparkApplication, t time.Time) (*v1beta2.SparkApplication, error) {
	labels := map[string]string{common.LabelScheduledSparkAppName: scheduled.Name}
	for k, v := range scheduled.Labels {
		labels[k] = v
	}

	precision := strings.TrimSpace(r.options.ScheduledSATimestampPrecision)
	if precision == "" {
		precision = "nanos"
	}
	suffix := formatTimestamp(precision, t)

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s", scheduled.Name, suffix),
			Namespace: scheduled.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         v1beta2.SchemeGroupVersion.String(),
					Kind:               reflect.TypeOf(v1beta2.ScheduledSparkApplication{}).Name(),
					Name:               scheduled.Name,
					UID:                scheduled.UID,
					BlockOwnerDeletion: ptr.To(true),
				},
			},
		},
		Spec: scheduled.Spec.Template,
	}

	if err := r.client.Create(context.TODO(), app); err != nil {
		return nil, err
	}
	return app, nil
}

// Allowed precisions: nanos, micros, millis, seconds, minutes.
func formatTimestamp(precision string, t time.Time) string {
	switch precision {
	case "minutes":
		return strconv.FormatInt(t.Unix()/60, 10)
	case "seconds":
		return strconv.FormatInt(t.Unix(), 10)
	case "millis":
		return strconv.FormatInt(t.UnixNano()/1e6, 10)
	case "micros":
		return strconv.FormatInt(t.UnixNano()/1e3, 10)
	case "nanos":
		fallthrough
	default:
		return strconv.FormatInt(t.UnixNano(), 10)
	}
}

func (r *Reconciler) shouldStartNextRun(scheduled *v1beta2.ScheduledSparkApplication) (bool, error) {
	apps, err := r.listSparkApplications(scheduled)
	if err != nil {
		return false, err
	}
	if len(apps) == 0 {
		return true, nil
	}

	sortSparkApplicationsInPlace(apps)
	last := apps[0]

	switch scheduled.Spec.ConcurrencyPolicy {
	case v1beta2.ConcurrencyAllow:
		return true, nil
	case v1beta2.ConcurrencyForbid:
		return r.hasLastRunFinished(last), nil
	case v1beta2.ConcurrencyReplace:
		if err := r.killLastRunIfNotFinished(last); err != nil {
			return false, err
		}
		return true, nil
	}

	return false, nil
}

func (r *Reconciler) startNextRun(scheduled *v1beta2.ScheduledSparkApplication, now time.Time) (*v1beta2.SparkApplication, error) {
	return r.createSparkApplication(scheduled, now)
}

func (r *Reconciler) hasLastRunFinished(app *v1beta2.SparkApplication) bool {
	return app.Status.AppState.State == v1beta2.ApplicationStateCompleted ||
		app.Status.AppState.State == v1beta2.ApplicationStateFailed
}

func (r *Reconciler) killLastRunIfNotFinished(app *v1beta2.SparkApplication) error {
	if r.hasLastRunFinished(app) {
		return nil
	}
	return r.client.Delete(context.TODO(), app, client.GracePeriodSeconds(0))
}

func (r *Reconciler) checkAndUpdatePastRuns(ctx context.Context, scheduled *v1beta2.ScheduledSparkApplication) error {
	apps, err := r.listSparkApplications(scheduled)
	if err != nil {
		return err
	}

	var completed, failed []*v1beta2.SparkApplication
	for _, app := range apps {
		switch app.Status.AppState.State {
		case v1beta2.ApplicationStateCompleted:
			completed = append(completed, app)
		case v1beta2.ApplicationStateFailed:
			failed = append(failed, app)
		}
	}

	// Completed history
	successLimit := 1
	if scheduled.Spec.SuccessfulRunHistoryLimit != nil {
		successLimit = int(*scheduled.Spec.SuccessfulRunHistoryLimit)
	}
	toKeep, toDelete := bookkeepPastRuns(completed, successLimit)
	scheduled.Status.PastSuccessfulRunNames = mapSparkAppNames(toKeep)
	for _, app := range toDelete {
		_ = r.client.Delete(ctx, app, client.GracePeriodSeconds(0))
	}

	// Failed history
	failLimit := 1
	if scheduled.Spec.FailedRunHistoryLimit != nil {
		failLimit = int(*scheduled.Spec.FailedRunHistoryLimit)
	}
	toKeep, toDelete = bookkeepPastRuns(failed, failLimit)
	scheduled.Status.PastFailedRunNames = mapSparkAppNames(toKeep)
	for _, app := range toDelete {
		_ = r.client.Delete(ctx, app, client.GracePeriodSeconds(0))
	}

	return nil
}

func mapSparkAppNames(apps []*v1beta2.SparkApplication) []string {
	var names []string
	for _, app := range apps {
		names = append(names, app.Name)
	}
	return names
}

func (r *Reconciler) updateScheduledSparkApplicationStatus(ctx context.Context, obj *v1beta2.ScheduledSparkApplication) error {
	if err := r.client.Status().Update(ctx, obj); err != nil {
		return fmt.Errorf("failed to update ScheduledSparkApplication status: %v", err)
	}
	return nil
}

func (r *Reconciler) listSparkApplications(app *v1beta2.ScheduledSparkApplication) ([]*v1beta2.SparkApplication, error) {
	set := labels.Set{common.LabelScheduledSparkAppName: app.Name}
	list := &v1beta2.SparkApplicationList{}
	if err := r.client.List(context.TODO(), list,
		client.InNamespace(app.Namespace),
		client.MatchingLabels(set)); err != nil {
		return nil, err
	}

	var out []*v1beta2.SparkApplication
	for _, item := range list.Items {
		out = append(out, &item)
	}
	return out, nil
}

func sortSparkApplicationsInPlace(apps []*v1beta2.SparkApplication) {
	sort.Slice(apps, func(i, j int) bool {
		return apps[i].CreationTimestamp.After(apps[j].CreationTimestamp.Time)
	})
}

func bookkeepPastRuns(apps []*v1beta2.SparkApplication, limit int) ([]*v1beta2.SparkApplication, []*v1beta2.SparkApplication) {
	if len(apps) <= limit {
		return apps, nil
	}
	sortSparkApplicationsInPlace(apps)
	return apps[:limit], apps[limit:]
}
