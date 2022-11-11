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
	"reflect"
	"sort"
	"time"

	"github.com/golang/glog"
	"github.com/robfig/cron"

	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	crdclientset "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
	crdscheme "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned/scheme"
	crdinformers "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/informers/externalversions"
	crdlisters "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/listers/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
)

var (
	keyFunc = cache.DeletionHandlingMetaNamespaceKeyFunc
)

type Controller struct {
	crdClient        crdclientset.Interface
	kubeClient       kubernetes.Interface
	extensionsClient apiextensionsclient.Interface
	queue            workqueue.RateLimitingInterface
	cacheSynced      cache.InformerSynced
	ssaLister        crdlisters.ScheduledSparkApplicationLister
	saLister         crdlisters.SparkApplicationLister
	clock            clock.Clock
}

func NewController(
	crdClient crdclientset.Interface,
	kubeClient kubernetes.Interface,
	extensionsClient apiextensionsclient.Interface,
	informerFactory crdinformers.SharedInformerFactory,
	clock clock.Clock) *Controller {
	crdscheme.AddToScheme(scheme.Scheme)

	queue := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(),
		"scheduled-spark-application-controller")

	controller := &Controller{
		crdClient:        crdClient,
		kubeClient:       kubeClient,
		extensionsClient: extensionsClient,
		queue:            queue,
		clock:            clock,
	}

	informer := informerFactory.Sparkoperator().V1beta2().ScheduledSparkApplications()
	informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.onAdd,
		UpdateFunc: controller.onUpdate,
		DeleteFunc: controller.onDelete,
	})
	controller.cacheSynced = informer.Informer().HasSynced
	controller.ssaLister = informer.Lister()
	controller.saLister = informerFactory.Sparkoperator().V1beta2().SparkApplications().Lister()

	return controller
}

func (c *Controller) Start(workers int, stopCh <-chan struct{}) error {
	glog.Info("Starting the ScheduledSparkApplication controller")

	if !cache.WaitForCacheSync(stopCh, c.cacheSynced) {
		return fmt.Errorf("timed out waiting for cache to sync")
	}

	glog.Info("Starting the workers of the ScheduledSparkApplication controller")
	for i := 0; i < workers; i++ {
		// runWorker will loop until "something bad" happens. Until will then rekick
		// the worker after one second.
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	return nil
}

func (c *Controller) Stop() {
	glog.Info("Stopping the ScheduledSparkApplication controller")
	c.queue.ShutDown()
}

func (c *Controller) runWorker() {
	defer utilruntime.HandleCrash()
	for c.processNextItem() {
	}
}

func (c *Controller) processNextItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.syncScheduledSparkApplication(key.(string))
	if err == nil {
		// Successfully processed the key or the key was not found so tell the queue to stop tracking
		// history for your key. This will reset things like failure counts for per-item rate limiting.
		c.queue.Forget(key)
		return true
	}

	// There was a failure so be sure to report it. This method allows for pluggable error handling
	// which can be used for things like cluster-monitoring
	utilruntime.HandleError(fmt.Errorf("failed to sync ScheduledSparkApplication %q: %v", key, err))
	// Since we failed, we should requeue the item to work on later.  This method will add a backoff
	// to avoid hot-looping on particular items (they're probably still not going to work right away)
	// and overall controller protection (everything I've done is broken, this controller needs to
	// calm down or it can starve other useful work) cases.
	c.queue.AddRateLimited(key)

	return true
}

func (c *Controller) syncScheduledSparkApplication(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	app, err := c.ssaLister.ScheduledSparkApplications(namespace).Get(name)
	if err != nil {
		return err
	}

	if app.Spec.Suspend != nil && *app.Spec.Suspend {
		return nil
	}

	glog.V(2).Infof("Syncing ScheduledSparkApplication %s/%s", app.Namespace, app.Name)
	status := app.Status.DeepCopy()
	schedule, err := cron.ParseStandard(app.Spec.Schedule)
	if err != nil {
		glog.Errorf("failed to parse schedule %s of ScheduledSparkApplication %s/%s: %v", app.Spec.Schedule, app.Namespace, app.Name, err)
		status.ScheduleState = v1beta2.FailedValidationState
		status.Reason = err.Error()
	} else {
		status.ScheduleState = v1beta2.ScheduledState
		now := c.clock.Now()
		nextRunTime := status.NextRun.Time
		// if we updated the schedule for an earlier execution - those changes need to be reflected
		updatedNextRunTime := schedule.Next(now)
		if nextRunTime.IsZero() || updatedNextRunTime.Before(nextRunTime) {
			// The first run of the application.
			nextRunTime = updatedNextRunTime
			status.NextRun = metav1.NewTime(nextRunTime)
		}
		if nextRunTime.Before(now) {
			// Check if the condition for starting the next run is satisfied.
			ok, err := c.shouldStartNextRun(app)
			if err != nil {
				return err
			}
			if ok {
				glog.Infof("Next run of ScheduledSparkApplication %s/%s is due, creating a new SparkApplication instance", app.Namespace, app.Name)
				name, err := c.startNextRun(app, now)
				if err != nil {
					return err
				}
				status.LastRun = metav1.NewTime(now)
				status.NextRun = metav1.NewTime(schedule.Next(status.LastRun.Time))
				status.LastRunName = name
			}
		}

		if err = c.checkAndUpdatePastRuns(app, status); err != nil {
			return err
		}
	}

	return c.updateScheduledSparkApplicationStatus(app, status)
}

func (c *Controller) onAdd(obj interface{}) {
	c.enqueue(obj)
}

func (c *Controller) onUpdate(oldObj, newObj interface{}) {
	c.enqueue(newObj)
}

func (c *Controller) onDelete(obj interface{}) {
	c.dequeue(obj)
}

func (c *Controller) enqueue(obj interface{}) {
	key, err := keyFunc(obj)
	if err != nil {
		glog.Errorf("failed to get key for %v: %v", obj, err)
		return
	}

	c.queue.AddRateLimited(key)
}

func (c *Controller) dequeue(obj interface{}) {
	key, err := keyFunc(obj)
	if err != nil {
		glog.Errorf("failed to get key for %v: %v", obj, err)
		return
	}

	c.queue.Forget(key)
	c.queue.Done(key)
}

func (c *Controller) createSparkApplication(
	scheduledApp *v1beta2.ScheduledSparkApplication, t time.Time) (string, error) {
	app := &v1beta2.SparkApplication{}
	app.Spec = scheduledApp.Spec.Template
	app.Name = fmt.Sprintf("%s-%d", scheduledApp.Name, t.UnixNano())
	app.OwnerReferences = append(app.OwnerReferences, metav1.OwnerReference{
		APIVersion: v1beta2.SchemeGroupVersion.String(),
		Kind:       reflect.TypeOf(v1beta2.ScheduledSparkApplication{}).Name(),
		Name:       scheduledApp.Name,
		UID:        scheduledApp.UID,
	})
	app.ObjectMeta.Namespace = scheduledApp.Namespace
	app.ObjectMeta.Labels = make(map[string]string)
	for key, value := range scheduledApp.Labels {
		app.ObjectMeta.Labels[key] = value
	}
	app.ObjectMeta.Labels[config.ScheduledSparkAppNameLabel] = scheduledApp.Name
	_, err := c.crdClient.SparkoperatorV1beta2().SparkApplications(scheduledApp.Namespace).Create(context.TODO(), app, metav1.CreateOptions{})
	if err != nil {
		return "", err
	}
	return app.Name, nil
}

func (c *Controller) shouldStartNextRun(app *v1beta2.ScheduledSparkApplication) (bool, error) {
	sortedApps, err := c.listSparkApplications(app)
	if err != nil {
		return false, err
	}
	if len(sortedApps) == 0 {
		return true, nil
	}

	// The last run (most recently started) is the first one in the sorted slice.
	lastRun := sortedApps[0]
	switch app.Spec.ConcurrencyPolicy {
	case v1beta2.ConcurrencyAllow:
		return true, nil
	case v1beta2.ConcurrencyForbid:
		return c.hasLastRunFinished(lastRun), nil
	case v1beta2.ConcurrencyReplace:
		if err := c.killLastRunIfNotFinished(lastRun); err != nil {
			return false, err
		}
		return true, nil
	}
	return true, nil
}

func (c *Controller) startNextRun(app *v1beta2.ScheduledSparkApplication, now time.Time) (string, error) {
	name, err := c.createSparkApplication(app, now)
	if err != nil {
		glog.Errorf("failed to create a SparkApplication instance for ScheduledSparkApplication %s/%s: %v", app.Namespace, app.Name, err)
		return "", err
	}
	return name, nil
}

func (c *Controller) hasLastRunFinished(app *v1beta2.SparkApplication) bool {
	return app.Status.AppState.State == v1beta2.CompletedState ||
		app.Status.AppState.State == v1beta2.FailedState
}

func (c *Controller) killLastRunIfNotFinished(app *v1beta2.SparkApplication) error {
	finished := c.hasLastRunFinished(app)
	if finished {
		return nil
	}

	// Delete the SparkApplication object of the last run.
	if err := c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Delete(
		context.TODO(),
		app.Name,
		metav1.DeleteOptions{GracePeriodSeconds: int64ptr(0)},
	); err != nil {
		return err
	}

	return nil
}

func (c *Controller) checkAndUpdatePastRuns(
	app *v1beta2.ScheduledSparkApplication,
	status *v1beta2.ScheduledSparkApplicationStatus) error {
	sortedApps, err := c.listSparkApplications(app)
	if err != nil {
		return err
	}

	var completedRuns []string
	var failedRuns []string
	for _, a := range sortedApps {
		if a.Status.AppState.State == v1beta2.CompletedState {
			completedRuns = append(completedRuns, a.Name)
		} else if a.Status.AppState.State == v1beta2.FailedState {
			failedRuns = append(failedRuns, a.Name)
		}
	}

	var toDelete []string
	status.PastSuccessfulRunNames, toDelete = bookkeepPastRuns(completedRuns, app.Spec.SuccessfulRunHistoryLimit)
	for _, name := range toDelete {
		c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Delete(context.TODO(), name, metav1.DeleteOptions{GracePeriodSeconds: int64ptr(0)})
	}
	status.PastFailedRunNames, toDelete = bookkeepPastRuns(failedRuns, app.Spec.FailedRunHistoryLimit)
	for _, name := range toDelete {
		c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Delete(context.TODO(), name, metav1.DeleteOptions{GracePeriodSeconds: int64ptr(0)})
	}

	return nil
}

func (c *Controller) updateScheduledSparkApplicationStatus(
	app *v1beta2.ScheduledSparkApplication,
	newStatus *v1beta2.ScheduledSparkApplicationStatus) error {
	// If the status has not changed, do not perform an update.
	if isStatusEqual(newStatus, &app.Status) {
		return nil
	}

	toUpdate := app.DeepCopy()
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		toUpdate.Status = *newStatus
		_, updateErr := c.crdClient.SparkoperatorV1beta2().ScheduledSparkApplications(toUpdate.Namespace).UpdateStatus(
			context.TODO(),
			toUpdate,
			metav1.UpdateOptions{},
		)
		if updateErr == nil {
			return nil
		}

		result, err := c.crdClient.SparkoperatorV1beta2().ScheduledSparkApplications(toUpdate.Namespace).Get(
			context.TODO(),
			toUpdate.Name,
			metav1.GetOptions{},
		)
		if err != nil {
			return err
		}
		toUpdate = result

		return updateErr
	})
}

func (c *Controller) listSparkApplications(app *v1beta2.ScheduledSparkApplication) (sparkApps, error) {
	set := labels.Set{config.ScheduledSparkAppNameLabel: app.Name}
	apps, err := c.saLister.SparkApplications(app.Namespace).List(set.AsSelector())
	if err != nil {
		return nil, fmt.Errorf("failed to list SparkApplications: %v", err)
	}
	sortedApps := sparkApps(apps)
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

func isStatusEqual(newStatus, currentStatus *v1beta2.ScheduledSparkApplicationStatus) bool {
	return newStatus.ScheduleState == currentStatus.ScheduleState &&
		newStatus.LastRun == currentStatus.LastRun &&
		newStatus.NextRun == currentStatus.NextRun &&
		newStatus.LastRunName == currentStatus.LastRunName &&
		reflect.DeepEqual(newStatus.PastSuccessfulRunNames, currentStatus.PastSuccessfulRunNames) &&
		reflect.DeepEqual(newStatus.PastFailedRunNames, currentStatus.PastFailedRunNames) &&
		newStatus.Reason == currentStatus.Reason
}

func int64ptr(n int64) *int64 {
	return &n
}
