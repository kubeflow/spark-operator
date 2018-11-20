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
	"fmt"
	"reflect"
	"time"

	"github.com/golang/glog"
	"github.com/robfig/cron"

	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/clock"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	crdclientset "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
	crdscheme "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned/scheme"
	crdinformers "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/informers/externalversions"
	crdlisters "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/listers/sparkoperator.k8s.io/v1alpha1"
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

	informer := informerFactory.Sparkoperator().V1alpha1().ScheduledSparkApplications()
	informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.onAdd,
		UpdateFunc: controller.onUpdate,
		DeleteFunc: controller.onDelete,
	})
	controller.cacheSynced = informer.Informer().HasSynced
	controller.ssaLister = informer.Lister()
	controller.saLister = informerFactory.Sparkoperator().V1alpha1().SparkApplications().Lister()

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
		status.ScheduleState = v1alpha1.FailedValidationState
		status.Reason = err.Error()
	} else {
		status.ScheduleState = v1alpha1.ScheduledState
		now := c.clock.Now()
		nextRunTime := status.NextRun.Time
		if nextRunTime.IsZero() {
			// The first run of the application.
			nextRunTime = schedule.Next(now)
			status.NextRun = metav1.NewTime(nextRunTime)
		}
		if nextRunTime.Before(now) {
			// The next run is due. Check if this is the first run of the application.
			if status.LastRunName == "" {
				// This is the first run of the application.
				if err = c.startNextRun(app, status, schedule); err != nil {
					return err
				}
			} else {
				// Check if the condition for starting the next run is satisfied.
				ok, err := c.shouldStartNextRun(app)
				if err != nil {
					return err
				}
				if ok {
					// Start the next run if the condition is satisfied.
					if err = c.startNextRun(app, status, schedule); err != nil {
						return err
					}
				} else {
					// Otherwise, check and update past runs.
					if err = c.checkAndUpdatePastRuns(app, status); err != nil {
						return err
					}
				}
			}
		} else {
			// The next run is not due yet, check and update past runs.
			if status.LastRunName != "" {
				if err = c.checkAndUpdatePastRuns(app, status); err != nil {
					return err
				}
			}
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
	scheduledApp *v1alpha1.ScheduledSparkApplication, t time.Time) (string, error) {
	app := &v1alpha1.SparkApplication{}
	app.Spec = scheduledApp.Spec.Template
	app.Name = fmt.Sprintf("%s-%d", scheduledApp.Name, t.UnixNano())
	app.OwnerReferences = append(app.OwnerReferences, metav1.OwnerReference{
		APIVersion: v1alpha1.SchemeGroupVersion.String(),
		Kind:       reflect.TypeOf(v1alpha1.ScheduledSparkApplication{}).Name(),
		Name:       scheduledApp.Name,
		UID:        scheduledApp.UID,
	})
	app.ObjectMeta.SetLabels(scheduledApp.GetLabels())
	_, err := c.crdClient.SparkoperatorV1alpha1().SparkApplications(scheduledApp.Namespace).Create(app)
	if err != nil {
		return "", err
	}
	return app.Name, nil
}

func (c *Controller) shouldStartNextRun(app *v1alpha1.ScheduledSparkApplication) (bool, error) {
	switch app.Spec.ConcurrencyPolicy {
	case v1alpha1.ConcurrencyAllow:
		return true, nil
	case v1alpha1.ConcurrencyForbid:
		finished, _, err := c.hasLastRunFinished(app.Namespace, app.Status.LastRunName)
		if err != nil {
			return false, err
		}
		return finished, nil
	case v1alpha1.ConcurrencyReplace:
		if err := c.killLastRunIfNotFinished(app.Namespace, app.Status.LastRunName); err != nil {
			return false, err
		}
		return true, nil
	}
	return true, nil
}

func (c *Controller) startNextRun(
	app *v1alpha1.ScheduledSparkApplication,
	status *v1alpha1.ScheduledSparkApplicationStatus,
	schedule cron.Schedule) error {
	glog.Infof("Next run of ScheduledSparkApplication %s/%s is due, creating a new SparkApplication instance", app.Namespace, app.Name)
	now := c.clock.Now()
	name, err := c.createSparkApplication(app, now)
	if err != nil {
		glog.Errorf("failed to create a SparkApplication instance for ScheduledSparkApplication %s/%s: %v", app.Namespace, app.Name, err)
		return err
	}

	status.LastRun = metav1.NewTime(now)
	status.NextRun = metav1.NewTime(schedule.Next(status.LastRun.Time))
	status.LastRunName = name
	return nil
}

func (c *Controller) hasLastRunFinished(
	namespace string,
	lastRunName string) (bool, *v1alpha1.SparkApplication, error) {
	app, err := c.saLister.SparkApplications(namespace).Get(lastRunName)
	if err != nil {
		// The SparkApplication of the last run may have been deleted already (e.g., manually by the user).
		if errors.IsNotFound(err) {
			return true, nil, nil
		}
		return false, nil, err
	}

	return app.Status.AppState.State == v1alpha1.CompletedState ||
		app.Status.AppState.State == v1alpha1.FailedState, app, nil
}

func (c *Controller) killLastRunIfNotFinished(namespace string, lastRunName string) error {
	finished, app, err := c.hasLastRunFinished(namespace, lastRunName)
	if err != nil {
		return err
	}
	if app == nil || finished {
		return nil
	}

	// Delete the SparkApplication object of the last run.
	if err = c.crdClient.SparkoperatorV1alpha1().SparkApplications(namespace).Delete(lastRunName,
		metav1.NewDeleteOptions(0)); err != nil {
		return err
	}

	return nil
}

func (c *Controller) checkAndUpdatePastRuns(
	app *v1alpha1.ScheduledSparkApplication,
	status *v1alpha1.ScheduledSparkApplicationStatus) error {
	lastRunName := status.LastRunName
	lastRunApp, err := c.saLister.SparkApplications(app.Namespace).Get(lastRunName)
	if err != nil {
		// The SparkApplication of the last run may have been deleted already (e.g., manually by the user).
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	var toDelete []string
	if lastRunApp.Status.AppState.State == v1alpha1.CompletedState {
		limit := 1
		if app.Spec.SuccessfulRunHistoryLimit != nil {
			limit = int(*app.Spec.SuccessfulRunHistoryLimit)
		}
		status.PastSuccessfulRunNames, toDelete = recordPastRuns(status.PastSuccessfulRunNames, lastRunName, limit)
	} else if lastRunApp.Status.AppState.State == v1alpha1.FailedState {
		limit := 1
		if app.Spec.FailedRunHistoryLimit != nil {
			limit = int(*app.Spec.FailedRunHistoryLimit)
		}
		status.PastFailedRunNames, toDelete = recordPastRuns(status.PastFailedRunNames, lastRunName, limit)
	}

	for _, name := range toDelete {
		c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Delete(name,
			metav1.NewDeleteOptions(0))
	}

	return nil
}

func (c *Controller) updateScheduledSparkApplicationStatus(
	app *v1alpha1.ScheduledSparkApplication,
	newStatus *v1alpha1.ScheduledSparkApplicationStatus) error {
	// If the status has not changed, do not perform an update.
	if isStatusEqual(newStatus, &app.Status) {
		return nil
	}

	toUpdate := app.DeepCopy()
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		toUpdate.Status = *newStatus
		_, updateErr := c.crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(toUpdate.Namespace).Update(
			toUpdate)
		if updateErr == nil {
			return nil
		}

		result, err := c.crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(toUpdate.Namespace).Get(
			toUpdate.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		toUpdate = result

		return updateErr
	})
}

func recordPastRuns(names []string, newName string, limit int) (updatedNames []string, toDelete []string) {
	if len(names) > 0 && names[0] == newName {
		// The name has already been recorded.
		return names, nil
	}

	rest := names
	if len(names) >= limit {
		rest = names[:limit-1]
		toDelete = names[limit-1:]
	}
	// Pre-append the name of the latest run.
	updatedNames = append([]string{newName}, rest...)
	return
}

func isStatusEqual(newStatus, currentStatus *v1alpha1.ScheduledSparkApplicationStatus) bool {
	return newStatus.ScheduleState == currentStatus.ScheduleState &&
		newStatus.LastRun == currentStatus.LastRun &&
		newStatus.NextRun == currentStatus.NextRun &&
		newStatus.LastRunName == currentStatus.LastRunName &&
		reflect.DeepEqual(newStatus.PastSuccessfulRunNames, currentStatus.PastSuccessfulRunNames) &&
		reflect.DeepEqual(newStatus.PastFailedRunNames, currentStatus.PastFailedRunNames) &&
		newStatus.Reason == currentStatus.Reason
}
