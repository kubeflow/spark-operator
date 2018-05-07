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

	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	crdclientset "k8s.io/spark-on-k8s-operator/pkg/client/clientset/versioned"
	crdscheme "k8s.io/spark-on-k8s-operator/pkg/client/clientset/versioned/scheme"
	crdinformers "k8s.io/spark-on-k8s-operator/pkg/client/informers/externalversions"
	crdlisters "k8s.io/spark-on-k8s-operator/pkg/client/listers/sparkoperator.k8s.io/v1alpha1"
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
	lister           crdlisters.ScheduledSparkApplicationLister
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
	controller.lister = informer.Lister()

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
	app, err := c.lister.ScheduledSparkApplications(namespace).Get(name)
	if err != nil {
		return err
	}

	if app.Spec.Suspend != nil && *app.Spec.Suspend {
		return nil
	}

	glog.V(2).Infof("Syncing ScheduledSparkApplication %s", app.Name)
	status := app.Status.DeepCopy()
	schedule, err := cron.ParseStandard(app.Spec.Schedule)
	if err != nil {
		glog.Errorf("failed to parse schedule %s of %s: %v", app.Spec.Schedule, app.Name, err)
		status.ScheduleState = v1alpha1.FailedValidationState
		status.Reason = err.Error()
	} else {
		status.ScheduleState = v1alpha1.ScheduledState
		nextRunTime := status.NextRun.Time
		if nextRunTime.IsZero() {
			nextRunTime = schedule.Next(status.LastRun.Time)
			status.NextRun = metav1.NewTime(nextRunTime)
		}
		now := c.clock.Now()
		if nextRunTime.Before(now) {
			// The next run is due. Check if this is the first run of the application.
			if len(status.PastRunNames) == 0 {
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
					if err = c.startNextRun(app, status, schedule); err != nil {
						return err
					}
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
		finished, _, err := c.hasLastRunFinished(app.Namespace, app.Status.PastRunNames[0])
		if err != nil {
			return false, err
		}
		return finished, nil
	case v1alpha1.ConcurrencyReplace:
		if err := c.killLastRunIfNotFinished(app.Namespace, app.Status.PastRunNames[0]); err != nil {
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
	glog.Infof("Next run of %s is due, creating a new SparkApplication instance", app.Name)
	now := metav1.Now()
	name, err := c.createSparkApplication(app, now.Time)
	if err != nil {
		glog.Errorf("failed to create a SparkApplication instance for %s: %v", app.Name, err)
		return err
	}

	status.LastRun = now
	status.NextRun = metav1.NewTime(schedule.Next(status.LastRun.Time))

	var limit int32 = 1
	if app.Spec.RunHistoryLimit != nil {
		limit = *app.Spec.RunHistoryLimit
	}

	rest := status.PastRunNames
	var toDelete []string
	if int32(len(status.PastRunNames)) >= limit {
		rest = status.PastRunNames[:limit-1]
		toDelete = status.PastRunNames[limit-1:]
	}
	// Pre-append the name of the latest run.
	status.PastRunNames = append([]string{name}, rest...)

	namespace := app.Namespace
	// Delete runs that should no longer be kept.
	for _, name := range toDelete {
		c.crdClient.SparkoperatorV1alpha1().SparkApplications(namespace).Delete(name, metav1.NewDeleteOptions(0))
	}

	return nil
}

func (c *Controller) hasLastRunFinished(
	namespace string,
	lastRunName string) (bool, *v1alpha1.SparkApplication, error) {
	app, err := c.crdClient.SparkoperatorV1alpha1().SparkApplications(namespace).Get(lastRunName, metav1.GetOptions{})
	if err != nil {
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

	// Delete the driver pod of the last run if applicable.
	if app.Status.DriverInfo.PodName != "" {
		if err = c.kubeClient.CoreV1().Pods(namespace).Delete(app.Status.DriverInfo.PodName,
			metav1.NewDeleteOptions(0)); err != nil {
			return err
		}
	}

	// Delete the SparkApplication object of the last run.
	if err = c.crdClient.SparkoperatorV1alpha1().SparkApplications(namespace).Delete(lastRunName,
		metav1.NewDeleteOptions(0)); err != nil {
		return err
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

func isStatusEqual(newStatus, currentStatus *v1alpha1.ScheduledSparkApplicationStatus) bool {
	return newStatus.ScheduleState == currentStatus.ScheduleState &&
		newStatus.LastRun == currentStatus.LastRun &&
		newStatus.NextRun == currentStatus.NextRun &&
		reflect.DeepEqual(newStatus.PastRunNames, currentStatus.PastRunNames) &&
		newStatus.Reason == currentStatus.Reason
}
