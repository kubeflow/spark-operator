/*
Copyright 2017 Google LLC

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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta1"
	crdclientset "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/util"
	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/lyft/flytestdlib/contextutils"
	v1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	v1beta12 "k8s.io/api/extensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/client-go/tools/record"
	"os/exec"
	//"reflect"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	//"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	//"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	"time"
)

const (
	sparkExecutorIDLabel      = "spark-exec-id"
	podAlreadyExistsErrorCode = "code=409"
	maximumUpdateRetries      = 3

	Deployment = "Deployment"
	Pod        = "Pod"
	Service    = "Service"
	Endpoints  = "Endpoints"
	Ingress    = "Ingress"
)

var (
	execCommand = exec.Command
	logger      = ctrl.Log.WithName("sparkapp-controller")
)

// Add creates a new SparkApplication Controller and adds it to the Manager with default RBAC. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager, metricsConfig *util.MetricConfig) error {
	return add(mgr, newReconciler(mgr, metricsConfig))
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	//p := predicate.Funcs{
	//	UpdateFunc: func(e event.UpdateEvent) bool {
	//
	//		oldObject := e.ObjectOld.(*v1beta1.SparkApplication)
	//		newObject := e.ObjectNew.(*v1beta1.SparkApplication)
	//		if !reflect.DeepEqual (oldObject.Status, newObject.Status) {
	//			// NO enqueue request
	//			return false
	//		}
	//		// ENQUEUE request
	//		return true
	//	},
	//}
	// Create a new controller
	c, err := controller.New("sparkapplication-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to SparkApplication
	//err = c.Watch(&source.Kind{Type: &v1beta1.SparkApplication{}}, &handler.EnqueueRequestForObject{}, p)
	err = c.Watch(&source.Kind{Type: &v1beta1.SparkApplication{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// Watch for Pod created by SparkApplication
	err = c.Watch(&source.Kind{Type: &apiv1.Pod{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &v1beta1.SparkApplication{},
	})
	if err != nil {
		return err
	}

	return nil
}

func newReconciler(mgr manager.Manager, metricsConfig *util.MetricConfig) reconcile.Reconciler {
	reconciler := &ReconcileSparkApplication{
		client:   mgr.GetClient(),
		scheme:   mgr.GetScheme(),
		recorder: mgr.GetEventRecorderFor("spark-operator"),
	}

	if metricsConfig != nil {
		reconciler.metrics = newSparkAppMetrics(metricsConfig.MetricsPrefix, metricsConfig.MetricsLabels)
		reconciler.metrics.registerMetrics()
	}
	return reconciler
}

var _ reconcile.Reconciler = &ReconcileSparkApplication{}

// ReconcileSparkApplication reconciles a SparkApplication object
type ReconcileSparkApplication struct {
	client           client.Client
	scheme           *runtime.Scheme
	metrics          *sparkAppMetrics
	recorder         record.EventRecorder
	crdClient        crdclientset.Interface
	ingressURLFormat string
}

func (r *ReconcileSparkApplication) recordSparkApplicationEvent(app *v1beta1.SparkApplication) {
	switch app.Status.AppState.State {
	case v1beta1.NewState:
		r.recorder.Eventf(
			app,
			apiv1.EventTypeNormal,
			"SparkApplicationAdded",
			"SparkApplication %s was added, enqueuing it for submission",
			app.Name)
	case v1beta1.SubmittedState:
		r.recorder.Eventf(
			app,
			apiv1.EventTypeNormal,
			"SparkApplicationSubmitted",
			"SparkApplication %s was submitted successfully",
			app.Name)
	case v1beta1.FailedSubmissionState:
		r.recorder.Eventf(
			app,
			apiv1.EventTypeWarning,
			"SparkApplicationSubmissionFailed",
			"failed to submit SparkApplication %s: %s",
			app.Name,
			app.Status.AppState.ErrorMessage)
	case v1beta1.CompletedState:
		r.recorder.Eventf(
			app,
			apiv1.EventTypeNormal,
			"SparkApplicationCompleted",
			"SparkApplication %s completed",
			app.Name)
	case v1beta1.FailedState:
		r.recorder.Eventf(
			app,
			apiv1.EventTypeWarning,
			"SparkApplicationFailed",
			"SparkApplication %s failed: %s",
			app.Name,
			app.Status.AppState.ErrorMessage)
	case v1beta1.PendingRerunState:
		r.recorder.Eventf(
			app,
			apiv1.EventTypeWarning,
			"SparkApplicationPendingRerun",
			"SparkApplication %s is pending rerun",
			app.Name)
	}
}

func (r *ReconcileSparkApplication) recordDriverEvent(app *v1beta1.SparkApplication, phase apiv1.PodPhase, name string) {
	switch phase {
	case apiv1.PodSucceeded:
		r.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkDriverCompleted", "Driver %s completed", name)
	case apiv1.PodPending:
		r.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkDriverPending", "Driver %s is pending", name)
	case apiv1.PodRunning:
		r.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkDriverRunning", "Driver %s is running", name)
	case apiv1.PodFailed:
		r.recorder.Eventf(app, apiv1.EventTypeWarning, "SparkDriverFailed", "Driver %s failed", name)
	case apiv1.PodUnknown:
		r.recorder.Eventf(app, apiv1.EventTypeWarning, "SparkDriverUnknownState", "Driver %s in unknown state", name)
	}
}

func (r *ReconcileSparkApplication) recordExecutorEvent(app *v1beta1.SparkApplication, state v1beta1.ExecutorState, name string) {
	switch state {
	case v1beta1.ExecutorCompletedState:
		r.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkExecutorCompleted", "Executor %s completed", name)
	case v1beta1.ExecutorPendingState:
		r.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkExecutorPending", "Executor %s is pending", name)
	case v1beta1.ExecutorRunningState:
		r.recorder.Eventf(app, apiv1.EventTypeNormal, "SparkExecutorRunning", "Executor %s is running", name)
	case v1beta1.ExecutorFailedState:
		r.recorder.Eventf(app, apiv1.EventTypeWarning, "SparkExecutorFailed", "Executor %s failed", name)
	case v1beta1.ExecutorUnknownState:
		r.recorder.Eventf(app, apiv1.EventTypeWarning, "SparkExecutorUnknownState", "Executor %s in unknown state", name)
	}
}

// submitSparkApplication creates a new submission for the given SparkApplication and submits it using spark-submit.
func (r *ReconcileSparkApplication) submitSparkApplication(ctx context.Context, app *v1beta1.SparkApplication) *v1beta1.SparkApplication {
	if app.PrometheusMonitoringEnabled() {
		if err := configPrometheusMonitoring(ctx, app, r.client); err != nil {
			logger.Error(err, "Error in configuring Prometheus monitoring")
		}
	}

	driverPodName := getDriverPodName(app)
	submissionID := uuid.New().String()
	submissionCmdArgs, err := buildSubmissionCommandArgs(app, driverPodName, submissionID)
	if err != nil {
		app.Status = v1beta1.SparkApplicationStatus{
			AppState: v1beta1.ApplicationState{
				State:        v1beta1.FailedSubmissionState,
				ErrorMessage: err.Error(),
			},
			SubmissionAttempts:        app.Status.SubmissionAttempts + 1,
			LastSubmissionAttemptTime: metav1.Now(),
		}
		return app
	}

	// Try submitting the application by running spark-submit.
	submitted, err := runSparkSubmit(newSubmission(submissionCmdArgs, app))
	if err != nil {
		app.Status = v1beta1.SparkApplicationStatus{
			AppState: v1beta1.ApplicationState{
				State:        v1beta1.FailedSubmissionState,
				ErrorMessage: err.Error(),
			},
			SubmissionAttempts:        app.Status.SubmissionAttempts + 1,
			LastSubmissionAttemptTime: metav1.Now(),
		}
		r.recordSparkApplicationEvent(app)
		logger.Error(err, "failed to run spark-submit for SparkApplication",
			"appNamespace", app.Namespace, "appName", app.Name)
		return app
	}
	if !submitted {
		// The application may not have been submitted even if err == nil, e.g., when some
		// state update caused an attempt to re-submit the application, in which case no
		// error gets returned from runSparkSubmit. If this is the case, we simply return.
		return app
	}

	logger.Info("SparkApplication has been submitted", "appNamespace", app.Namespace, "appName", app.Name)
	app.Status = v1beta1.SparkApplicationStatus{
		SubmissionID: submissionID,
		AppState: v1beta1.ApplicationState{
			State: v1beta1.SubmittedState,
		},
		DriverInfo: v1beta1.DriverInfo{
			PodName: driverPodName,
		},
		SubmissionAttempts:        app.Status.SubmissionAttempts + 1,
		ExecutionAttempts:         app.Status.ExecutionAttempts + 1,
		LastSubmissionAttemptTime: metav1.Now(),
	}
	driverPod, err := r.getDriverPod(ctx, app)
	if err != nil {
		logger.Error(err, "Unable to get driver pod in order to set owner reference")
	} else {
		if err = ctrl.SetControllerReference(app, driverPod, r.scheme); err != nil {
			logger.Error(err, "Setting owner reference of driver pod failed")
		} else {
			if err = r.client.Update(ctx, driverPod); err != nil {
				logger.Error(err, "Failed to update driver pod with owner reference at the K8s API server")
			} else {
				logger.Info("driverPod updated with owner reference at the K8s API server", "appNamespace", app.Namespace, "appName", app.Name)
			}
		}
	}
	r.recordSparkApplicationEvent(app)

	service, err := createSparkUIService(ctx, r.client, app)
	if err != nil {
		logger.Error(err, "failed to create UI service for SparkApplication",
			"appNamespace", app.Namespace, "appName", app.Name)
	} else {
		app.Status.DriverInfo.WebUIServiceName = service.serviceName
		app.Status.DriverInfo.WebUIPort = service.servicePort
		app.Status.DriverInfo.WebUIAddress = fmt.Sprintf("%s:%d", service.serviceIP, app.Status.DriverInfo.WebUIPort)
		// Create UI Ingress if ingress-format is set.
		if r.ingressURLFormat != "" {
			ingress, err := createSparkUIIngress(ctx, app, *service, r.ingressURLFormat, r.client)
			if err != nil {
				logger.Error(err, "failed to create UI Ingress for SparkApplication",
					"appNamespace", app.Namespace, "appName", app.Name)
			} else {
				app.Status.DriverInfo.WebUIIngressAddress = ingress.ingressURL
				app.Status.DriverInfo.WebUIIngressName = ingress.ingressName
			}
		}
	}
	return app
}

func (r *ReconcileSparkApplication) updateApplicationStatusWithRetries(
	ctx context.Context,
	original *v1beta1.SparkApplication,
	updateFunc func(status *v1beta1.SparkApplicationStatus)) (*v1beta1.SparkApplication, error) {
	toUpdate := original.DeepCopy()

	var lastUpdateErr error
	for i := 0; i < maximumUpdateRetries; i++ {
		updateFunc(&toUpdate.Status)
		if equality.Semantic.DeepEqual(original.Status, toUpdate.Status) {
			return toUpdate, nil
		}

		err := r.client.Update(ctx, toUpdate)
		if err == nil {
			return toUpdate, nil
		}

		lastUpdateErr = err

		// Failed to update to the API server.
		// Get the latest version from the API server first and re-apply the update.
		toUpdate, err = r.getSparkApplication(ctx, toUpdate.Namespace, toUpdate.Name)
		if err != nil {
			logger.Error(err, "failed to get SparkApplication", "namespace", original.Namespace, "name", toUpdate.Name)
			return nil, err
		}
	}

	if lastUpdateErr != nil {
		logger.Error(lastUpdateErr, "failed to update SparkApplication", "namespace", original.Namespace, "name", original.Name)
		return nil, lastUpdateErr
	}

	return toUpdate, nil
}

// updateStatusAndExportMetrics updates the status of the SparkApplication and export the metrics.
func (r *ReconcileSparkApplication) updateStatusAndExportMetrics(ctx context.Context, oldApp, newApp *v1beta1.SparkApplication) error {
	// Skip update if nothing changed.
	if equality.Semantic.DeepEqual(oldApp, newApp) {
		return nil
	}

	updatedApp, err := r.updateApplicationStatusWithRetries(ctx, oldApp, func(status *v1beta1.SparkApplicationStatus) {
		*status = newApp.Status
	})

	// Export metrics if the update was successful.
	if err == nil && r.metrics != nil {
		r.metrics.exportMetrics(oldApp, updatedApp)
	}

	return err
}

func (r *ReconcileSparkApplication) getSparkApplication(ctx context.Context, namespace string, name string) (*v1beta1.SparkApplication, error) {
	namespacedName := types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}
	sparkapp := &v1beta1.SparkApplication{}
	err := r.client.Get(ctx, namespacedName, sparkapp)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return sparkapp, nil
}

// Delete the driver pod and optional UI resources (Service/Ingress) created for the application.
func (r *ReconcileSparkApplication) deleteSparkResources(ctx context.Context, app *v1beta1.SparkApplication) error {
	driverPodName := app.Status.DriverInfo.PodName
	if driverPodName != "" {
		logger.V(2).Info("Deleting pod in namespace", "podName", driverPodName, "namespace", app.Namespace)
		pod, err := r.getDriverPod(ctx, app)
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
		err = r.client.Delete(ctx, pod, client.GracePeriodSeconds(0))
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	sparkUIServiceName := app.Status.DriverInfo.WebUIServiceName
	if sparkUIServiceName != "" {
		logger.V(2).Info("Deleting Spark UI Service in namespace", "serviceName", sparkUIServiceName, "namespace", app.Namespace)
		namespacedName := types.NamespacedName{
			Namespace: app.Namespace,
			Name:      sparkUIServiceName,
		}
		svc := &apiv1.Service{}
		err := r.client.Get(ctx, namespacedName, svc)
		if err != nil && !errors.IsNotFound(err) {
			return err
		}

		err = r.client.Delete(ctx, svc, client.GracePeriodSeconds(0))
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	sparkUIIngressName := app.Status.DriverInfo.WebUIIngressName
	if sparkUIIngressName != "" {
		logger.V(2).Info("Deleting Spark UI Ingress in namespace", "ingressName", sparkUIIngressName, "namespace", app.Namespace)
		namespacedName := types.NamespacedName{
			Namespace: app.Namespace,
			Name:      sparkUIIngressName,
		}
		ingress := &v1beta12.Ingress{}
		err := r.client.Get(ctx, namespacedName, ingress)
		if err != nil && !errors.IsNotFound(err) {
			return err
		}

		err = r.client.Delete(ctx, ingress, client.GracePeriodSeconds(0))
		if err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	return nil
}

func (r *ReconcileSparkApplication) validateSparkApplication(app *v1beta1.SparkApplication) error {
	appSpec := app.Spec
	driverSpec := appSpec.Driver
	executorSpec := appSpec.Executor
	if appSpec.NodeSelector != nil && (driverSpec.NodeSelector != nil || executorSpec.NodeSelector != nil) {
		return fmt.Errorf("NodeSelector property can be defined at SparkApplication or at any of Driver,Executor")
	}

	return nil
}

// Validate that any Spark resources (driver/Service/Ingress) created for the application have been deleted.
func (r *ReconcileSparkApplication) validateSparkResourceDeletion(ctx context.Context, app *v1beta1.SparkApplication) bool {
	driverPodName := app.Status.DriverInfo.PodName
	if driverPodName != "" {
		namespacedName := types.NamespacedName{
			Namespace: app.Namespace,
			Name:      driverPodName,
		}
		pod := &apiv1.Pod{}
		err := r.client.Get(ctx, namespacedName, pod)
		if err == nil || !errors.IsNotFound(err) {
			return false
		}
	}

	sparkUIServiceName := app.Status.DriverInfo.WebUIServiceName
	if sparkUIServiceName != "" {
		namespacedName := types.NamespacedName{
			Namespace: app.Namespace,
			Name:      sparkUIServiceName,
		}
		svc := &apiv1.Service{}
		err := r.client.Get(ctx, namespacedName, svc)
		if err == nil || !errors.IsNotFound(err) {
			return false
		}
	}

	sparkUIIngressName := app.Status.DriverInfo.WebUIIngressName
	if sparkUIIngressName != "" {
		namespacedName := types.NamespacedName{
			Namespace: app.Namespace,
			Name:      sparkUIIngressName,
		}
		ingress := &v1beta12.Ingress{}
		err := r.client.Get(ctx, namespacedName, ingress)
		if err == nil || !errors.IsNotFound(err) {
			return false
		}
	}

	return true
}

func (r *ReconcileSparkApplication) clearStatus(status *v1beta1.SparkApplicationStatus) {
	if status.AppState.State == v1beta1.InvalidatingState {
		status.SparkApplicationID = ""
		status.SubmissionAttempts = 0
		status.ExecutionAttempts = 0
		status.LastSubmissionAttemptTime = metav1.Time{}
		status.TerminationTime = metav1.Time{}
		status.AppState.ErrorMessage = ""
		status.ExecutorState = nil
		status.AppHash = ""
	} else if status.AppState.State == v1beta1.PendingRerunState {
		status.SparkApplicationID = ""
		status.SubmissionAttempts = 0
		status.LastSubmissionAttemptTime = metav1.Time{}
		status.DriverInfo = v1beta1.DriverInfo{}
		status.AppState.ErrorMessage = ""
		status.ExecutorState = nil
		status.AppHash = ""
	}
}

func (r *ReconcileSparkApplication) calculateSparkAppSpecHash(spec v1beta1.SparkApplicationSpec) string {
	reqBodyBytes := new(bytes.Buffer)
	_ = json.NewEncoder(reqBodyBytes).Encode(spec)
	sha256s := sha256.Sum256(reqBodyBytes.Bytes())
	return hex.EncodeToString(sha256s[:])
}

// State Machine for SparkApplication:
//+--------------------------------------------------------------------------------------------------------------------+
//|                                                                                                                    |
//|                +---------+                                                                                         |
//|                |         |                                                                                         |
//|                |         +                                                                                         |
//|                |Submission                                                                                         |
//|           +----> Failed  +-----+------------------------------------------------------------------+                |
//|           |    |         |     |                                                                  |                |
//|           |    |         |     |                                                                  |                |
//|           |    +----^----+     |                                                                  |                |
//|           |         |          |                                                                  |                |
//|           |         |          |                                                                  |                |
//|      +----+----+    |    +-----v----+          +----------+           +-----------+          +----v-----+          |
//|      |         |    |    |          |          |          |           |           |          |          |          |
//|      |         |    |    |          |          |          |           |           |          |		    |          |
//|      |   New   +---------> Submitted+----------> Running  +----------->  Failing  +---------->  Failed  |          |
//|      |         |    |    |          |          |          |           |           |          |          |          |
//|      |         |    |    |          |          |          |           |           |          |          |          |
//|      |         |    |    |          |          |          |           |           |          |          |          |
//|      +---------+    |    +----^-----+          +-----+----+           +-----+-----+          +----------+          |
//|                     |         |                      |                      |                                      |
//|                     |         |                      |                      |                                      |
//|    +------------+   |         |             +-------------------------------+                                      |
//|    |            |   |   +-----+-----+       |        |                +-----------+          +----------+          |
//|    |            |   |   |  Pending  |       |        |                |           |          |          |          |
//|    |            |   +---+   Rerun   <-------+        +---------------->Succeeding +---------->Completed |          |
//|    |Invalidating|       |           <-------+                         |           |          |          |          |
//|    |            +------->           |       |                         |           |          |          |          |
//|    |            |       |           |       |                         |           |          |          |          |
//|    |            |       +-----------+       |                         +-----+-----+          +----------+          |
//|    +------------+                           |                               |                                      |
//|                                             |                               |                                      |
//|                                             +-------------------------------+                                      |
//|                                                                                                                    |
//+--------------------------------------------------------------------------------------------------------------------+

func (r *ReconcileSparkApplication) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	ctx := context.Background()
	ctx = contextutils.WithNamespace(ctx, request.Namespace)
	ctx = contextutils.WithAppName(ctx, request.Name)

	logger.Info("Reconciling object with", "name", request.Name, "ns", request.Namespace)

	// Fetch the SparkApplication
	sparkapp := &v1beta1.SparkApplication{}
	err := r.client.Get(ctx, request.NamespacedName, sparkapp)
	if err != nil {
		if errors.IsNotFound(err) {
			// Object not found, return. Created objects are automatically garbage collected.
			// For additional cleanup logic use finalizers.
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	appToUpdate := sparkapp.DeepCopy()
	calculatedAppHash := r.calculateSparkAppSpecHash((*appToUpdate).Spec)
	logger.Info("Current Spark app state", "appState", appToUpdate.Status.AppState.State)

	shouldInvalidate := appToUpdate.Status.AppHash != "" && calculatedAppHash != appToUpdate.Status.AppHash &&
		appToUpdate.Status.AppState.State != v1beta1.InvalidatingState

	if shouldInvalidate {
		logger.Info("Setting to invalidating state", "app", appToUpdate.Name)
		if _, err := r.updateApplicationStatusWithRetries(ctx, appToUpdate, func(status *v1beta1.SparkApplicationStatus) {
			status.AppState.State = v1beta1.InvalidatingState
		}); err != nil {
			return reconcile.Result{}, err
		}
	} else {
		switch appToUpdate.Status.AppState.State {
		case v1beta1.NewState:
			v1beta1.SetSparkApplicationDefaults(appToUpdate)
			logger.Info("SparkApplication was added", "namespace", appToUpdate.Namespace, "name", appToUpdate.Name)
			r.recordSparkApplicationEvent(appToUpdate)
			if err := r.validateSparkApplication(appToUpdate); err != nil {
				appToUpdate.Status.AppState.State = v1beta1.FailedState
				appToUpdate.Status.AppState.ErrorMessage = err.Error()
			} else {
				appToUpdate = r.submitSparkApplication(ctx, appToUpdate)
			}
		case v1beta1.SucceedingState:
			if !shouldRetry(appToUpdate) {
				// Application is not subject to retry. Move to terminal CompletedState.
				appToUpdate.Status.AppState.State = v1beta1.CompletedState
				r.recordSparkApplicationEvent(appToUpdate)
			} else {
				if err := r.deleteSparkResources(ctx, appToUpdate); err != nil {
					logger.Error(err, "failed to delete resources associated with SparkApplication",
						"namespace", appToUpdate.Namespace, "name", appToUpdate.Name)
					return reconcile.Result{}, err
				}
				appToUpdate.Status.AppState.State = v1beta1.PendingRerunState
			}
		case v1beta1.FailingState:
			if !shouldRetry(appToUpdate) {
				// Application is not subject to retry. Move to terminal FailedState.
				appToUpdate.Status.AppState.State = v1beta1.FailedState
				r.recordSparkApplicationEvent(appToUpdate)
			} else if hasRetryIntervalPassed(appToUpdate.Spec.RestartPolicy.OnFailureRetryInterval, appToUpdate.Status.ExecutionAttempts, appToUpdate.Status.TerminationTime) {
				if err := r.deleteSparkResources(ctx, appToUpdate); err != nil {
					logger.Error(err, "failed to delete resources associated with SparkApplication",
						"namespace", appToUpdate.Namespace, "name", appToUpdate.Name)
					return reconcile.Result{}, err
				}
				appToUpdate.Status.AppState.State = v1beta1.PendingRerunState
			}
		case v1beta1.FailedSubmissionState:
			if !shouldRetry(appToUpdate) {
				// App will never be retried. Move to terminal FailedState.
				appToUpdate.Status.AppState.State = v1beta1.FailedState
				r.recordSparkApplicationEvent(appToUpdate)
			} else if hasRetryIntervalPassed(appToUpdate.Spec.RestartPolicy.OnSubmissionFailureRetryInterval, appToUpdate.Status.SubmissionAttempts, appToUpdate.Status.LastSubmissionAttemptTime) {
				appToUpdate = r.submitSparkApplication(ctx, appToUpdate)
			}
		case v1beta1.InvalidatingState:
			// Invalidate the current run and enqueue the SparkApplication for re-execution.
			if err := r.deleteSparkResources(ctx, appToUpdate); err != nil {
				logger.Error(err, "failed to delete resources associated with SparkApplication",
					"namespace", appToUpdate.Namespace, "name", appToUpdate.Name)
				return reconcile.Result{}, err
			}
			r.clearStatus(&appToUpdate.Status)
			appToUpdate.Status.AppState.State = v1beta1.PendingRerunState
		case v1beta1.PendingRerunState:
			logger.V(2).Info("SparkApplication pending rerun", "namespace", appToUpdate.Namespace, "name", appToUpdate.Name)
			if r.validateSparkResourceDeletion(ctx, appToUpdate) {
				logger.V(2).Info("Resources for SparkApplication successfully deleted", "namespace", appToUpdate.Namespace, "name", appToUpdate.Name)
				r.recordSparkApplicationEvent(appToUpdate)
				r.clearStatus(&appToUpdate.Status)
				appToUpdate = r.submitSparkApplication(ctx, appToUpdate)
			}
		case v1beta1.RunningState:

			if err := r.getAndUpdateAppState(ctx, appToUpdate); err != nil {
				return reconcile.Result{}, err
			}
		case v1beta1.SubmittedState, v1beta1.UnknownState:
			if err := r.getAndUpdateAppState(ctx, appToUpdate); err != nil {
				return reconcile.Result{}, err
			}
		}
	}

	if appToUpdate != nil {
		if appToUpdate.Status.AppHash == "" {
			appToUpdate.Status.AppHash = r.calculateSparkAppSpecHash((*appToUpdate).Spec)
		}

		logger.Info("Trying to update SparkApplication", "namespace", sparkapp.Namespace, "name", sparkapp.Name,
			"fromStatus", sparkapp.Status, "toStatus", appToUpdate.Status)
		err = r.updateStatusAndExportMetrics(ctx, sparkapp, appToUpdate)
		if err != nil {
			glog.Errorf("failed to update SparkApplication %s/%s: %v", appToUpdate.Namespace, appToUpdate.Name, err)
			return reconcile.Result{}, err
		}
	}

	return reconcile.Result{}, nil
}

func (r *ReconcileSparkApplication) getExecutorPods(ctx context.Context, app *v1beta1.SparkApplication) (*apiv1.PodList, error) {
	matchLabels := getResourceLabels(app)
	matchLabels[config.SparkRoleLabel] = config.SparkExecutorRole

	// Fetch all the executor pods for the current run of the application.
	podList := &apiv1.PodList{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       Pod,
		},
	}

	err := r.client.List(ctx, podList, client.MatchingLabels(matchLabels), client.InNamespace(app.Namespace))
	if err != nil {
		return nil, fmt.Errorf("failed to get pods for SparkApplication %s/%s: %v", app.Namespace, app.Name, err)
	}
	return podList, nil
}

func (r *ReconcileSparkApplication) getDriverPod(ctx context.Context, app *v1beta1.SparkApplication) (*apiv1.Pod, error) {
	namespacedName := types.NamespacedName{
		Namespace: app.Namespace,
		Name:      app.Status.DriverInfo.PodName,
	}
	pod := &apiv1.Pod{}
	err := r.client.Get(ctx, namespacedName, pod)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, fmt.Errorf("driver pod %s: %v not found", app.Status.DriverInfo.PodName, err)
		}
		return nil, fmt.Errorf("failed to get driver pod %s: %v", app.Status.DriverInfo.PodName, err)
	}
	return pod, nil
}

// getAndUpdateDriverState finds the driver pod of the application
// and updates the driver state based on the current phase of the pod.
func (r *ReconcileSparkApplication) getAndUpdateDriverState(ctx context.Context, app *v1beta1.SparkApplication) error {
	// Either the driver pod doesn't exist yet or its name has not been updated.
	if app.Status.DriverInfo.PodName == "" {
		return fmt.Errorf("empty driver pod name with application state %s", app.Status.AppState.State)
	}

	driverPod, err := r.getDriverPod(ctx, app)
	if err != nil {
		return err
	}

	if driverPod == nil {
		app.Status.AppState.ErrorMessage = "Driver Pod not found"
		app.Status.AppState.State = v1beta1.FailingState
		app.Status.TerminationTime = metav1.Now()
		return nil
	}

	app.Status.SparkApplicationID = getSparkApplicationID(*driverPod)

	if driverPod.Status.Phase == apiv1.PodSucceeded || driverPod.Status.Phase == apiv1.PodFailed {
		if app.Status.TerminationTime.IsZero() {
			app.Status.TerminationTime = metav1.Now()
		}
		if driverPod.Status.Phase == apiv1.PodFailed {
			if len(driverPod.Status.ContainerStatuses) > 0 {
				terminatedState := driverPod.Status.ContainerStatuses[0].State.Terminated
				if terminatedState != nil {
					app.Status.AppState.ErrorMessage = fmt.Sprintf("driver pod failed with ExitCode: %d, Reason: %s", terminatedState.ExitCode, terminatedState.Reason)
				}
			} else {
				app.Status.AppState.ErrorMessage = "driver container status missing"
			}
		}
	}

	newState := driverStateToApplicationState(driverPod.Status)
	// Only record a driver event if the application state (derived from the driver pod phase) has changed.
	if newState != app.Status.AppState.State {
		r.recordDriverEvent(app, driverPod.Status.Phase, driverPod.Name)
	}
	app.Status.AppState.State = newState

	return nil
}

// getAndUpdateExecutorState lists the executor pods of the application
// and updates the executor state based on the current phase of the pods.
func (r *ReconcileSparkApplication) getAndUpdateExecutorState(ctx context.Context, app *v1beta1.SparkApplication) error {
	podList, err := r.getExecutorPods(ctx, app)
	if err != nil {
		return err
	}

	executorStateMap := make(map[string]v1beta1.ExecutorState)
	var executorApplicationID string
	for _, pod := range podList.Items {
		if util.IsExecutorPod(&pod) {
			newState := podPhaseToExecutorState(pod.Status.Phase)
			oldState, exists := app.Status.ExecutorState[pod.Name]
			// Only record an executor event if the executor state is new or it has changed.
			if !exists || newState != oldState {
				r.recordExecutorEvent(app, newState, pod.Name)
			}
			executorStateMap[pod.Name] = newState

			if executorApplicationID == "" {
				executorApplicationID = getSparkApplicationID(pod)
			}
		}
	}

	// ApplicationID label can be different on driver/executors. Prefer executor ApplicationID if set.
	// Refer https://issues.apache.org/jira/projects/SPARK/issues/SPARK-25922 for details.
	if executorApplicationID != "" {
		app.Status.SparkApplicationID = executorApplicationID
	}

	if app.Status.ExecutorState == nil {
		app.Status.ExecutorState = make(map[string]v1beta1.ExecutorState)
	}
	for name, execStatus := range executorStateMap {
		app.Status.ExecutorState[name] = execStatus
	}

	// Handle missing/deleted executors.
	for name, oldStatus := range app.Status.ExecutorState {
		_, exists := executorStateMap[name]
		if !isExecutorTerminated(oldStatus) && !exists {
			logger.Info("Executor pod not found, assuming it was deleted.", "podName", name)
			app.Status.ExecutorState[name] = v1beta1.ExecutorFailedState
		}
	}

	return nil
}

func (r *ReconcileSparkApplication) getAndUpdateAppState(ctx context.Context, app *v1beta1.SparkApplication) error {
	if err := r.getAndUpdateDriverState(ctx, app); err != nil {
		return err
	}
	if err := r.getAndUpdateExecutorState(ctx, app); err != nil {
		return err
	}
	return nil
}

func (r *ReconcileSparkApplication) handleSparkApplicationDeletion(ctx context.Context, app *v1beta1.SparkApplication) {
	// SparkApplication deletion requested, lets delete driver pod.
	if err := r.deleteSparkResources(ctx, app); err != nil {
		logger.Error(err, "failed to delete resources associated with deleted SparkApplication",
			"namespace", app.Namespace, "name", app.Name)
	}
}

// ShouldRetry determines if SparkApplication in a given state should be retried.
func shouldRetry(app *v1beta1.SparkApplication) bool {
	switch app.Status.AppState.State {
	case v1beta1.SucceedingState:
		return app.Spec.RestartPolicy.Type == v1beta1.Always
	case v1beta1.FailingState:
		if app.Spec.RestartPolicy.Type == v1beta1.Always {
			return true
		} else if app.Spec.RestartPolicy.Type == v1beta1.OnFailure {
			// We retry if we haven't hit the retry limit.
			if app.Spec.RestartPolicy.OnFailureRetries != nil && app.Status.ExecutionAttempts <= *app.Spec.RestartPolicy.OnFailureRetries {
				return true
			}
		}
	case v1beta1.FailedSubmissionState:
		if app.Spec.RestartPolicy.Type == v1beta1.Always {
			return true
		} else if app.Spec.RestartPolicy.Type == v1beta1.OnFailure {
			// We retry if we haven't hit the retry limit.
			if app.Spec.RestartPolicy.OnSubmissionFailureRetries != nil && app.Status.SubmissionAttempts <= *app.Spec.RestartPolicy.OnSubmissionFailureRetries {
				return true
			}
		}
	}
	return false
}

// Helper func to determine if we have waited enough to retry the SparkApplication.
func hasRetryIntervalPassed(retryInterval *int64, attemptsDone int32, lastEventTime metav1.Time) bool {
	logger.V(3).Info("Retry info",
		"retryInterval", retryInterval, "lastEventTime", lastEventTime, "attemptsDone", attemptsDone)
	if retryInterval == nil || lastEventTime.IsZero() || attemptsDone <= 0 {
		return false
	}

	// Retry if we have waited at-least equal to attempts*RetryInterval since we do a linear back-off.
	interval := time.Duration(*retryInterval) * time.Second * time.Duration(attemptsDone)
	currentTime := time.Now()
	logger.V(3).Info("Retry info", "currentTime", currentTime, "interval", interval)
	if currentTime.After(lastEventTime.Add(interval)) {
		return true
	}
	return false
}
