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

package sparkapplication

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	extensionsv1beta1 "k8s.io/api/extensions/v1beta1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/internal/batchscheduler"
	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
)

var (
	logger = log.Log.WithName("")
)

type Options struct {
	Namespaces       []string
	EnableUIService  bool
	IngressClassName string
	IngressURLFormat string
}

// SparkApplicationReconciler reconciles a SparkApplication object
type SparkApplicationReconciler struct {
	scheme   *runtime.Scheme
	client   client.Client
	recorder record.EventRecorder
	options  Options
	metrics  *Metrics
	manager  *batchscheduler.SchedulerManager
}

var _ reconcile.Reconciler = &SparkApplicationReconciler{}

func NewReconciler(
	scheme *runtime.Scheme,
	client client.Client,
	recorder record.EventRecorder,
	metrics *Metrics,
	manager *batchscheduler.SchedulerManager,
	options Options,
) *SparkApplicationReconciler {
	return &SparkApplicationReconciler{
		scheme:   scheme,
		client:   client,
		recorder: recorder,
		metrics:  metrics,
		manager:  manager,
		options:  options,
	}
}

// +kubebuilder:rbac:groups=sparkoperator.k8s.io,resources=sparkapplications,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sparkoperator.k8s.io,resources=sparkapplications/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=sparkoperator.k8s.io,resources=sparkapplications/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the SparkApplication object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.18.2/pkg/reconcile

// Reconcile handles Create, Update and Delete events of the custom resource
func (r *SparkApplicationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	key := req.NamespacedName
	app, err := r.getSparkApplication(key)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{Requeue: true}, err
	}
	logger.Info("Reconciling SparkApplication", "name", app.Name, "namespace", app.Namespace, "state", app.Status.AppState.State)

	// Check if the spark application is being deleted
	if !app.DeletionTimestamp.IsZero() && util.ContainsString(app.Finalizers, common.SparkApplicationFinalizerName) {
		return r.finalizeSparkApplication(ctx, req)
	}
	switch app.Status.AppState.State {
	case v1beta2.ApplicationStateNew:
		return r.reconcileNewSparkApplication(ctx, req)
	case v1beta2.ApplicationStateSubmitted:
		return r.reconcileSubmittedSparkApplication(ctx, req)
	case v1beta2.ApplicationStateFailedSubmission:
		return r.reconcileFailedSubmissionSparkApplication(ctx, req)
	case v1beta2.ApplicationStateRunning:
		return r.reconcileRunningSparkApplication(ctx, req)
	case v1beta2.ApplicationStatePendingRerun:
		return r.reconcilePendingRerunSparkApplication(ctx, req)
	case v1beta2.ApplicationStateInvalidating:
		return r.reconcileInvalidatingSparkApplication(ctx, req)
	case v1beta2.ApplicationStateSucceeding:
		return r.reconcileSucceedingSparkApplication(ctx, req)
	case v1beta2.ApplicationStateFailing:
		return r.reconcileFailingSparkApplication(ctx, req)
	case v1beta2.ApplicationStateCompleted:
		return r.reconcileCompletedSparkApplication(ctx, req)
	case v1beta2.ApplicationStateFailed:
		return r.reconcileFailedSparkApplication(ctx, req)
	case v1beta2.ApplicationStateUnknown:
		return r.reconcileUnknownSparkApplication(ctx, req)
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *SparkApplicationReconciler) SetupWithManager(mgr ctrl.Manager, options controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("spark-application-controller").
		Watches(
			&v1beta2.SparkApplication{},
			NewSparkApplicationEventHandler(),
			builder.WithPredicates(
				NewSparkApplicationEventFilter(
					mgr.GetClient(),
					mgr.GetEventRecorderFor("spark-application-event-handler"),
					r.options.Namespaces,
				),
			),
		).
		Watches(
			&corev1.Pod{},
			NewSparkPodEventHandler(mgr.GetCache()),
			builder.WithPredicates(newSparkPodEventFilter(r.options.Namespaces)),
		).
		WithOptions(options).
		Complete(r)
}

func (r *SparkApplicationReconciler) finalizeSparkApplication(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	key := req.NamespacedName
	if retryErr := retry.RetryOnConflict(
		retry.DefaultRetry,
		func() error {
			old, err := r.getSparkApplication(key)
			if err != nil {
				if errors.IsNotFound(err) {
					return nil
				}
				return err
			}
			if old.DeletionTimestamp.IsZero() || !util.ContainsString(old.Finalizers, common.SparkApplicationFinalizerName) {
				return nil
			}
			app := old.DeepCopy()

			if err := r.deleteSparkResources(app); err != nil {
				logger.Error(err, "Failed to delete resources associated with the SparkApplication", "name", app.Name, "namespace", app.Namespace)
				return err
			}

			// Remove our finalizer after cleaning up
			app.Finalizers = util.RemoveString(app.Finalizers, common.SparkApplicationFinalizerName)
			if err := r.updateSparkApplication(ctx, app); err != nil {
				return err
			}

			if r.metrics != nil {
				r.metrics.exportMetricsOnDeletion(app)
			}
			return nil
		},
	); retryErr != nil {
		logger.Error(retryErr, "Failed to finalize SparkApplication", "name", key.Name, "namespace", key.Namespace)
		return ctrl.Result{Requeue: true}, retryErr
	}
	return ctrl.Result{}, nil
}

func (r *SparkApplicationReconciler) reconcileNewSparkApplication(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	key := req.NamespacedName
	retryErr := retry.RetryOnConflict(
		retry.DefaultRetry,
		func() error {
			old, err := r.getSparkApplication(key)
			if err != nil {
				return err
			}
			if old.Status.AppState.State != v1beta2.ApplicationStateNew {
				return nil
			}
			app := old.DeepCopy()

			// If the application does not have our finalizer, add it.
			if !util.ContainsString(app.Finalizers, common.SparkApplicationFinalizerName) {
				app.Finalizers = append(app.Finalizers, common.SparkApplicationFinalizerName)
				if err := r.updateSparkApplication(ctx, app); err != nil {
					return err
				}
				return nil
			}

			if err := r.submitSparkApplication(app); err != nil {
				logger.Error(err, "Failed to submit SparkApplication", "name", app.Name, "namespace", app.Namespace)
				app.Status = v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State:        v1beta2.ApplicationStateFailedSubmission,
						ErrorMessage: err.Error(),
					},
					SubmissionAttempts:        app.Status.SubmissionAttempts + 1,
					LastSubmissionAttemptTime: metav1.Now(),
				}
			}
			if err := r.updateSparkApplicationStatus(ctx, app); err != nil {
				return err
			}
			return nil
		},
	)
	if retryErr != nil {
		logger.Error(retryErr, "Failed to reconcile SparkApplication", "name", key.Name, "namespace", key.Namespace)
		return ctrl.Result{Requeue: true}, retryErr
	}
	return ctrl.Result{}, nil
}

func (r *SparkApplicationReconciler) reconcileSubmittedSparkApplication(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	key := req.NamespacedName
	retryErr := retry.RetryOnConflict(
		retry.DefaultRetry,
		func() error {
			old, err := r.getSparkApplication(key)
			if err != nil {
				return err
			}
			if old.Status.AppState.State != v1beta2.ApplicationStateSubmitted {
				return nil
			}
			app := old.DeepCopy()
			if err := r.updateSparkApplicationState(ctx, app); err != nil {
				return err
			}
			if err := r.updateSparkApplicationStatus(ctx, app); err != nil {
				return err
			}
			return nil
		},
	)
	if retryErr != nil {
		logger.Error(retryErr, "Failed to reconcile SparkApplication", "name", key.Name, "namespace", key.Namespace)
		return ctrl.Result{Requeue: true}, retryErr
	}
	return ctrl.Result{}, nil
}

func (r *SparkApplicationReconciler) reconcileFailedSubmissionSparkApplication(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	key := req.NamespacedName
	retryErr := retry.RetryOnConflict(
		retry.DefaultRetry,
		func() error {
			old, err := r.getSparkApplication(key)
			if err != nil {
				return err
			}
			app := old.DeepCopy()
			if util.ShouldRetry(app) {
				if isNextRetryDue(app) {
					if r.validateSparkResourceDeletion(app) {
						r.submitSparkApplication(app)
					} else {
						if err := r.deleteSparkResources(app); err != nil {
							logger.Error(err, "failed to delete resources associated with SparkApplication", "name", app.Name, "namespace", app.Namespace)
						}
						return err
					}
				}
			} else {
				app.Status.AppState.State = v1beta2.ApplicationStateFailed
				r.recordSparkApplicationEvent(app)
			}
			if err := r.updateSparkApplicationStatus(ctx, app); err != nil {
				return err
			}
			return nil
		},
	)
	if retryErr != nil {
		logger.Error(retryErr, "Failed to reconcile SparkApplication", "name", key.Name, "namespace", key.Namespace)
		return ctrl.Result{Requeue: true}, retryErr
	}
	return ctrl.Result{}, nil
}

func (r *SparkApplicationReconciler) reconcileRunningSparkApplication(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	key := req.NamespacedName
	retryErr := retry.RetryOnConflict(
		retry.DefaultRetry,
		func() error {
			old, err := r.getSparkApplication(key)
			if err != nil {
				return err
			}
			app := old.DeepCopy()
			if err := r.updateSparkApplicationState(ctx, app); err != nil {
				return err
			}
			if err := r.updateSparkApplicationStatus(ctx, app); err != nil {
				return err
			}
			return nil
		},
	)
	if retryErr != nil {
		logger.Error(retryErr, "Failed to reconcile SparkApplication", "name", key.Name, "namespace", key.Namespace)
		return ctrl.Result{Requeue: true}, retryErr
	}
	return ctrl.Result{}, nil
}

func (r *SparkApplicationReconciler) reconcilePendingRerunSparkApplication(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	key := req.NamespacedName
	retryErr := retry.RetryOnConflict(
		retry.DefaultRetry,
		func() error {
			old, err := r.getSparkApplication(key)
			if err != nil {
				return err
			}
			app := old.DeepCopy()
			logger.Info("Pending rerun SparkApplication", "name", app.Name, "namespace", app.Namespace, "state", app.Status.AppState.State)
			if r.validateSparkResourceDeletion(app) {
				logger.Info("Successfully deleted resources associated with SparkApplication", "name", app.Name, "namespace", app.Namespace, "state", app.Status.AppState.State)
				r.recordSparkApplicationEvent(app)
				r.resetSparkApplicationStatus(app)
				if err = r.submitSparkApplication(app); err != nil {
					logger.Error(err, "Failed to run spark-submit", "name", app.Name, "namespace", app.Namespace, "state", app.Status.AppState.State)
				}
			}
			if err := r.updateSparkApplicationStatus(ctx, app); err != nil {
				return err
			}
			return nil
		},
	)
	if retryErr != nil {
		logger.Error(retryErr, "Failed to reconcile SparkApplication", "name", key.Name, "namespace", key.Namespace)
		return ctrl.Result{Requeue: true}, retryErr
	}
	return ctrl.Result{}, nil
}

func (r *SparkApplicationReconciler) reconcileInvalidatingSparkApplication(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	key := req.NamespacedName
	retryErr := retry.RetryOnConflict(
		retry.DefaultRetry,
		func() error {
			old, err := r.getSparkApplication(key)
			if err != nil {
				return err
			}
			app := old.DeepCopy()
			// Invalidate the current run and enqueue the SparkApplication for re-execution.
			if err := r.deleteSparkResources(app); err != nil {
				logger.Error(err, "Failed to delete resources associated with SparkApplication", "name", app.Name, "namespace", app.Namespace)
			} else {
				r.resetSparkApplicationStatus(app)
				app.Status.AppState.State = v1beta2.ApplicationStatePendingRerun
			}
			if err := r.updateSparkApplicationStatus(ctx, app); err != nil {
				return err
			}
			return nil
		},
	)
	if retryErr != nil {
		logger.Error(retryErr, "Failed to reconcile SparkApplication", "name", key.Name, "namespace", key.Namespace)
		return ctrl.Result{Requeue: true}, retryErr
	}
	return ctrl.Result{}, nil
}

func (r *SparkApplicationReconciler) reconcileSucceedingSparkApplication(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	key := req.NamespacedName
	retryErr := retry.RetryOnConflict(
		retry.DefaultRetry,
		func() error {
			old, err := r.getSparkApplication(key)
			if err != nil {
				return err
			}
			app := old.DeepCopy()
			if util.ShouldRetry(app) {
				if err := r.deleteSparkResources(app); err != nil {
					logger.Error(err, "failed to delete spark resources", "name", app.Name, "namespace", app.Namespace)
					return err
				}
				app.Status.AppState.State = v1beta2.ApplicationStatePendingRerun
			} else {
				app.Status.AppState.State = v1beta2.ApplicationStateCompleted
			}
			if err := r.updateSparkApplicationStatus(ctx, app); err != nil {
				return err
			}
			return nil
		},
	)
	if retryErr != nil {
		logger.Error(retryErr, "Failed to reconcile SparkApplication", "name", key.Name, "namespace", key.Namespace)
		return ctrl.Result{Requeue: true}, retryErr
	}
	return ctrl.Result{}, nil
}

func (r *SparkApplicationReconciler) reconcileFailingSparkApplication(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	key := req.NamespacedName
	retryErr := retry.RetryOnConflict(
		retry.DefaultRetry,
		func() error {
			old, err := r.getSparkApplication(key)
			if err != nil {
				return err
			}
			app := old.DeepCopy()
			if util.ShouldRetry(app) {
				if isNextRetryDue(app) {
					if err := r.deleteSparkResources(app); err != nil {
						logger.Error(err, "failed to delete spark resources", "name", app.Name, "namespace", app.Namespace)
						return err
					}
					app.Status.AppState.State = v1beta2.ApplicationStatePendingRerun
				}
			} else {
				app.Status.AppState.State = v1beta2.ApplicationStateFailed
			}
			if err := r.updateSparkApplicationStatus(ctx, app); err != nil {
				return err
			}
			return nil
		},
	)
	if retryErr != nil {
		logger.Error(retryErr, "Failed to reconcile SparkApplication", "name", key.Name, "namespace", key.Namespace)
		return ctrl.Result{Requeue: true}, retryErr
	}
	return ctrl.Result{}, nil
}

func (r *SparkApplicationReconciler) reconcileCompletedSparkApplication(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	key := req.NamespacedName
	retryErr := retry.RetryOnConflict(
		retry.DefaultRetry,
		func() error {
			old, err := r.getSparkApplication(key)
			if err != nil {
				return err
			}
			if old.Status.AppState.State != v1beta2.ApplicationStateCompleted {
				return nil
			}
			app := old.DeepCopy()
			if util.IsExpired(app) {
				logger.Info("Deleting expired SparkApplication", "name", app.Name, "namespace", app.Namespace, "state", app.Status.AppState.State)
				if err := r.client.Delete(ctx, app); err != nil {
					return err
				}
				return nil
			}
			if err := r.updateExecutorState(ctx, app); err != nil {
				return err
			}
			if err := r.updateSparkApplicationStatus(ctx, app); err != nil {
				return err
			}
			if err := r.cleanUpOnTermination(old, app); err != nil {
				logger.Error(err, "Failed to clean up resources for SparkApplication", "name", old.Name, "namespace", old.Namespace, "state", old.Status.AppState.State)
				return err
			}
			return nil
		},
	)
	if retryErr != nil {
		logger.Error(retryErr, "Failed to reconcile SparkApplication", "name", key.Name, "namespace", key.Namespace)
		return ctrl.Result{Requeue: true}, retryErr
	}
	return ctrl.Result{}, nil
}

func (r *SparkApplicationReconciler) reconcileFailedSparkApplication(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	key := req.NamespacedName
	retryErr := retry.RetryOnConflict(
		retry.DefaultRetry,
		func() error {
			old, err := r.getSparkApplication(key)
			if err != nil {
				return err
			}
			if old.Status.AppState.State != v1beta2.ApplicationStateFailed {
				return nil
			}
			app := old.DeepCopy()
			if util.IsExpired(app) {
				logger.Info("Deleting expired SparkApplication", "name", app.Name, "namespace", app.Namespace, "state", app.Status.AppState.State)
				if err := r.client.Delete(ctx, app); err != nil {
					return err
				}
				return nil
			}
			if err := r.updateExecutorState(ctx, app); err != nil {
				return err
			}
			if err := r.updateSparkApplicationStatus(ctx, app); err != nil {
				return err
			}
			if err := r.cleanUpOnTermination(old, app); err != nil {
				logger.Error(err, "Failed to clean up resources for SparkApplication", "name", old.Name, "namespace", old.Namespace, "state", old.Status.AppState.State)
				return err
			}
			return nil
		},
	)
	if retryErr != nil {
		logger.Error(retryErr, "Failed to reconcile SparkApplication", "name", key.Name, "namespace", key.Namespace)
		return ctrl.Result{Requeue: true}, retryErr
	}
	return ctrl.Result{}, nil
}

func (r *SparkApplicationReconciler) reconcileUnknownSparkApplication(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	key := req.NamespacedName
	retryErr := retry.RetryOnConflict(
		retry.DefaultRetry,
		func() error {
			old, err := r.getSparkApplication(key)
			if err != nil {
				return err
			}
			if old.Status.AppState.State != v1beta2.ApplicationStateUnknown {
				return nil
			}
			app := old.DeepCopy()
			if err := r.updateSparkApplicationState(ctx, app); err != nil {
				return err
			}
			if err := r.updateSparkApplicationStatus(ctx, app); err != nil {
				return err
			}
			return nil
		},
	)
	if retryErr != nil {
		logger.Error(retryErr, "Failed to reconcile SparkApplication", "name", key.Name, "namespace", key.Namespace)
		return ctrl.Result{Requeue: true}, retryErr
	}
	return ctrl.Result{}, nil
}

// getSparkApplication gets the SparkApplication with the given name and namespace.
func (r *SparkApplicationReconciler) getSparkApplication(key types.NamespacedName) (*v1beta2.SparkApplication, error) {
	app := &v1beta2.SparkApplication{}
	if err := r.client.Get(context.TODO(), key, app); err != nil {
		return nil, err
	}
	return app, nil
}

// submitSparkApplication creates a new submission for the given SparkApplication and submits it using spark-submit.
func (r *SparkApplicationReconciler) submitSparkApplication(app *v1beta2.SparkApplication) error {
	logger.Info("Submitting SparkApplication", "name", app.Name, "namespace", app.Namespace, "state", app.Status.AppState.State)

	if util.PrometheusMonitoringEnabled(app) {
		logger.V(1).Info("Configure Prometheus monitoring for SparkApplication", "name", app.Name, "namespace", app.Namespace)
		if err := configPrometheusMonitoring(app, r.client); err != nil {
			return fmt.Errorf("failed to configure Prometheus monitoring: %v", err)
		}
	}

	// Use batch scheduler to perform scheduling task before submitting (before build command arguments).
	if needScheduling, scheduler := r.shouldDoBatchScheduling(app); needScheduling {
		logger.V(1).Info("Do batch scheduling for SparkApplication", "name", app.Name, "namespace", app.Namespace)
		if err := scheduler.Schedule(app); err != nil {
			return fmt.Errorf("failed to process batch scheduler: %v", err)
		}
	}

	driverInfo := v1beta2.DriverInfo{}

	if r.options.EnableUIService {
		logger.Info("Creating web UI service for SparkApplication", "name", app.Name, "namespace", app.Namespace)
		service, err := r.createWebUIService(app)
		if err != nil {
			return fmt.Errorf("failed to create web UI service")
		}
		driverInfo.WebUIServiceName = service.serviceName
		driverInfo.WebUIPort = service.servicePort
		driverInfo.WebUIAddress = fmt.Sprintf("%s:%d", service.serviceIP, app.Status.DriverInfo.WebUIPort)

		// Create UI Ingress if ingress-format is set.
		if r.options.IngressURLFormat != "" {
			logger.Info("Creating web UI ingress for SparkApplication", "name", app.Name, "namespace", app.Namespace)
			// We are going to want to use an ingress url.
			ingressURL, err := getDriverIngressURL(r.options.IngressURLFormat, app.Name, app.Namespace)
			if err != nil {
				return fmt.Errorf("failed to get ingress url: %v", err)
			}
			// need to ensure the spark.ui variables are configured correctly if a subPath is used.
			if ingressURL.Path != "" {
				if app.Spec.SparkConf == nil {
					app.Spec.SparkConf = make(map[string]string)
				}
				app.Spec.SparkConf[common.SparkUIProxyBase] = ingressURL.Path
				app.Spec.SparkConf[common.SparkUIProxyRedirectURI] = "/"
			}
			ingress, err := r.createWebUIIngress(app, *service, ingressURL, r.options.IngressClassName)
			if err != nil {
				return fmt.Errorf("failed to create web UI service")
			}
			driverInfo.WebUIIngressAddress = ingress.ingressURL.String()
			driverInfo.WebUIIngressName = ingress.ingressName
		}
	}

	for _, driverIngressConfiguration := range app.Spec.DriverIngressOptions {
		logger.Info("Creating driver ingress service for SparkApplication", "name", app.Name, "namespace", app.Namespace)
		service, err := r.createDriverIngressServiceFromConfiguration(app, &driverIngressConfiguration)
		if err != nil {
			return fmt.Errorf("failed to create driver ingress service for SparkApplication: %v", err)
		}
		// Create ingress if ingress-format is set.
		if driverIngressConfiguration.IngressURLFormat != "" {
			// We are going to want to use an ingress url.
			ingressURL, err := getDriverIngressURL(driverIngressConfiguration.IngressURLFormat, app.Name, app.Namespace)
			if err != nil {
				return fmt.Errorf("failed to get driver ingress url: %v", err)
			}
			ingress, err := r.createDriverIngress(app, &driverIngressConfiguration, *service, ingressURL, r.options.IngressClassName)
			if err != nil {
				return fmt.Errorf("failed to create driver ingress: %v", err)
			}
			logger.V(1).Info("Created driver ingress for SparkApplication", "name", app.Name, "namespace", app.Namespace, "ingressName", ingress.ingressName, "ingressURL", ingress.ingressURL)
		}
	}

	driverPodName := util.GetDriverPodName(app)
	driverInfo.PodName = driverPodName
	app.Status.SubmissionID = uuid.New().String()
	sparkSubmitArgs, err := buildSparkSubmitArgs(app)
	if err != nil {
		return fmt.Errorf("failed to build spark-submit arguments: %v", err)
	}

	// Try submitting the application by running spark-submit.
	logger.Info("Running spark-submit for SparkApplication", "name", app.Name, "namespace", app.Namespace, "arguments", sparkSubmitArgs)
	submitted, err := runSparkSubmit(newSubmission(sparkSubmitArgs, app))
	if err != nil {
		r.recordSparkApplicationEvent(app)
		return fmt.Errorf("failed to run spark-submit: %v", err)
	}
	if !submitted {
		// The application may not have been submitted even if err == nil, e.g., when some
		// state update caused an attempt to re-submit the application, in which case no
		// error gets returned from runSparkSubmit. If this is the case, we simply return.
		return nil
	}

	app.Status.AppState = v1beta2.ApplicationState{
		State: v1beta2.ApplicationStateSubmitted,
	}
	app.Status.DriverInfo = driverInfo
	app.Status.SubmissionAttempts = app.Status.SubmissionAttempts + 1
	app.Status.ExecutionAttempts = app.Status.ExecutionAttempts + 1
	app.Status.LastSubmissionAttemptTime = metav1.Now()
	r.recordSparkApplicationEvent(app)
	return nil
}

// Helper func to determine if the next retry the SparkApplication is due now.
func isNextRetryDue(app *v1beta2.SparkApplication) bool {
	retryInterval := app.Spec.RestartPolicy.OnFailureRetryInterval
	attemptsDone := app.Status.SubmissionAttempts
	lastEventTime := app.Status.LastSubmissionAttemptTime
	if retryInterval == nil || lastEventTime.IsZero() || attemptsDone <= 0 {
		return false
	}

	// Retry if we have waited at-least equal to attempts*RetryInterval since we do a linear back-off.
	interval := time.Duration(*retryInterval) * time.Second * time.Duration(attemptsDone)
	currentTime := time.Now()
	logger.Info(fmt.Sprintf("currentTime is %v, interval is %v", currentTime, interval))
	return currentTime.After(lastEventTime.Add(interval))
}

// updateDriverState finds the driver pod of the application
// and updates the driver state based on the current phase of the pod.
func (r *SparkApplicationReconciler) updateDriverState(_ context.Context, app *v1beta2.SparkApplication) error {
	// Either the driver pod doesn't exist yet or its name has not been updated.
	if app.Status.DriverInfo.PodName == "" {
		return fmt.Errorf("empty driver pod name with application state %s", app.Status.AppState.State)
	}

	driverPod, err := r.getDriverPod(app)
	if err != nil {
		return err
	}

	if driverPod == nil {
		app.Status.AppState.State = v1beta2.ApplicationStateFailing
		app.Status.AppState.ErrorMessage = "driver pod not found"
		app.Status.TerminationTime = metav1.Now()
		return nil
	}

	app.Status.SparkApplicationID = util.GetSparkApplicationID(driverPod)
	driverState := util.GetDriverState(driverPod)
	if util.IsDriverTerminated(driverState) {
		if app.Status.TerminationTime.IsZero() {
			app.Status.TerminationTime = metav1.Now()
		}
		if driverState == v1beta2.DriverStateFailed {
			if state := util.GetDriverContainerTerminatedState(driverPod); state != nil {
				if state.ExitCode != 0 {
					app.Status.AppState.ErrorMessage = fmt.Sprintf("driver container failed with ExitCode: %d, Reason: %s", state.ExitCode, state.Reason)
				}
			} else {
				app.Status.AppState.ErrorMessage = "driver container status missing"
			}
		}
	}

	newState := util.DriverStateToApplicationState(driverState)
	// Only record a driver event if the application state (derived from the driver pod phase) has changed.
	if newState != app.Status.AppState.State {
		r.recordDriverEvent(app, driverState, driverPod.Name)
		app.Status.AppState.State = newState
	}

	return nil
}

// updateExecutorState lists the executor pods of the application
// and updates the executor state based on the current phase of the pods.
func (r *SparkApplicationReconciler) updateExecutorState(_ context.Context, app *v1beta2.SparkApplication) error {
	podList, err := r.getExecutorPods(app)
	if err != nil {
		return err
	}
	pods := podList.Items

	executorStateMap := make(map[string]v1beta2.ExecutorState)
	var executorApplicationID string
	for _, pod := range pods {
		if util.IsExecutorPod(&pod) {
			newState := util.GetExecutorState(&pod)
			oldState, exists := app.Status.ExecutorState[pod.Name]
			// Only record an executor event if the executor state is new or it has changed.
			if !exists || newState != oldState {
				if newState == v1beta2.ExecutorStateFailed {
					execContainerState := util.GetExecutorContainerTerminatedState(&pod)
					if execContainerState != nil {
						r.recordExecutorEvent(app, newState, pod.Name, execContainerState.ExitCode, execContainerState.Reason)
					} else {
						// If we can't find the container state,
						// we need to set the exitCode and the Reason to unambiguous values.
						r.recordExecutorEvent(app, newState, pod.Name, -1, "Unknown (Container not Found)")
					}
				} else {
					r.recordExecutorEvent(app, newState, pod.Name)
				}
			}
			executorStateMap[pod.Name] = newState

			if executorApplicationID == "" {
				executorApplicationID = util.GetSparkApplicationID(&pod)
			}
		}
	}

	// ApplicationID label can be different on driver/executors. Prefer executor ApplicationID if set.
	// Refer https://issues.apache.org/jira/projects/SPARK/issues/SPARK-25922 for details.
	if executorApplicationID != "" {
		app.Status.SparkApplicationID = executorApplicationID
	}

	if app.Status.ExecutorState == nil {
		app.Status.ExecutorState = make(map[string]v1beta2.ExecutorState)
	}
	for name, state := range executorStateMap {
		app.Status.ExecutorState[name] = state
	}

	// Handle missing/deleted executors.
	for name, oldStatus := range app.Status.ExecutorState {
		_, exists := executorStateMap[name]
		if !util.IsExecutorTerminated(oldStatus) && !exists {
			if !util.IsDriverRunning(app) {
				// If ApplicationState is COMPLETED, in other words, the driver pod has been completed
				// successfully. The executor pods terminate and are cleaned up, so we could not found
				// the executor pod, under this circumstances, we assume the executor pod are completed.
				if app.Status.AppState.State == v1beta2.ApplicationStateCompleted {
					app.Status.ExecutorState[name] = v1beta2.ExecutorStateCompleted
				} else {
					glog.Infof("Executor pod %s not found, assuming it was deleted.", name)
					app.Status.ExecutorState[name] = v1beta2.ExecutorStateFailed
				}
			} else {
				app.Status.ExecutorState[name] = v1beta2.ExecutorStateUnknown
			}
		}
	}

	return nil
}

func (r *SparkApplicationReconciler) getExecutorPods(app *v1beta2.SparkApplication) (*corev1.PodList, error) {
	matchLabels := util.GetResourceLabels(app)
	matchLabels[common.LabelSparkRole] = common.SparkRoleExecutor
	pods := &corev1.PodList{}
	if err := r.client.List(context.TODO(), pods, client.InNamespace(app.Namespace), client.MatchingLabels(matchLabels)); err != nil {
		return nil, fmt.Errorf("failed to get pods for SparkApplication %s/%s: %v", app.Namespace, app.Name, err)
	}
	return pods, nil
}

func (r *SparkApplicationReconciler) getDriverPod(app *v1beta2.SparkApplication) (*corev1.Pod, error) {
	pod := &corev1.Pod{}
	var err error

	key := types.NamespacedName{Namespace: app.Namespace, Name: app.Status.DriverInfo.PodName}
	err = r.client.Get(context.TODO(), key, pod)
	if err == nil {
		return pod, nil
	}
	if !errors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to get driver pod %s: %v", app.Status.DriverInfo.PodName, err)
	}

	return nil, nil
}

func (r *SparkApplicationReconciler) updateSparkApplicationState(ctx context.Context, app *v1beta2.SparkApplication) error {
	if err := r.updateDriverState(ctx, app); err != nil {
		return err
	}
	if err := r.updateExecutorState(ctx, app); err != nil {
		return err
	}
	return nil
}

func (r *SparkApplicationReconciler) updateSparkApplication(ctx context.Context, app *v1beta2.SparkApplication) error {
	// logger.Info("Updating SparkApplication", "name", app.Name, "namespace", app.Namespace, "state", app.Status.AppState.State)
	if err := r.client.Update(ctx, app); err != nil {
		// logger.Error(err, "Failed to update SparkApplication", "name", app.Name, "namespace", app.Namespace, "state", app.Status.AppState.State)
		return err
	}
	return nil
}

// updateSparkApplicationStatus updates the status of the SparkApplication.
func (r *SparkApplicationReconciler) updateSparkApplicationStatus(ctx context.Context, app *v1beta2.SparkApplication) error {
	// logger.Info("Updating SparkApplication status", "name", app.Name, "namespace", app.Namespace, "state", app.Status.AppState.State)
	if err := r.client.Status().Update(ctx, app); err != nil {
		// logger.Error(err, "Failed to update SparkApplication status", "name", app.Name, "namespace", app.Namespace, "state", app.Status.AppState.State)
		return err
	}
	return nil
}

// Delete the resources associated with the spark application.
func (r *SparkApplicationReconciler) deleteSparkResources(app *v1beta2.SparkApplication) error {
	if err := r.deleteDriverPod(app); err != nil {
		return err
	}

	if err := r.deleteWebUIService(app); err != nil {
		return err
	}

	if err := r.deleteWebUIIngress(app); err != nil {
		return err
	}

	return nil
}

func (r *SparkApplicationReconciler) deleteDriverPod(app *v1beta2.SparkApplication) error {
	podName := app.Status.DriverInfo.PodName
	// Derive the driver pod name in case the driver pod name was not recorded in the status,
	// which could happen if the status update right after submission failed.
	if podName == "" {
		podName = util.GetDriverPodName(app)
	}

	logger.Info("Deleting driver pod", "name", podName, "namespace", app.Namespace)
	if err := r.client.Delete(
		context.TODO(),
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: app.Namespace,
			},
		},
	); err != nil && !errors.IsNotFound(err) {
		return err
	}

	return nil
}

func (r *SparkApplicationReconciler) deleteWebUIService(app *v1beta2.SparkApplication) error {
	svcName := app.Status.DriverInfo.WebUIServiceName
	if svcName == "" {
		return nil
	}
	logger.Info("Deleting Spark web UI service", "name", svcName, "namespace", app.Namespace)
	if err := r.client.Delete(
		context.TODO(),
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      svcName,
				Namespace: app.Namespace,
			},
		},
		&client.DeleteOptions{
			GracePeriodSeconds: util.Int64Ptr(0),
		},
	); err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func (r *SparkApplicationReconciler) deleteWebUIIngress(app *v1beta2.SparkApplication) error {
	ingressName := app.Status.DriverInfo.WebUIIngressName
	if ingressName == "" {
		return nil
	}

	if util.IngressCapabilities.Has("networking.k8s.io/v1") {
		logger.V(1).Info("Deleting Spark web UI ingress", "name", ingressName, "namespace", app.Namespace)
		if err := r.client.Delete(
			context.TODO(),
			&networkingv1.Ingress{
				ObjectMeta: metav1.ObjectMeta{
					Name:      ingressName,
					Namespace: app.Namespace,
				},
			},
			&client.DeleteOptions{
				GracePeriodSeconds: util.Int64Ptr(0),
			},
		); err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	if util.IngressCapabilities.Has("extensions/v1beta1") {
		logger.V(1).Info("Deleting extensions/v1beta1 Spark UI Ingress", "name", ingressName, "namespace", app.Namespace)
		if err := r.client.Delete(
			context.TODO(),
			&extensionsv1beta1.Ingress{
				ObjectMeta: metav1.ObjectMeta{
					Name:      ingressName,
					Namespace: app.Namespace,
				},
			},
			&client.DeleteOptions{
				GracePeriodSeconds: util.Int64Ptr(0),
			},
		); err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	return nil
}

// Validate that any Spark resources (driver/Service/Ingress) created for the application have been deleted.
func (r *SparkApplicationReconciler) validateSparkResourceDeletion(app *v1beta2.SparkApplication) bool {
	driverPodName := app.Status.DriverInfo.PodName
	// Derive the driver pod name in case the driver pod name was not recorded in the status,
	// which could happen if the status update right after submission failed.
	if driverPodName == "" {
		driverPodName = util.GetDriverPodName(app)
	}

	if err := r.client.Get(context.TODO(), types.NamespacedName{Name: driverPodName, Namespace: app.Namespace}, &corev1.Pod{}); err == nil || !errors.IsNotFound(err) {
		return false
	}

	sparkUIServiceName := app.Status.DriverInfo.WebUIServiceName
	if sparkUIServiceName != "" {
		if err := r.client.Get(context.TODO(), types.NamespacedName{Name: sparkUIServiceName, Namespace: app.Namespace}, &corev1.Service{}); err == nil || !errors.IsNotFound(err) {
			return false
		}
	}

	sparkUIIngressName := app.Status.DriverInfo.WebUIIngressName
	if sparkUIIngressName != "" {
		if err := r.client.Get(context.TODO(), types.NamespacedName{Name: sparkUIIngressName, Namespace: app.Namespace}, &networkingv1.Ingress{}); err == nil || !errors.IsNotFound(err) {
			return false
		}
	}

	return true
}

func (r *SparkApplicationReconciler) recordSparkApplicationEvent(app *v1beta2.SparkApplication) {
	switch app.Status.AppState.State {
	case v1beta2.ApplicationStateNew:
		r.recorder.Eventf(
			app,
			corev1.EventTypeNormal,
			"SparkApplicationAdded",
			"SparkApplication %s was added, enqueuing it for submission",
			app.Name,
		)
	case v1beta2.ApplicationStateSubmitted:
		r.recorder.Eventf(
			app,
			corev1.EventTypeNormal,
			"SparkApplicationSubmitted",
			"SparkApplication %s was submitted successfully",
			app.Name,
		)
	case v1beta2.ApplicationStateFailedSubmission:
		r.recorder.Eventf(
			app,
			corev1.EventTypeWarning,
			"SparkApplicationSubmissionFailed",
			"failed to submit SparkApplication %s: %s",
			app.Name,
			app.Status.AppState.ErrorMessage,
		)
	case v1beta2.ApplicationStateCompleted:
		r.recorder.Eventf(
			app,
			corev1.EventTypeNormal,
			"SparkApplicationCompleted",
			"SparkApplication %s completed",
			app.Name,
		)
	case v1beta2.ApplicationStateFailed:
		r.recorder.Eventf(
			app,
			corev1.EventTypeWarning,
			"SparkApplicationFailed",
			"SparkApplication %s failed: %s",
			app.Name,
			app.Status.AppState.ErrorMessage,
		)
	case v1beta2.ApplicationStatePendingRerun:
		r.recorder.Eventf(
			app,
			corev1.EventTypeWarning,
			"SparkApplicationPendingRerun",
			"SparkApplication %s is pending rerun",
			app.Name,
		)
	}
}

func (r *SparkApplicationReconciler) recordDriverEvent(app *v1beta2.SparkApplication, state v1beta2.DriverState, name string) {
	switch state {
	case v1beta2.DriverStatePending:
		r.recorder.Eventf(app, corev1.EventTypeNormal, "SparkDriverPending", "Driver %s is pending", name)
	case v1beta2.DriverStateRunning:
		r.recorder.Eventf(app, corev1.EventTypeNormal, "SparkDriverRunning", "Driver %s is running", name)
	case v1beta2.DriverStateCompleted:
		r.recorder.Eventf(app, corev1.EventTypeNormal, "SparkDriverCompleted", "Driver %s completed", name)
	case v1beta2.DriverStateFailed:
		r.recorder.Eventf(app, corev1.EventTypeWarning, "SparkDriverFailed", "Driver %s failed", name)
	case v1beta2.DriverStateUnknown:
		r.recorder.Eventf(app, corev1.EventTypeWarning, "SparkDriverUnknownState", "Driver %s in unknown state", name)
	}
}

func (r *SparkApplicationReconciler) recordExecutorEvent(app *v1beta2.SparkApplication, state v1beta2.ExecutorState, args ...interface{}) {
	switch state {
	case v1beta2.ExecutorStatePending:
		r.recorder.Eventf(app, corev1.EventTypeNormal, "SparkExecutorPending", "Executor %s is pending", args)
	case v1beta2.ExecutorStateRunning:
		r.recorder.Eventf(app, corev1.EventTypeNormal, "SparkExecutorRunning", "Executor %s is running", args)
	case v1beta2.ExecutorStateCompleted:
		r.recorder.Eventf(app, corev1.EventTypeNormal, "SparkExecutorCompleted", "Executor %s completed", args)
	case v1beta2.ExecutorStateFailed:
		r.recorder.Eventf(app, corev1.EventTypeWarning, "SparkExecutorFailed", "Executor %s failed with ExitCode: %d, Reason: %s", args)
	case v1beta2.ExecutorStateUnknown:
		r.recorder.Eventf(app, corev1.EventTypeWarning, "SparkExecutorUnknownState", "Executor %s in unknown state", args)
	}
}

func (r *SparkApplicationReconciler) resetSparkApplicationStatus(app *v1beta2.SparkApplication) {
	status := &app.Status
	if status.AppState.State == v1beta2.ApplicationStateInvalidating {
		status.SparkApplicationID = ""
		status.SubmissionAttempts = 0
		status.ExecutionAttempts = 0
		status.LastSubmissionAttemptTime = metav1.Time{}
		status.TerminationTime = metav1.Time{}
		status.AppState.ErrorMessage = ""
		status.ExecutorState = nil
	} else if status.AppState.State == v1beta2.ApplicationStatePendingRerun {
		status.SparkApplicationID = ""
		status.SubmissionAttempts = 0
		status.LastSubmissionAttemptTime = metav1.Time{}
		status.DriverInfo = v1beta2.DriverInfo{}
		status.AppState.ErrorMessage = ""
		status.ExecutorState = nil
	}
}

func (r *SparkApplicationReconciler) shouldDoBatchScheduling(app *v1beta2.SparkApplication) (bool, batchscheduler.Interface) {
	if r.manager == nil || app.Spec.BatchScheduler == nil || *app.Spec.BatchScheduler == "" {
		return false, nil
	}

	scheduler, err := r.manager.GetScheduler(*app.Spec.BatchScheduler)
	if err != nil || scheduler == nil {
		logger.Error(err, "Failed to get scheduler for SparkApplication", "name", app.Name, "namespace", app.Namespace, "scheduler", *app.Spec.BatchScheduler)
		return false, nil
	}
	return scheduler.ShouldSchedule(app), scheduler
}

// Clean up when the spark application is terminated.
func (r *SparkApplicationReconciler) cleanUpOnTermination(_, newApp *v1beta2.SparkApplication) error {
	if needScheduling, scheduler := r.shouldDoBatchScheduling(newApp); needScheduling {
		if err := scheduler.Cleanup(newApp); err != nil {
			return err
		}
	}
	return nil
}
