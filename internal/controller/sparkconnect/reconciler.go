/*
Copyright 2025 The Kubeflow authors.

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

package sparkconnect

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/yaml"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

const (
	ExecutorPodTemplateFileName = "executor-pod-template.yaml"
)

// Options defines the options of SparkConnect reconciler.
type Options struct {
	// A list of namespaces that should be watched.
	Namespaces        []string
	NamespaceSelector string
}

// Reconciler reconciles a SparkConnect object.
type Reconciler struct {
	manager  ctrl.Manager
	scheme   *runtime.Scheme
	client   client.Client
	recorder record.EventRecorder
	options  Options
}

// Reconciler implements the reconcile.Reconciler interface.
var _ reconcile.Reconciler = &Reconciler{}

// NewReconciler creates a new SparkConnect Reconciler.
func NewReconciler(
	manager ctrl.Manager,
	scheme *runtime.Scheme,
	client client.Client,
	recorder record.EventRecorder,
	options Options,
) *Reconciler {
	return &Reconciler{
		manager:  manager,
		scheme:   scheme,
		client:   client,
		recorder: recorder,
		options:  options,
	}
}

// SetupWithManager sets up the SparkConnect reconciler with the manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, options controller.Options) error {
	kind := "SparkConnect"

	// Use a custom log constructor.
	options.LogConstructor = util.NewLogConstructor(mgr.GetLogger(), kind)

	// Create predicate before the builder chain.
	namespacePredicate, err := util.NewNamespacePredicate(
		r.client,
		r.options.Namespaces,
		r.options.NamespaceSelector,
	)
	if err != nil {
		return fmt.Errorf("failed to create namespace predicate: %w", err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.SparkConnect{}).
		Owns(
			&corev1.ConfigMap{},
			builder.WithPredicates(
				util.NewLabelPredicate(map[string]string{
					common.LabelCreatedBySparkOperator: "true",
				}),
			),
		).
		Owns(
			&corev1.Service{},
			builder.WithPredicates(
				util.NewLabelPredicate(map[string]string{
					common.LabelCreatedBySparkOperator: "true",
				}),
			),
		).
		Watches(
			&corev1.Pod{},
			&handler.TypedFuncs[client.Object, reconcile.Request]{
				CreateFunc: func(ctx context.Context, e event.TypedCreateEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
					labels := e.Object.GetLabels()
					name := labels[common.LabelSparkConnectName]
					if name != "" {
						key := types.NamespacedName{
							Namespace: e.Object.GetNamespace(),
							Name:      name,
						}
						q.AddRateLimited(reconcile.Request{NamespacedName: key})
					}
				},
				UpdateFunc: func(ctx context.Context, e event.TypedUpdateEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
					labels := e.ObjectNew.GetLabels()
					name := labels[common.LabelSparkConnectName]
					if name != "" {
						key := types.NamespacedName{
							Namespace: e.ObjectNew.GetNamespace(),
							Name:      name,
						}
						q.AddRateLimited(reconcile.Request{NamespacedName: key})
					}
				},
				DeleteFunc: func(ctx context.Context, e event.TypedDeleteEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
					labels := e.Object.GetLabels()
					name := labels[common.LabelSparkConnectName]
					if name != "" {
						key := types.NamespacedName{
							Namespace: e.Object.GetNamespace(),
							Name:      name,
						}
						q.AddRateLimited(reconcile.Request{NamespacedName: key})
					}
				},
				GenericFunc: func(ctx context.Context, e event.TypedGenericEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
					labels := e.Object.GetLabels()
					name := labels[common.LabelSparkConnectName]
					if name != "" {
						key := types.NamespacedName{
							Namespace: e.Object.GetNamespace(),
							Name:      name,
						}
						q.AddRateLimited(reconcile.Request{NamespacedName: key})
					}
				},
			},
			builder.WithPredicates(
				util.NewLabelPredicate(map[string]string{
					common.LabelLaunchedBySparkOperator: "true",
				}),
			),
		).
		WithEventFilter(namespacePredicate).
		WithOptions(options).
		Complete(r)
}

// +kubebuilder:rbac:groups=,resources=events,verbs=create;update;patch
// +kubebuilder:rbac:groups=,resources=configmaps,verbs=get;list;create;update;delete
// +kubebuilder:rbac:groups=,resources=pods,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups=,resources=services,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups=sparkoperator.k8s.io,resources=sparkconnects,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sparkoperator.k8s.io,resources=sparkconnects/status,verbs=get;update;patch

// Reconcile implements reconcile.TypedReconciler.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (reconcile.Result, error) {
	// Get SparkConnect object.
	old := &v1alpha1.SparkConnect{}
	if err := r.client.Get(ctx, req.NamespacedName, old); err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	logger := ctrl.LoggerFrom(ctx)

	// Deep copy the SparkConnect object.
	conn := old.DeepCopy()
	if !conn.DeletionTimestamp.IsZero() {
		logger.Info("Skip reconciling SparkConnect in terminating state")
		return ctrl.Result{}, nil
	}

	logger.Info("Reconciling SparkConnect")

	if conn.Status.StartTime.IsZero() {
		conn.Status.StartTime = metav1.Now()
	}

	if err := r.createOrUpdateConfigMap(ctx, conn); err != nil {
		return reconcile.Result{}, err
	}

	if err := r.createOrUpdateServerPod(ctx, conn); err != nil {
		return reconcile.Result{}, err
	}

	if err := r.createOrUpdateServerService(ctx, conn); err != nil {
		return reconcile.Result{}, err
	}

	if err := r.updateSparkConnectStatus(ctx, old, conn); err != nil {
		if errors.IsConflict(err) {
			logger.V(1).Info("conflict updating SparkConnect status")
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to update SparkConnect status: %v", err)
	}

	return reconcile.Result{}, nil
}

// createOrUpdateConfigMap creates or updates the ConfigMap for the SparkConnect resource.
func (r *Reconciler) createOrUpdateConfigMap(ctx context.Context, conn *v1alpha1.SparkConnect) error {
	logger := ctrl.LoggerFrom(ctx)
	logger.V(1).Info("Create or update ConfigMap")

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GetConfigMapName(conn),
			Namespace: conn.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.client, cm, func() error {
		if err := r.mutateConfigMap(ctx, conn, cm); err != nil {
			return fmt.Errorf("failed to mutate configmap: %v", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to create or update configmap: %v", err)
	}

	return nil
}

// mutateConfigMap mutates the configmap for the SparkConnect resource.
func (r *Reconciler) mutateConfigMap(_ context.Context, conn *v1alpha1.SparkConnect, cm *corev1.ConfigMap) error {
	if cm.Labels == nil {
		cm.Labels = map[string]string{}
	}
	for key, val := range GetCommonLabels(conn) {
		cm.Labels[key] = val
	}

	if err := ctrl.SetControllerReference(conn, cm, r.scheme); err != nil {
		return fmt.Errorf("failed to set controller reference")
	}

	podTemplateData, err := yaml.Marshal(conn.Spec.Executor.Template)
	if err != nil {
		return fmt.Errorf("failed to marshal executor pod template: %v", err)
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data[ExecutorPodTemplateFileName] = string(podTemplateData)
	return nil
}

// createOrUpdateServerPod creates or updates the server pod for the SparkConnect resource.
func (r *Reconciler) createOrUpdateServerPod(ctx context.Context, conn *v1alpha1.SparkConnect) error {
	logger := ctrl.LoggerFrom(ctx)
	logger.V(1).Info("Create or update server pod")

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GetServerPodName(conn),
			Namespace: conn.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.client, pod, func() error {
		// Mutate server pod.
		if err := r.mutateServerPod(ctx, conn, pod); err != nil {
			return fmt.Errorf("failed to build server pod: %v", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to create or update server pod: %v", err)
	}

	// Update SparkConnect status.
	ready := util.IsPodReady(pod)
	if ready {
		condition := metav1.Condition{
			Type:    string(v1alpha1.SparkConnectConditionServerPodReady),
			Status:  metav1.ConditionTrue,
			Reason:  string(v1alpha1.SparkConnectConditionReasonServerPodReady),
			Message: "Server pod is ready",
		}
		_ = meta.SetStatusCondition(&conn.Status.Conditions, condition)
		conn.Status.State = v1alpha1.SparkConnectStateReady
	} else {
		condition := metav1.Condition{
			Type:    string(v1alpha1.SparkConnectConditionServerPodReady),
			Status:  metav1.ConditionFalse,
			Reason:  string(v1alpha1.SparkConnectConditionReasonServerPodNotReady),
			Message: fmt.Sprintf("Server pod is not ready: %s", pod.Status.Message),
		}
		_ = meta.SetStatusCondition(&conn.Status.Conditions, condition)
		conn.Status.State = v1alpha1.SparkConnectStateNotReady
	}

	conn.Status.Server.PodName = pod.Name
	conn.Status.Server.PodIP = pod.Status.PodIP

	return nil
}

// mutateServerPod mutates the server pod for SparkConnect.
func (r *Reconciler) mutateServerPod(_ context.Context, conn *v1alpha1.SparkConnect, pod *corev1.Pod) error {
	// Server pod not created yet.
	if pod.CreationTimestamp.IsZero() {
		template := conn.Spec.Server.Template
		if template != nil {
			pod.Labels = template.Labels
			pod.Annotations = template.Annotations
			pod.Spec = template.Spec
		}

		// Add a default server container if not specified.
		if len(pod.Spec.Containers) == 0 {
			pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{
				Name: common.SparkDriverContainerName,
			})
		}

		index := 0
		for i, container := range pod.Spec.Containers {
			if container.Name == common.SparkDriverContainerName {
				index = i
				break
			}
		}

		// Build Spark connect server container.
		container := &pod.Spec.Containers[index]

		// Setup image.
		if container.Image == "" {
			if conn.Spec.Image == nil || *conn.Spec.Image == "" {
				return fmt.Errorf("image is not specified")
			}
			container.Image = *conn.Spec.Image
		}

		// Setup entrypoint.
		container.Command = []string{"bash", "-c"}
		args, err := buildStartConnectServerArgs(conn)
		if err != nil {
			return fmt.Errorf("failed to build spark connection args: %v", err)
		}
		container.Args = []string{strings.Join(args, " ")}

		// Setup environment variables.
		container.Env = append(
			container.Env,
			corev1.EnvVar{
				Name: "POD_IP",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "status.podIP",
					},
				},
			},
			corev1.EnvVar{
				Name:  common.EnvSparkNoDaemonize,
				Value: "true",
			},
		)

		// Setup volumes and volumeMounts.
		container.VolumeMounts = append(
			container.VolumeMounts,
			corev1.VolumeMount{
				Name:      common.SparkConfigMapVolumeMountName,
				SubPath:   ExecutorPodTemplateFileName,
				MountPath: fmt.Sprintf("/tmp/spark/%s", ExecutorPodTemplateFileName),
				ReadOnly:  true,
			},
		)

		container.Lifecycle = &corev1.Lifecycle{
			PreStop: &corev1.LifecycleHandler{
				Exec: &corev1.ExecAction{
					Command: []string{
						"bash",
						"-c",
						"${SPARK_HOME}/sbin/stop-connect-server.sh",
					},
				},
			},
		}

		pod.Spec.Volumes = append(
			pod.Spec.Volumes,
			corev1.Volume{
				Name: common.SparkConfigMapVolumeMountName,
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: GetConfigMapName(conn),
						},
						Items: []corev1.KeyToPath{
							{
								Key:  ExecutorPodTemplateFileName,
								Path: ExecutorPodTemplateFileName,
							},
						},
					},
				},
			},
		)
	}

	// Set controller owner reference on server pod.
	if err := ctrl.SetControllerReference(conn, pod, r.scheme); err != nil {
		return fmt.Errorf("failed to set controller reference: %v", err)
	}

	// Set labels on server pod.
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	for key, val := range GetServerSelectorLabels(conn) {
		pod.Labels[key] = val
	}
	pod.Labels[common.LabelSparkVersion] = conn.Spec.SparkVersion

	return nil
}

// createOrUpdateServerService creates or updates the server service for the SparkConnect resource.
func (r *Reconciler) createOrUpdateServerService(ctx context.Context, conn *v1alpha1.SparkConnect) error {
	logger := ctrl.LoggerFrom(ctx)

	// Use the service specified in the server spec if provided.
	svc := conn.Spec.Server.Service
	if svc == nil {
		svc = &corev1.Service{}
	}
	svc.Name = GetServerServiceName(conn)
	// Namespace provided by user will be ignored.
	svc.Namespace = conn.Namespace

	// Create or update server service.
	opResult, err := controllerutil.CreateOrUpdate(ctx, r.client, svc, func() error {
		if err := r.mutateServerService(ctx, conn, svc); err != nil {
			return fmt.Errorf("failed to mutate server service: %v", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to create or update server service: %v", err)
	}
	switch opResult {
	case controllerutil.OperationResultCreated:
		logger.Info("Server service created")
	case controllerutil.OperationResultUpdated:
		logger.Info("Server service updated")
	}

	// Update SparkConnect status.
	conn.Status.Server.ServiceName = svc.Name

	return nil
}

// mutateServerService mutates the server service for the SparkConnect resource.
func (r *Reconciler) mutateServerService(_ context.Context, conn *v1alpha1.SparkConnect, svc *corev1.Service) error {
	if svc.CreationTimestamp.IsZero() {
		svc.Spec.Ports = []corev1.ServicePort{
			{
				Name:       "driver-rpc",
				Port:       7078,
				TargetPort: intstr.FromInt(7078),
				Protocol:   corev1.ProtocolTCP,
			},
			{
				Name:       "blockmanager",
				Port:       7079,
				TargetPort: intstr.FromInt(7079),
				Protocol:   corev1.ProtocolTCP,
			},
			{
				Name:       "web-ui",
				Port:       4040,
				TargetPort: intstr.FromInt(4040),
				Protocol:   corev1.ProtocolTCP,
			},
			{
				Name:       "spark-connect-server",
				Port:       15002,
				TargetPort: intstr.FromInt(15002),
				Protocol:   corev1.ProtocolTCP,
			},
		}

		// Set pod label selector on server service.
		if svc.Spec.Selector == nil {
			svc.Spec.Selector = map[string]string{}
		}
		for key, val := range GetServerSelectorLabels(conn) {
			svc.Spec.Selector[key] = val
		}
	}

	// Set labels on server service.
	if svc.Labels == nil {
		svc.Labels = make(map[string]string)
	}
	for key, val := range GetCommonLabels(conn) {
		svc.Labels[key] = val
	}

	// Set controller owner reference on server service.
	if err := ctrl.SetControllerReference(conn, svc, r.scheme); err != nil {
		return fmt.Errorf("failed to set controller reference: %v", err)
	}

	return nil
}

// updateSparkConnectStatus updates the status of the SparkConnect resource.
func (r *Reconciler) updateSparkConnectStatus(ctx context.Context, old *v1alpha1.SparkConnect, conn *v1alpha1.SparkConnect) error {
	logger := log.FromContext(ctx)
	if err := r.updateExecutorStatus(ctx, conn); err != nil {
		return fmt.Errorf("failed to update status: %v", err)
	}

	// Skip updating if the status is not changed.
	if equality.Semantic.DeepEqual(old.Status, conn.Status) {
		return nil
	}

	conn.Status.LastUpdateTime = metav1.Now()
	if err := r.client.Status().Update(ctx, conn); err != nil {
		return err
	}
	logger.Info("Updated SparkConnect", "state", conn.Status.State, "executors", conn.Status.Executors)

	return nil
}

// updateExecutorStatus updates the SparkConnect status with the executor status.
func (r *Reconciler) updateExecutorStatus(ctx context.Context, conn *v1alpha1.SparkConnect) error {
	podList, err := r.listExecutorPods(ctx, conn)
	if err != nil {
		return err
	}

	executors := make(map[string]int)
	for _, executor := range podList.Items {
		phase := strings.ToLower(string(executor.Status.Phase))
		executors[phase]++
	}

	conn.Status.Executors = executors
	return nil
}

// listExecutorPods lists all executor pods for a given SparkConnect resource.
func (r *Reconciler) listExecutorPods(ctx context.Context, conn *v1alpha1.SparkConnect) (*corev1.PodList, error) {
	labels := map[string]string{
		common.LabelLaunchedBySparkOperator: "true",
		common.LabelSparkConnectName:        conn.Name,
		common.LabelSparkRole:               common.SparkRoleExecutor,
	}
	pods := &corev1.PodList{}
	if err := r.client.List(ctx,
		pods,
		client.InNamespace(conn.Namespace),
		client.MatchingLabels(labels),
	); err != nil {
		return nil, fmt.Errorf("failed to list executor pods for SparkConnect %s/%s: %v", conn.Namespace, conn.Name, err)
	}
	return pods, nil
}
