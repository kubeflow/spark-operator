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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
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
	// ServerPodSpecHashAnnotationKey is the annotation key for storing the server pod spec hash
	ServerPodSpecHashAnnotationKey = "sparkoperator.k8s.io/server-pod-spec-hash"
)

// Options defines the options of SparkConnect reconciler.
type Options struct {
	// A list of namespaces that should be watched.
	Namespaces []string
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
		WithEventFilter(util.NewNamespacePredicate(r.options.Namespaces)).
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

// buildServerPodSpec builds the server pod spec from the SparkConnect resource.
// This is a helper function used for both pod creation and hash computation.
func (r *Reconciler) buildServerPodSpec(conn *v1alpha1.SparkConnect, pod *corev1.Pod, isNewPod bool) error {
	// Apply template if provided (only on creation, as pod spec is immutable)
	if isNewPod {
		template := conn.Spec.Server.Template
		if template != nil {
			// Merge labels and annotations from template
			if pod.Labels == nil {
				pod.Labels = make(map[string]string)
			}
			for k, v := range template.Labels {
				pod.Labels[k] = v
			}
			if pod.Annotations == nil {
				pod.Annotations = make(map[string]string)
			}
			for k, v := range template.Annotations {
				pod.Annotations[k] = v
			}
			// Apply pod spec from template
			pod.Spec = template.Spec
		}
	}

	// Ensure annotations map exists
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
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

	// Setup image - always use spec image if provided, otherwise use container image from template
	if conn.Spec.Image != nil && *conn.Spec.Image != "" {
		container.Image = *conn.Spec.Image
	} else if container.Image == "" {
		return fmt.Errorf("image is not specified")
	}

	// Setup entrypoint - always set from spec
	container.Command = []string{"bash", "-c"}
	args, err := buildStartConnectServerArgs(conn)
	if err != nil {
		return fmt.Errorf("failed to build spark connection args: %v", err)
	}
	container.Args = []string{strings.Join(args, " ")}

	// Setup environment variables - ensure required env vars are present
	envVars := make(map[string]corev1.EnvVar)
	// Preserve existing env vars
	for _, env := range container.Env {
		envVars[env.Name] = env
	}
	// Add/update required env vars
	envVars["POD_IP"] = corev1.EnvVar{
		Name: "POD_IP",
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{
				FieldPath: "status.podIP",
			},
		},
	}
	envVars[common.EnvSparkNoDaemonize] = corev1.EnvVar{
		Name:  common.EnvSparkNoDaemonize,
		Value: "true",
	}
	// Convert back to slice
	container.Env = make([]corev1.EnvVar, 0, len(envVars))
	for _, env := range envVars {
		container.Env = append(container.Env, env)
	}

	// Setup volumes and volumeMounts - ensure ConfigMap volume is present
	volumeMounts := make(map[string]corev1.VolumeMount)
	for _, vm := range container.VolumeMounts {
		volumeMounts[vm.Name] = vm
	}
	volumeMounts[common.SparkConfigMapVolumeMountName] = corev1.VolumeMount{
		Name:      common.SparkConfigMapVolumeMountName,
		SubPath:   ExecutorPodTemplateFileName,
		MountPath: fmt.Sprintf("/tmp/spark/%s", ExecutorPodTemplateFileName),
		ReadOnly:  true,
	}
	container.VolumeMounts = make([]corev1.VolumeMount, 0, len(volumeMounts))
	for _, vm := range volumeMounts {
		container.VolumeMounts = append(container.VolumeMounts, vm)
	}

	// Setup lifecycle hook - always set PreStop hook
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

	// Ensure ConfigMap volume is present
	volumes := make(map[string]corev1.Volume)
	for _, vol := range pod.Spec.Volumes {
		volumes[vol.Name] = vol
	}
	volumes[common.SparkConfigMapVolumeMountName] = corev1.Volume{
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
	}
	pod.Spec.Volumes = make([]corev1.Volume, 0, len(volumes))
	for _, vol := range volumes {
		pod.Spec.Volumes = append(pod.Spec.Volumes, vol)
	}

	return nil
}

// computeServerPodSpecHash computes a deterministic hash of the server pod spec.
// This hash is used to detect when the pod spec has changed and needs to be recreated.
func (r *Reconciler) computeServerPodSpecHash(conn *v1alpha1.SparkConnect) (string, error) {
	// Create a temporary pod to compute the hash from the desired spec
	tempPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GetServerPodName(conn),
			Namespace: conn.Namespace,
		},
	}

	// Build the pod spec without setting annotations or owner references
	if err := r.buildServerPodSpec(conn, tempPod, true); err != nil {
		return "", fmt.Errorf("failed to build pod spec for hash computation: %v", err)
	}

	// Create a hashable representation of the pod spec
	// We hash the container image, command, args, env vars, volumes, and resources
	hashData := struct {
		Image     string
		Command   []string
		Args      []string
		Env       []corev1.EnvVar
		Resources corev1.ResourceRequirements
		Volumes   []corev1.Volume
		Labels    map[string]string
	}{
		Image:     "",
		Command:   []string{},
		Args:      []string{},
		Env:       []corev1.EnvVar{},
		Resources: corev1.ResourceRequirements{},
		Volumes:   []corev1.Volume{},
		Labels:    tempPod.Labels,
	}

	// Find the server container
	index := 0
	for i, container := range tempPod.Spec.Containers {
		if container.Name == common.SparkDriverContainerName {
			index = i
			break
		}
	}
	if index < len(tempPod.Spec.Containers) {
		container := tempPod.Spec.Containers[index]
		hashData.Image = container.Image
		hashData.Command = container.Command
		hashData.Args = container.Args
		// Sort env vars by name for deterministic hashing
		envVars := make([]corev1.EnvVar, len(container.Env))
		copy(envVars, container.Env)
		sort.Slice(envVars, func(i, j int) bool {
			return envVars[i].Name < envVars[j].Name
		})
		hashData.Env = envVars
		hashData.Resources = container.Resources
	}
	// Sort volumes by name for deterministic hashing
	volumes := make([]corev1.Volume, len(tempPod.Spec.Volumes))
	copy(volumes, tempPod.Spec.Volumes)
	sort.Slice(volumes, func(i, j int) bool {
		return volumes[i].Name < volumes[j].Name
	})
	hashData.Volumes = volumes

	// Serialize to YAML for hashing
	hashBytes, err := yaml.Marshal(hashData)
	if err != nil {
		return "", fmt.Errorf("failed to marshal pod spec for hashing: %v", err)
	}

	// Compute SHA256 hash
	hash := sha256.Sum256(hashBytes)
	return hex.EncodeToString(hash[:]), nil
}

// needsPodRestart checks if the pod needs to be restarted due to spec changes.
func (r *Reconciler) needsPodRestart(ctx context.Context, conn *v1alpha1.SparkConnect, pod *corev1.Pod) (bool, string, error) {
	// If pod doesn't exist, no restart needed (will be created)
	if pod == nil || pod.CreationTimestamp.IsZero() {
		return false, "", nil
	}

	// If pod is being deleted, don't restart
	if !pod.DeletionTimestamp.IsZero() {
		return false, "", nil
	}

	// Compute desired spec hash
	desiredHash, err := r.computeServerPodSpecHash(conn)
	if err != nil {
		return false, "", fmt.Errorf("failed to compute desired pod spec hash: %v", err)
	}

	// Get current hash from pod annotations
	currentHash := pod.Annotations[ServerPodSpecHashAnnotationKey]

	// If hashes differ, pod needs restart
	if currentHash != desiredHash {
		return true, desiredHash, nil
	}

	return false, desiredHash, nil
}

// createOrUpdateServerPod creates or updates the server pod for the SparkConnect resource.
func (r *Reconciler) createOrUpdateServerPod(ctx context.Context, conn *v1alpha1.SparkConnect) error {
	logger := ctrl.LoggerFrom(ctx)
	logger.V(1).Info("Create or update server pod")

	podName := GetServerPodName(conn)
	podKey := types.NamespacedName{
		Name:      podName,
		Namespace: conn.Namespace,
	}

	// Check if pod exists and if it needs restart
	existingPod := &corev1.Pod{}
	err := r.client.Get(ctx, podKey, existingPod)
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to get existing server pod: %v", err)
	}

	// Check if pod needs restart due to spec changes
	needsRestart := false
	desiredHash := ""
	if err == nil {
		var err2 error
		needsRestart, desiredHash, err2 = r.needsPodRestart(ctx, conn, existingPod)
		if err2 != nil {
			return fmt.Errorf("failed to check if pod needs restart: %v", err2)
		}
	}

	// If pod needs restart, delete it first
	if needsRestart {
		logger.Info("Server pod spec changed, deleting pod for recreation",
			"pod", podName,
			"oldHash", existingPod.Annotations[ServerPodSpecHashAnnotationKey],
			"newHash", desiredHash)

		// Set updating condition
		condition := metav1.Condition{
			Type:    string(v1alpha1.SparkConnectConditionServerPodUpdating),
			Status:  metav1.ConditionTrue,
			Reason:  string(v1alpha1.SparkConnectConditionReasonServerPodSpecChanged),
			Message: fmt.Sprintf("Server pod spec changed, restarting pod with new spec hash: %s", desiredHash),
		}
		_ = meta.SetStatusCondition(&conn.Status.Conditions, condition)
		conn.Status.State = v1alpha1.SparkConnectStateNotReady

		// Emit event
		r.recorder.Eventf(conn, corev1.EventTypeNormal, "SparkConnectServerPodSpecChanged",
			"Server pod spec changed, restarting pod %s", podName)

		// Delete the pod
		if err := r.client.Delete(ctx, existingPod); err != nil {
			if !errors.IsNotFound(err) {
				return fmt.Errorf("failed to delete server pod for restart: %v", err)
			}
		} else {
			logger.Info("Deleted server pod for restart", "pod", podName)
			r.recorder.Eventf(conn, corev1.EventTypeNormal, "SparkConnectServerPodDeleting",
				"Deleted server pod %s for restart", podName)
		}

		// Wait for deletion to complete before recreating
		// The next reconcile will recreate the pod
		return nil
	}

	// Create or update the pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: conn.Namespace,
		},
	}

	_, err = controllerutil.CreateOrUpdate(ctx, r.client, pod, func() error {
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
		// Remove updating condition if present
		_ = meta.RemoveStatusCondition(&conn.Status.Conditions, string(v1alpha1.SparkConnectConditionServerPodUpdating))
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
// This function always applies mutations, not just on creation, to ensure the pod spec
// matches the desired state from the SparkConnect resource.
func (r *Reconciler) mutateServerPod(_ context.Context, conn *v1alpha1.SparkConnect, pod *corev1.Pod) error {
	isNewPod := pod.CreationTimestamp.IsZero()

	// Build the pod spec
	if err := r.buildServerPodSpec(conn, pod, isNewPod); err != nil {
		return err
	}

	// Compute and set spec hash annotation
	specHash, err := r.computeServerPodSpecHash(conn)
	if err != nil {
		return fmt.Errorf("failed to compute pod spec hash: %v", err)
	}
	pod.Annotations[ServerPodSpecHashAnnotationKey] = specHash

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
