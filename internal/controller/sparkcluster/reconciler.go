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

package sparkcluster

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

const (
	// Default Spark standalone ports.
	masterPort    = 7077
	masterWebPort = 8080
	workerWebPort = 8081
)

// Options defines the options of SparkCluster reconciler.
type Options struct {
	Namespaces        []string
	NamespaceSelector string
}

// Reconciler reconciles a SparkCluster object.
type Reconciler struct {
	manager  ctrl.Manager
	scheme   *runtime.Scheme
	client   client.Client
	recorder record.EventRecorder
	options  Options
}

var _ reconcile.Reconciler = &Reconciler{}

// NewReconciler creates a new SparkCluster Reconciler.
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

// SetupWithManager sets up the SparkCluster reconciler with the manager.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, options controller.Options) error {
	kind := "SparkCluster"
	options.LogConstructor = util.NewLogConstructor(mgr.GetLogger(), kind)

	namespacePredicate, err := util.NewNamespacePredicate(
		r.client,
		r.options.Namespaces,
		r.options.NamespaceSelector,
	)
	if err != nil {
		return fmt.Errorf("failed to create namespace predicate: %w", err)
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.SparkCluster{}).
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
					enqueueSparkCluster(e.Object, q)
				},
				UpdateFunc: func(ctx context.Context, e event.TypedUpdateEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
					enqueueSparkCluster(e.ObjectNew, q)
				},
				DeleteFunc: func(ctx context.Context, e event.TypedDeleteEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
					enqueueSparkCluster(e.Object, q)
				},
				GenericFunc: func(ctx context.Context, e event.TypedGenericEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
					enqueueSparkCluster(e.Object, q)
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

func enqueueSparkCluster(obj client.Object, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	labels := obj.GetLabels()
	name := labels[common.LabelSparkClusterName]
	if name != "" {
		q.AddRateLimited(reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: obj.GetNamespace(),
				Name:      name,
			},
		})
	}
}

// +kubebuilder:rbac:groups=,resources=events,verbs=create;update;patch
// +kubebuilder:rbac:groups=,resources=pods,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups=,resources=services,verbs=get;list;watch;create;update;delete
// +kubebuilder:rbac:groups=sparkoperator.k8s.io,resources=sparkclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sparkoperator.k8s.io,resources=sparkclusters/status,verbs=get;update;patch

// Reconcile implements reconcile.Reconciler.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (reconcile.Result, error) {
	old := &v1alpha1.SparkCluster{}
	if err := r.client.Get(ctx, req.NamespacedName, old); err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, err
	}

	logger := ctrl.LoggerFrom(ctx)
	cluster := old.DeepCopy()

	if !cluster.DeletionTimestamp.IsZero() {
		logger.Info("Skip reconciling SparkCluster in terminating state")
		return ctrl.Result{}, nil
	}

	logger.Info("Reconciling SparkCluster")

	if cluster.Status.StartTime.IsZero() {
		cluster.Status.StartTime = metav1.Now()
	}

	if err := r.createOrUpdateMasterPod(ctx, cluster); err != nil {
		return reconcile.Result{}, err
	}

	if err := r.createOrUpdateMasterService(ctx, cluster); err != nil {
		return reconcile.Result{}, err
	}

	if err := r.reconcileWorkerPods(ctx, cluster); err != nil {
		return reconcile.Result{}, err
	}

	if err := r.updateStatus(ctx, old, cluster); err != nil {
		if errors.IsConflict(err) {
			logger.V(1).Info("conflict updating SparkCluster status")
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to update SparkCluster status: %v", err)
	}

	return reconcile.Result{}, nil
}

func (r *Reconciler) createOrUpdateMasterPod(ctx context.Context, cluster *v1alpha1.SparkCluster) error {
	logger := ctrl.LoggerFrom(ctx)
	logger.V(1).Info("Create or update master pod")

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GetMasterPodName(cluster),
			Namespace: cluster.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.client, pod, func() error {
		return r.mutateMasterPod(cluster, pod)
	})
	if err != nil {
		return fmt.Errorf("failed to create or update master pod: %v", err)
	}

	// Update status.
	ready := util.IsPodReady(pod)
	if ready {
		condition := metav1.Condition{
			Type:    string(v1alpha1.SparkClusterConditionMasterPodReady),
			Status:  metav1.ConditionTrue,
			Reason:  string(v1alpha1.SparkClusterConditionReasonMasterPodReady),
			Message: "Master pod is ready",
		}
		_ = meta.SetStatusCondition(&cluster.Status.Conditions, condition)
		cluster.Status.State = v1alpha1.SparkClusterStateReady
	} else {
		condition := metav1.Condition{
			Type:    string(v1alpha1.SparkClusterConditionMasterPodReady),
			Status:  metav1.ConditionFalse,
			Reason:  string(v1alpha1.SparkClusterConditionReasonMasterPodNotReady),
			Message: fmt.Sprintf("Master pod is not ready: %s", pod.Status.Message),
		}
		_ = meta.SetStatusCondition(&cluster.Status.Conditions, condition)
		cluster.Status.State = v1alpha1.SparkClusterStateNotReady
	}

	cluster.Status.Master.PodName = pod.Name
	cluster.Status.Master.PodIP = pod.Status.PodIP

	return nil
}

func (r *Reconciler) mutateMasterPod(cluster *v1alpha1.SparkCluster, pod *corev1.Pod) error {
	if pod.CreationTimestamp.IsZero() {
		// Apply template if provided.
		if cluster.Spec.Master.Template != nil {
			applyPodTemplate(pod, cluster.Spec.Master.Template)
		}

		// Ensure at least one container exists.
		if len(pod.Spec.Containers) == 0 {
			pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{
				Name: "spark-master",
			})
		}

		container := &pod.Spec.Containers[0]

		// Set image if not already set from template.
		if container.Image == "" {
			if cluster.Spec.Image == nil || *cluster.Spec.Image == "" {
				return fmt.Errorf("image is not specified")
			}
			container.Image = *cluster.Spec.Image
		}

		// Set the command to start Spark master.
		container.Command = []string{"/opt/spark/sbin/start-master.sh"}
		container.Args = nil

		// Add environment variables.
		container.Env = appendEnvIfMissing(container.Env,
			corev1.EnvVar{Name: common.EnvSparkNoDaemonize, Value: "true"},
		)

		// Apply SparkConf as environment variables for master.
		for key, val := range cluster.Spec.SparkConf {
			envName := sparkConfToEnv(key)
			if envName != "" {
				container.Env = appendEnvIfMissing(container.Env,
					corev1.EnvVar{Name: envName, Value: val},
				)
			}
		}

		// Ensure master ports are exposed.
		container.Ports = ensureContainerPorts(container.Ports,
			corev1.ContainerPort{Name: "spark", ContainerPort: masterPort, Protocol: corev1.ProtocolTCP},
			corev1.ContainerPort{Name: "web-ui", ContainerPort: masterWebPort, Protocol: corev1.ProtocolTCP},
		)
	}

	// Set owner reference.
	if err := ctrl.SetControllerReference(cluster, pod, r.scheme); err != nil {
		return fmt.Errorf("failed to set controller reference: %v", err)
	}

	// Set labels.
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	for key, val := range GetMasterSelectorLabels(cluster) {
		pod.Labels[key] = val
	}
	pod.Labels[common.LabelSparkVersion] = cluster.Spec.SparkVersion

	return nil
}

func (r *Reconciler) createOrUpdateMasterService(ctx context.Context, cluster *v1alpha1.SparkCluster) error {
	logger := ctrl.LoggerFrom(ctx)
	logger.V(1).Info("Create or update master service")

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GetMasterServiceName(cluster),
			Namespace: cluster.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.client, svc, func() error {
		return r.mutateMasterService(cluster, svc)
	})
	if err != nil {
		return fmt.Errorf("failed to create or update master service: %v", err)
	}

	cluster.Status.Master.ServiceName = svc.Name
	return nil
}

func (r *Reconciler) mutateMasterService(cluster *v1alpha1.SparkCluster, svc *corev1.Service) error {
	if svc.CreationTimestamp.IsZero() {
		svc.Spec.Ports = []corev1.ServicePort{
			{
				Name:        "spark",
				Port:        masterPort,
				TargetPort:  intstr.FromInt(masterPort),
				Protocol:    corev1.ProtocolTCP,
				AppProtocol: ptr.To("tcp"),
			},
			{
				Name:        "web-ui",
				Port:        masterWebPort,
				TargetPort:  intstr.FromInt(masterWebPort),
				Protocol:    corev1.ProtocolTCP,
				AppProtocol: ptr.To("http"),
			},
		}

		if svc.Spec.Selector == nil {
			svc.Spec.Selector = map[string]string{}
		}
		for key, val := range GetMasterSelectorLabels(cluster) {
			svc.Spec.Selector[key] = val
		}
	}

	// Set labels.
	if svc.Labels == nil {
		svc.Labels = map[string]string{}
	}
	for key, val := range GetCommonLabels(cluster) {
		svc.Labels[key] = val
	}

	if err := ctrl.SetControllerReference(cluster, svc, r.scheme); err != nil {
		return fmt.Errorf("failed to set controller reference: %v", err)
	}

	return nil
}

func (r *Reconciler) reconcileWorkerPods(ctx context.Context, cluster *v1alpha1.SparkCluster) error {
	logger := ctrl.LoggerFrom(ctx)

	masterURL := fmt.Sprintf("spark://%s:%d", GetMasterServiceHost(cluster), masterPort)

	for _, group := range cluster.Spec.WorkerGroups {
		replicas := int32(1)
		if group.Replicas != nil {
			replicas = *group.Replicas
		}

		logger.V(1).Info("Reconciling worker group", "group", group.Name, "replicas", replicas)

		// Create or update the desired number of worker pods.
		for i := int32(0); i < replicas; i++ {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      GetWorkerPodName(cluster, group.Name, int(i)),
					Namespace: cluster.Namespace,
				},
			}

			_, err := controllerutil.CreateOrUpdate(ctx, r.client, pod, func() error {
				return r.mutateWorkerPod(cluster, &group, pod, masterURL)
			})
			if err != nil {
				return fmt.Errorf("failed to create or update worker pod %s: %v", pod.Name, err)
			}
		}

		// Delete excess worker pods if replicas decreased.
		existingPods := &corev1.PodList{}
		if err := r.client.List(ctx, existingPods,
			client.InNamespace(cluster.Namespace),
			client.MatchingLabels(GetWorkerSelectorLabels(cluster, group.Name)),
		); err != nil {
			return fmt.Errorf("failed to list worker pods: %v", err)
		}

		for _, existing := range existingPods.Items {
			// Check if this pod is beyond the desired replicas.
			keep := false
			for i := int32(0); i < replicas; i++ {
				if existing.Name == GetWorkerPodName(cluster, group.Name, int(i)) {
					keep = true
					break
				}
			}
			if !keep {
				logger.Info("Deleting excess worker pod", "pod", existing.Name)
				if err := r.client.Delete(ctx, &existing); err != nil && !errors.IsNotFound(err) {
					return fmt.Errorf("failed to delete excess worker pod %s: %v", existing.Name, err)
				}
			}
		}
	}

	return nil
}

func (r *Reconciler) mutateWorkerPod(cluster *v1alpha1.SparkCluster, group *v1alpha1.WorkerGroupSpec, pod *corev1.Pod, masterURL string) error {
	if pod.CreationTimestamp.IsZero() {
		if group.Template != nil {
			applyPodTemplate(pod, group.Template)
		}

		// Ensure at least one container exists.
		if len(pod.Spec.Containers) == 0 {
			pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{
				Name: "spark-worker",
			})
		}

		container := &pod.Spec.Containers[0]

		// Set image.
		if container.Image == "" {
			if cluster.Spec.Image == nil || *cluster.Spec.Image == "" {
				return fmt.Errorf("image is not specified")
			}
			container.Image = *cluster.Spec.Image
		}

		// Set the command to start Spark worker.
		container.Command = []string{"/opt/spark/sbin/start-worker.sh"}
		container.Args = []string{masterURL}

		// Add environment variables.
		container.Env = appendEnvIfMissing(container.Env,
			corev1.EnvVar{Name: common.EnvSparkNoDaemonize, Value: "true"},
		)

		// Apply SparkConf.
		for key, val := range cluster.Spec.SparkConf {
			envName := sparkConfToEnv(key)
			if envName != "" {
				container.Env = appendEnvIfMissing(container.Env,
					corev1.EnvVar{Name: envName, Value: val},
				)
			}
		}

		// Ensure worker ports.
		container.Ports = ensureContainerPorts(container.Ports,
			corev1.ContainerPort{Name: "web-ui", ContainerPort: workerWebPort, Protocol: corev1.ProtocolTCP},
		)
	}

	// Set owner reference.
	if err := ctrl.SetControllerReference(cluster, pod, r.scheme); err != nil {
		return fmt.Errorf("failed to set controller reference: %v", err)
	}

	// Set labels.
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	for key, val := range GetWorkerSelectorLabels(cluster, group.Name) {
		pod.Labels[key] = val
	}
	pod.Labels[common.LabelSparkVersion] = cluster.Spec.SparkVersion

	return nil
}

func (r *Reconciler) updateStatus(ctx context.Context, old *v1alpha1.SparkCluster, cluster *v1alpha1.SparkCluster) error {
	// Count worker pods by phase.
	podList := &corev1.PodList{}
	if err := r.client.List(ctx, podList,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{
			common.LabelLaunchedBySparkOperator: "true",
			common.LabelSparkClusterName:        cluster.Name,
			common.LabelSparkRole:               common.SparkRoleClusterWorker,
		},
	); err != nil {
		return fmt.Errorf("failed to list worker pods: %v", err)
	}

	workers := make(map[string]int)
	for _, pod := range podList.Items {
		phase := strings.ToLower(string(pod.Status.Phase))
		workers[phase]++
	}
	cluster.Status.Workers = workers

	if equality.Semantic.DeepEqual(old.Status, cluster.Status) {
		return nil
	}

	cluster.Status.LastUpdateTime = metav1.Now()
	return r.client.Status().Update(ctx, cluster)
}

// applyPodTemplate applies the SparkCluster pod template to a Pod.
func applyPodTemplate(pod *corev1.Pod, tmpl *v1alpha1.SparkClusterPodTemplateSpec) {
	if tmpl.Metadata != nil {
		pod.Labels = tmpl.Metadata.Labels
		pod.Annotations = tmpl.Metadata.Annotations
	}

	if tmpl.Spec == nil {
		return
	}

	pod.Spec.NodeSelector = tmpl.Spec.NodeSelector
	pod.Spec.ServiceAccountName = tmpl.Spec.ServiceAccountName

	// Convert tolerations.
	for _, t := range tmpl.Spec.Tolerations {
		pod.Spec.Tolerations = append(pod.Spec.Tolerations, corev1.Toleration{
			Key:      t.Key,
			Operator: corev1.TolerationOperator(t.Operator),
			Value:    t.Value,
			Effect:   corev1.TaintEffect(t.Effect),
		})
	}

	// Convert containers.
	for _, c := range tmpl.Spec.Containers {
		container := corev1.Container{
			Name:            c.Name,
			Image:           c.Image,
			ImagePullPolicy: corev1.PullPolicy(c.ImagePullPolicy),
		}

		if c.Resources != nil {
			container.Resources = convertResources(c.Resources)
		}

		if c.SecurityContext != nil {
			container.SecurityContext = convertSecurityContext(c.SecurityContext)
		}

		for _, e := range c.Env {
			container.Env = append(container.Env, corev1.EnvVar{Name: e.Name, Value: e.Value})
		}

		for _, p := range c.Ports {
			container.Ports = append(container.Ports, corev1.ContainerPort{
				Name:          p.Name,
				ContainerPort: p.ContainerPort,
				Protocol:      corev1.Protocol(p.Protocol),
			})
		}

		pod.Spec.Containers = append(pod.Spec.Containers, container)
	}
}

func convertResources(r *v1alpha1.ResourceRequirements) corev1.ResourceRequirements {
	result := corev1.ResourceRequirements{}
	if len(r.Requests) > 0 {
		result.Requests = make(corev1.ResourceList)
		for k, v := range r.Requests {
			result.Requests[corev1.ResourceName(k)] = resource.MustParse(v)
		}
	}
	if len(r.Limits) > 0 {
		result.Limits = make(corev1.ResourceList)
		for k, v := range r.Limits {
			result.Limits[corev1.ResourceName(k)] = resource.MustParse(v)
		}
	}
	return result
}

func convertSecurityContext(sc *v1alpha1.SecurityContext) *corev1.SecurityContext {
	result := &corev1.SecurityContext{
		AllowPrivilegeEscalation: sc.AllowPrivilegeEscalation,
		RunAsNonRoot:             sc.RunAsNonRoot,
		RunAsUser:                sc.RunAsUser,
		RunAsGroup:               sc.RunAsGroup,
	}

	if sc.Capabilities != nil {
		result.Capabilities = &corev1.Capabilities{}
		for _, d := range sc.Capabilities.Drop {
			result.Capabilities.Drop = append(result.Capabilities.Drop, corev1.Capability(d))
		}
		for _, a := range sc.Capabilities.Add {
			result.Capabilities.Add = append(result.Capabilities.Add, corev1.Capability(a))
		}
	}

	if sc.SeccompProfile != nil {
		result.SeccompProfile = &corev1.SeccompProfile{
			Type: corev1.SeccompProfileType(sc.SeccompProfile.Type),
		}
	}

	return result
}

// appendEnvIfMissing adds an env var only if no env var with the same name exists.
func appendEnvIfMissing(envs []corev1.EnvVar, newEnv corev1.EnvVar) []corev1.EnvVar {
	for _, e := range envs {
		if e.Name == newEnv.Name {
			return envs
		}
	}
	return append(envs, newEnv)
}

// ensureContainerPorts adds ports that are not already present.
func ensureContainerPorts(existing []corev1.ContainerPort, ports ...corev1.ContainerPort) []corev1.ContainerPort {
	for _, p := range ports {
		found := false
		for _, e := range existing {
			if e.ContainerPort == p.ContainerPort {
				found = true
				break
			}
		}
		if !found {
			existing = append(existing, p)
		}
	}
	return existing
}

// sparkConfToEnv converts known Spark configuration keys to environment variable names.
func sparkConfToEnv(key string) string {
	switch key {
	case "spark.master.ui.port":
		return "SPARK_MASTER_WEBUI_PORT"
	case "spark.master.ui.title":
		return ""
	case "spark.worker.ui.port":
		return "SPARK_WORKER_WEBUI_PORT"
	default:
		return ""
	}
}
