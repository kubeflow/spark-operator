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

package workload

import (
	"context"
	"fmt"

	schedulingv1alpha2 "k8s.io/api/scheduling/v1alpha2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/internal/scheduler"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

var (
	logger = log.Log.WithName("workload-scheduler")
)

const (
	// SchedulerName is the name of the workload scheduler
	SchedulerName = "workload"

	// podGroupTemplateName is the fixed template name used in Workload.spec.podGroupTemplates
	podGroupTemplateName = "spark-gang"

	// workloadAPIGroupVersion is the API group/version for discovery
	workloadAPIGroupVersion = "scheduling.k8s.io/v1alpha2"
)

// Scheduler is a batch scheduler that uses Kubernetes native Workload API (scheduling/v1alpha2)
// to schedule Spark applications with gang scheduling semantics.
type Scheduler struct {
	client client.Client
}

// Scheduler implements scheduler.Interface.
var _ scheduler.Interface = &Scheduler{}

// Config defines the configurations of Workload scheduler.
type Config struct {
	RestConfig *rest.Config
	Client     client.Client
	// DiscoveryClient is optional for testing; if nil, one is created from RestConfig
	DiscoveryClient discovery.DiscoveryInterface
}

// Config implements scheduler.Config.
var _ scheduler.Config = &Config{}

// Factory creates a new Workload scheduler instance with API discovery validation.
func Factory(config scheduler.Config) (scheduler.Interface, error) {
	c, ok := config.(*Config)
	if !ok {
		return nil, fmt.Errorf("failed to get workload scheduler config")
	}

	if c.Client == nil {
		return nil, fmt.Errorf("workload scheduler: Client is required")
	}

	// Create or use provided discovery client
	dc := c.DiscoveryClient
	if dc == nil {
		if c.RestConfig == nil {
			return nil, fmt.Errorf("workload scheduler: RestConfig is required when DiscoveryClient is not provided")
		}
		var err error
		dc, err = discovery.NewDiscoveryClientForConfig(c.RestConfig)
		if err != nil {
			return nil, fmt.Errorf("workload scheduler: failed to build discovery client: %w", err)
		}
	}

	// Probe for scheduling.k8s.io/v1alpha2 API availability
	if _, err := dc.ServerResourcesForGroupVersion(workloadAPIGroupVersion); err != nil {
		return nil, fmt.Errorf(
			"workload scheduler: %s is not being served by the API server "+
				"(requires Kubernetes v1.36+ with the GenericWorkload feature gate enabled): %w",
			workloadAPIGroupVersion, err)
	}

	return &Scheduler{
		client: c.Client,
	}, nil
}

// Name implements scheduler.Interface.
func (s *Scheduler) Name() string {
	return SchedulerName
}

// ShouldSchedule implements scheduler.Interface.
func (s *Scheduler) ShouldSchedule(_ *v1beta2.SparkApplication) bool {
	// There is no additional requirement for workload scheduler
	return true
}

// Schedule implements scheduler.Interface.
// It creates Workload and PodGroup objects, and stamps the pod templates with schedulingGroup references.
func (s *Scheduler) Schedule(app *v1beta2.SparkApplication) error {
	ctx := context.TODO()

	// 1. Create or update Workload
	workloadName := getWorkloadName(app)
	if err := s.syncWorkload(ctx, app); err != nil {
		return fmt.Errorf("failed to sync Workload: %w", err)
	}

	// 2. Create or update PodGroup
	podGroupName := getPodGroupName(app)
	if err := s.syncPodGroup(ctx, app); err != nil {
		return fmt.Errorf("failed to sync PodGroup: %w", err)
	}

	// 3. Stamp executor pod template with schedulingGroup for gang scheduling.
	// The cluster-mode driver must schedule independently because it creates the executor pods.
	// Adding it to the executor PodGroup would deadlock while waiting for executors that do not yet exist.
	schedulingGroup := &v1beta2.PodSchedulingGroup{
		PodGroupName: podGroupName,
	}

	app.Spec.Executor.SchedulingGroup = schedulingGroup
	logger.Info("Stamped executor pod template with schedulingGroup",
		"app", app.Name,
		"namespace", app.Namespace,
		"mode", app.Spec.Mode,
		"podGroup", podGroupName,
		"workload", workloadName,
	)

	return nil
}

// Cleanup implements scheduler.Interface.
// It deletes only the PodGroup (per-submission runtime unit).
// The Workload (policy template) is retained and garbage-collected via ownerReference.
func (s *Scheduler) Cleanup(app *v1beta2.SparkApplication) error {
	ctx := context.TODO()
	podGroupName := getPodGroupName(app)
	namespace := app.Namespace

	// Delete PodGroup only (not Workload)
	pg := &schedulingv1alpha2.PodGroup{}
	err := s.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podGroupName}, pg)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get PodGroup %s/%s: %w", namespace, podGroupName, err)
	}

	if err := s.client.Delete(ctx, pg); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete PodGroup %s/%s: %w", namespace, podGroupName, err)
	}

	logger.Info("Deleted PodGroup", "name", podGroupName, "namespace", namespace)
	return nil
}

// syncWorkload creates the Workload object if it doesn't exist.
// Workloads are immutable after creation, so updates are not performed.
func (s *Scheduler) syncWorkload(ctx context.Context, app *v1beta2.SparkApplication) error {
	workloadName := getWorkloadName(app)
	namespace := app.Namespace

	// Check if Workload already exists
	existing := &schedulingv1alpha2.Workload{}
	err := s.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: workloadName}, existing)
	if err == nil {
		// Workload exists - it's immutable, so we don't update it
		logger.V(1).Info("Workload already exists", "name", workloadName, "namespace", namespace)
		return nil
	}

	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to get Workload: %w", err)
	}

	// Create new Workload
	workload := buildWorkload(app)
	if err := s.client.Create(ctx, workload); err != nil {
		return fmt.Errorf("failed to create Workload: %w", err)
	}

	logger.Info("Created Workload", "name", workloadName, "namespace", namespace)
	return nil
}

// syncPodGroup creates the PodGroup object if it doesn't exist.
func (s *Scheduler) syncPodGroup(ctx context.Context, app *v1beta2.SparkApplication) error {
	podGroupName := getPodGroupName(app)
	workloadName := getWorkloadName(app)
	namespace := app.Namespace

	// Check if PodGroup already exists
	existing := &schedulingv1alpha2.PodGroup{}
	err := s.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podGroupName}, existing)
	if err == nil {
		// PodGroup exists - most fields are immutable
		logger.V(1).Info("PodGroup already exists", "name", podGroupName, "namespace", namespace)
		return nil
	}

	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to get PodGroup: %w", err)
	}

	// Create new PodGroup
	podGroup := buildPodGroup(app, workloadName)
	if err := s.client.Create(ctx, podGroup); err != nil {
		return fmt.Errorf("failed to create PodGroup: %w", err)
	}

	logger.Info("Created PodGroup", "name", podGroupName, "namespace", namespace)
	return nil
}

// buildWorkload constructs a Workload object from a SparkApplication.
func buildWorkload(app *v1beta2.SparkApplication) *schedulingv1alpha2.Workload {
	workloadName := getWorkloadName(app)
	minCount := calculateMinCount(app)

	podGroupTemplate := schedulingv1alpha2.PodGroupTemplate{
		Name: podGroupTemplateName,
		SchedulingPolicy: schedulingv1alpha2.PodGroupSchedulingPolicy{
			Gang: &schedulingv1alpha2.GangSchedulingPolicy{
				MinCount: minCount,
			},
		},
	}

	// Add priority if specified
	if app.Spec.BatchSchedulerOptions != nil && app.Spec.BatchSchedulerOptions.PriorityClassName != nil {
		podGroupTemplate.PriorityClassName = *app.Spec.BatchSchedulerOptions.PriorityClassName
	}

	workload := &schedulingv1alpha2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      workloadName,
			Namespace: app.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(app, v1beta2.SchemeGroupVersion.WithKind("SparkApplication")),
			},
		},
		Spec: schedulingv1alpha2.WorkloadSpec{
			ControllerRef: &schedulingv1alpha2.TypedLocalObjectReference{
				APIGroup: v1beta2.GroupVersion.Group,
				Kind:     "SparkApplication",
				Name:     app.Name,
			},
			PodGroupTemplates: []schedulingv1alpha2.PodGroupTemplate{
				podGroupTemplate,
			},
		},
	}

	return workload
}

// buildPodGroup constructs a PodGroup object from a SparkApplication.
func buildPodGroup(app *v1beta2.SparkApplication, workloadName string) *schedulingv1alpha2.PodGroup {
	podGroupName := getPodGroupName(app)
	minCount := calculateMinCount(app)

	podGroup := &schedulingv1alpha2.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podGroupName,
			Namespace: app.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(app, v1beta2.SchemeGroupVersion.WithKind("SparkApplication")),
			},
		},
		Spec: schedulingv1alpha2.PodGroupSpec{
			PodGroupTemplateRef: &schedulingv1alpha2.PodGroupTemplateReference{
				Workload: &schedulingv1alpha2.WorkloadPodGroupTemplateReference{
					WorkloadName:         workloadName,
					PodGroupTemplateName: podGroupTemplateName,
				},
			},
			SchedulingPolicy: schedulingv1alpha2.PodGroupSchedulingPolicy{
				Gang: &schedulingv1alpha2.GangSchedulingPolicy{
					MinCount: minCount,
				},
			},
		},
	}

	// Add priority if specified
	if app.Spec.BatchSchedulerOptions != nil && app.Spec.BatchSchedulerOptions.PriorityClassName != nil {
		podGroup.Spec.PriorityClassName = *app.Spec.BatchSchedulerOptions.PriorityClassName
	}

	return podGroup
}

// calculateMinCount determines the minimum number of pods for gang scheduling.
// Uses util.GetInitialExecutorNumber for DRA-aware sizing.
func calculateMinCount(app *v1beta2.SparkApplication) int32 {
	// Check for explicit MinMember override
	if app.Spec.BatchSchedulerOptions != nil && app.Spec.BatchSchedulerOptions.MinMember != nil {
		return *app.Spec.BatchSchedulerOptions.MinMember
	}

	// Only executors participate in gang scheduling.
	// The cluster-mode driver schedules independently to avoid bootstrap deadlock.
	return util.GetInitialExecutorNumber(app)
}

// getWorkloadName returns the Workload name for a SparkApplication.
// Workloads are 1:1 with SparkApplications, so we use the app name directly.
func getWorkloadName(app *v1beta2.SparkApplication) string {
	return app.Name
}

// getPodGroupName returns the PodGroup name for a SparkApplication.
// PodGroups are per-submission, so we use app.Name + submission ID.
func getPodGroupName(app *v1beta2.SparkApplication) string {
	return fmt.Sprintf("%s-%s", app.Name, app.Status.SubmissionID)
}

// AddToScheme adds schedulingv1alpha2 types to the given scheme.
// This is exposed for external callers that need to register these types.
func AddToScheme(s *runtime.Scheme) error {
	return schedulingv1alpha2.AddToScheme(s)
}
