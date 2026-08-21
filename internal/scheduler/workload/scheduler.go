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

corev1 "k8s.io/api/core/v1"
schedulingv1alpha2 "k8s.io/api/scheduling/v1alpha2"
"k8s.io/apimachinery/pkg/api/errors"
metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
"k8s.io/client-go/kubernetes"
"k8s.io/client-go/rest"
"sigs.k8s.io/controller-runtime/pkg/log"

"github.com/kubeflow/spark-operator/v2/api/v1beta2"
"github.com/kubeflow/spark-operator/v2/internal/scheduler"
"github.com/kubeflow/spark-operator/v2/pkg/common"
"github.com/kubeflow/spark-operator/v2/pkg/util"
)

var (
logger = log.Log.WithName("")
)

const (
// SparkGangPodGroupTemplateName is the fixed template name used in Workload.spec.podGroupTemplates
SparkGangPodGroupTemplateName = "spark-gang"
)

// Scheduler is a batch scheduler that uses Kubernetes native Workload API (scheduling/v1alpha2)
// to schedule Spark applications with gang scheduling semantics.
type Scheduler struct {
kubeClient kubernetes.Interface
}

// Scheduler implements scheduler.Interface.
var _ scheduler.Interface = &Scheduler{}

// Config defines the configurations of Workload scheduler.
type Config struct {
RestConfig *rest.Config
}

// Config implements scheduler.Config.
var _ scheduler.Config = &Config{}

// Factory creates a new Workload scheduler instance.
func Factory(config scheduler.Config) (scheduler.Interface, error) {
c, ok := config.(*Config)
if !ok {
return nil, fmt.Errorf("failed to get workload scheduler config")
}

kubeClient, err := kubernetes.NewForConfig(c.RestConfig)
if err != nil {
return nil, fmt.Errorf("failed to initialize kubernetes client: %v", err)
}

scheduler := &Scheduler{
kubeClient: kubeClient,
}
return scheduler, nil
}

// Name implements scheduler.Interface.
func (s *Scheduler) Name() string {
return common.WorkloadSchedulerName
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

// Initialize annotations if needed
if app.Annotations == nil {
app.Annotations = make(map[string]string)
}
if app.Spec.Driver.Annotations == nil {
app.Spec.Driver.Annotations = make(map[string]string)
}
if app.Spec.Executor.Annotations == nil {
app.Spec.Executor.Annotations = make(map[string]string)
}

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

// 3. Stamp pod templates with schedulingGroup
// This tells Kubernetes to associate these pods with the PodGroup for gang scheduling
schedulingGroup := &v1beta2.PodSchedulingGroup{
PodGroupName: podGroupName,
}

switch app.Spec.Mode {
case v1beta2.DeployModeClient:
// Client mode: only executors need gang scheduling
app.Spec.Executor.SchedulingGroup = schedulingGroup
logger.Info("Stamped executor pod template with schedulingGroup",
"app", app.Name,
"namespace", app.Namespace,
"podGroup", podGroupName,
"workload", workloadName,
)

case v1beta2.DeployModeCluster:
// Cluster mode: both driver and executors need gang scheduling
app.Spec.Driver.SchedulingGroup = schedulingGroup
app.Spec.Executor.SchedulingGroup = schedulingGroup
logger.Info("Stamped driver and executor pod templates with schedulingGroup",
"app", app.Name,
"namespace", app.Namespace,
"podGroup", podGroupName,
"workload", workloadName,
)
}

return nil
}

// Cleanup implements scheduler.Interface.
// It deletes the PodGroup and Workload objects associated with the application.
func (s *Scheduler) Cleanup(app *v1beta2.SparkApplication) error {
ctx := context.TODO()
namespace := app.Namespace

// Delete PodGroup first (child object)
podGroupName := getPodGroupName(app)
if err := s.kubeClient.SchedulingV1alpha2().PodGroups(namespace).Delete(ctx, podGroupName, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
return fmt.Errorf("failed to delete PodGroup %s/%s: %w", namespace, podGroupName, err)
}
logger.Info("Deleted PodGroup", "name", podGroupName, "namespace", namespace)

// Delete Workload (parent object)
workloadName := getWorkloadName(app)
if err := s.kubeClient.SchedulingV1alpha2().Workloads(namespace).Delete(ctx, workloadName, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
return fmt.Errorf("failed to delete Workload %s/%s: %w", namespace, workloadName, err)
}
logger.Info("Deleted Workload", "name", workloadName, "namespace", namespace)

return nil
}

// syncWorkload creates or updates the Workload object for the SparkApplication.
func (s *Scheduler) syncWorkload(ctx context.Context, app *v1beta2.SparkApplication) error {
workloadName := getWorkloadName(app)
namespace := app.Namespace

// Check if Workload already exists
_, err := s.kubeClient.SchedulingV1alpha2().Workloads(namespace).Get(ctx, workloadName, metav1.GetOptions{})
if err != nil {
if !errors.IsNotFound(err) {
return err
}

// Create new Workload
workload := buildWorkload(app)
_, err = s.kubeClient.SchedulingV1alpha2().Workloads(namespace).Create(ctx, workload, metav1.CreateOptions{})
if err != nil {
return fmt.Errorf("failed to create Workload: %w", err)
}
logger.Info("Created Workload", "name", workloadName, "namespace", namespace)
return nil
}

// Workload exists - it's immutable, so we don't update it
logger.V(1).Info("Workload already exists", "name", workloadName, "namespace", namespace)
return nil
}

// syncPodGroup creates or updates the PodGroup object for the SparkApplication.
func (s *Scheduler) syncPodGroup(ctx context.Context, app *v1beta2.SparkApplication) error {
podGroupName := getPodGroupName(app)
workloadName := getWorkloadName(app)
namespace := app.Namespace

// Check if PodGroup already exists
existing, err := s.kubeClient.SchedulingV1alpha2().PodGroups(namespace).Get(ctx, podGroupName, metav1.GetOptions{})
if err != nil {
if !errors.IsNotFound(err) {
return err
}

// Create new PodGroup
podGroup := buildPodGroup(app, workloadName)
_, err = s.kubeClient.SchedulingV1alpha2().PodGroups(namespace).Create(ctx, podGroup, metav1.CreateOptions{})
if err != nil {
return fmt.Errorf("failed to create PodGroup: %w", err)
}
logger.Info("Created PodGroup", "name", podGroupName, "namespace", namespace)
return nil
}

// PodGroup exists - most fields are immutable, but we might need to update in the future
_ = existing
logger.V(1).Info("PodGroup already exists", "name", podGroupName, "namespace", namespace)
return nil
}

// buildWorkload constructs a Workload object from a SparkApplication.
func buildWorkload(app *v1beta2.SparkApplication) *schedulingv1alpha2.Workload {
workloadName := getWorkloadName(app)

// Build the PodGroupTemplate with gang scheduling policy
minCount := calculateMinCount(app)
podGroupTemplate := schedulingv1alpha2.PodGroupTemplate{
Name: SparkGangPodGroupTemplateName,
SchedulingPolicy: schedulingv1alpha2.PodGroupSchedulingPolicy{
// Use gang scheduling with minCount
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
// ControllerRef points back to the SparkApplication
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
// ACTUAL K8s v0.36.0 API structure - nested union type
PodGroupTemplateRef: &schedulingv1alpha2.PodGroupTemplateReference{
Workload: &schedulingv1alpha2.WorkloadPodGroupTemplateReference{
WorkloadName:         workloadName,
PodGroupTemplateName: SparkGangPodGroupTemplateName, // NOT "TemplateName" - actual field name
},
},
SchedulingPolicy: schedulingv1alpha2.PodGroupSchedulingPolicy{
Gang: &schedulingv1alpha2.GangSchedulingPolicy{
MinCount: minCount,
},
},
// NOTE: MinResources field does NOT exist in actual K8s v0.36.0 API
// The design doc assumed it, but it's not present in scheduling/v1alpha2
},
}

// Add priority if specified
if app.Spec.BatchSchedulerOptions != nil && app.Spec.BatchSchedulerOptions.PriorityClassName != nil {
podGroup.Spec.PriorityClassName = *app.Spec.BatchSchedulerOptions.PriorityClassName
}

return podGroup
}

// calculateMinCount determines the minimum number of pods for gang scheduling.
func calculateMinCount(app *v1beta2.SparkApplication) int32 {
switch app.Spec.Mode {
case v1beta2.DeployModeClient:
// Client mode: driver runs outside cluster, only executors need gang scheduling
// MinCount = 1 (to allow at least one executor to start)
return 1

case v1beta2.DeployModeCluster:
// Cluster mode: driver + executors need gang scheduling
// MinCount = 1 (driver) + at least 1 executor
// We use 1 initially to allow the driver to start first
return 1

default:
return 1
}
}

