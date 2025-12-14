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
	"reflect"

	"k8s.io/apimachinery/pkg/api/equality"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

// UpdateScope represents the scope of changes detected in a SparkApplication update.
type UpdateScope string

const (
	// UpdateScopeNone indicates no changes were detected.
	UpdateScopeNone UpdateScope = "None"
	// UpdateScopeFull indicates changes that require a full application restart.
	UpdateScopeFull UpdateScope = "Full"
	// UpdateScopeExecutorDynamic indicates changes that can be handled via dynamic allocation.
	UpdateScopeExecutorDynamic UpdateScope = "ExecutorDynamic"
	// UpdateScopeHot indicates changes that can be applied without any pod restarts.
	UpdateScopeHot UpdateScope = "Hot"
)

// ChangeDetectionResult contains the result of change detection.
type ChangeDetectionResult struct {
	// Scope is the detected update scope.
	Scope UpdateScope
	// DriverChanges indicates if driver-related changes were detected.
	DriverChanges bool
	// ExecutorChanges contains details about executor changes.
	ExecutorChanges *ExecutorChanges
	// ServiceChanges contains details about service configuration changes.
	ServiceChanges *ServiceChanges
	// MetadataChanges indicates if only metadata changes were detected.
	MetadataChanges bool
}

// ExecutorChanges contains details about executor-related changes.
type ExecutorChanges struct {
	// InstancesChanged indicates if the number of executor instances changed.
	InstancesChanged bool
	// OldInstances is the previous number of instances.
	OldInstances int32
	// NewInstances is the new number of instances.
	NewInstances int32
	// ResourcesChanged indicates if executor resources changed.
	ResourcesChanged bool
	// ConfigChanged indicates if executor configuration changed.
	ConfigChanged bool
}

// ServiceChanges contains details about service-related changes.
type ServiceChanges struct {
	// DriverServiceAnnotationsChanged indicates if driver service annotations changed.
	DriverServiceAnnotationsChanged bool
	// DriverServiceLabelsChanged indicates if driver service labels changed.
	DriverServiceLabelsChanged bool
	// SparkUIOptionsChanged indicates if Spark UI options changed.
	SparkUIOptionsChanged bool
	// DriverIngressOptionsChanged indicates if driver ingress options changed.
	DriverIngressOptionsChanged bool
	// MonitoringChanged indicates if monitoring configuration changed.
	MonitoringChanged bool
}

// ChangeDetector detects and categorizes changes between SparkApplication specs.
type ChangeDetector struct{}

// NewChangeDetector creates a new ChangeDetector.
func NewChangeDetector() *ChangeDetector {
	return &ChangeDetector{}
}

// DetectChanges compares old and new SparkApplication specs and returns the change detection result.
func (d *ChangeDetector) DetectChanges(oldApp, newApp *v1beta2.SparkApplication) *ChangeDetectionResult {
	result := &ChangeDetectionResult{
		Scope:           UpdateScopeNone,
		ExecutorChanges: &ExecutorChanges{},
		ServiceChanges:  &ServiceChanges{},
	}

	// If specs are equal, no changes
	if equality.Semantic.DeepEqual(oldApp.Spec, newApp.Spec) {
		return result
	}

	// Check for driver-related changes (require full restart)
	if d.hasDriverChanges(oldApp, newApp) {
		result.DriverChanges = true
		result.Scope = UpdateScopeFull
		return result
	}

	// Check for core infrastructure changes (require full restart)
	if d.hasCoreInfrastructureChanges(oldApp, newApp) {
		result.Scope = UpdateScopeFull
		return result
	}

	// Check for global configuration changes (require full restart)
	if d.hasGlobalConfigChanges(oldApp, newApp) {
		result.Scope = UpdateScopeFull
		return result
	}

	// Check for executor changes that can be handled dynamically
	if d.hasExecutorDynamicChanges(oldApp, newApp, result.ExecutorChanges) {
		result.Scope = UpdateScopeExecutorDynamic
	}

	// Check for hot-updatable changes
	if d.hasServiceChanges(oldApp, newApp, result.ServiceChanges) {
		if result.Scope == UpdateScopeNone {
			result.Scope = UpdateScopeHot
		}
	}

	// Check for metadata-only changes
	if d.hasMetadataOnlyChanges(oldApp, newApp) {
		result.MetadataChanges = true
		if result.Scope == UpdateScopeNone {
			result.Scope = UpdateScopeHot
		}
	}

	// If we detected changes but couldn't categorize them, default to full restart
	if result.Scope == UpdateScopeNone && !equality.Semantic.DeepEqual(oldApp.Spec, newApp.Spec) {
		result.Scope = UpdateScopeFull
	}

	return result
}

// hasDriverChanges checks if driver-related configuration has changed.
func (d *ChangeDetector) hasDriverChanges(oldApp, newApp *v1beta2.SparkApplication) bool {
	oldDriver := &oldApp.Spec.Driver
	newDriver := &newApp.Spec.Driver

	// Check driver pod spec changes that require restart
	if !equality.Semantic.DeepEqual(oldDriver.Cores, newDriver.Cores) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.CoreLimit, newDriver.CoreLimit) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.CoreRequest, newDriver.CoreRequest) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.Memory, newDriver.Memory) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.MemoryLimit, newDriver.MemoryLimit) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.MemoryOverhead, newDriver.MemoryOverhead) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.PriorityClassName, newDriver.PriorityClassName) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.JavaOptions, newDriver.JavaOptions) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.Image, newDriver.Image) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.Env, newDriver.Env) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.EnvVars, newDriver.EnvVars) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.EnvFrom, newDriver.EnvFrom) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.VolumeMounts, newDriver.VolumeMounts) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.Affinity, newDriver.Affinity) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.Tolerations, newDriver.Tolerations) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.NodeSelector, newDriver.NodeSelector) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.SecurityContext, newDriver.SecurityContext) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.PodSecurityContext, newDriver.PodSecurityContext) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.SchedulerName, newDriver.SchedulerName) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.Sidecars, newDriver.Sidecars) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.InitContainers, newDriver.InitContainers) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.HostNetwork, newDriver.HostNetwork) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.DNSConfig, newDriver.DNSConfig) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.KubernetesMaster, newDriver.KubernetesMaster) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.Template, newDriver.Template) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.GPU, newDriver.GPU) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.Lifecycle, newDriver.Lifecycle) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.Ports, newDriver.Ports) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.ConfigMaps, newDriver.ConfigMaps) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.Secrets, newDriver.Secrets) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldDriver.ServiceAccount, newDriver.ServiceAccount) {
		return true
	}

	return false
}

// hasCoreInfrastructureChanges checks if core infrastructure configuration has changed.
func (d *ChangeDetector) hasCoreInfrastructureChanges(oldApp, newApp *v1beta2.SparkApplication) bool {
	// Spark version change
	if oldApp.Spec.SparkVersion != newApp.Spec.SparkVersion {
		return true
	}
	// Deploy mode change
	if oldApp.Spec.Mode != newApp.Spec.Mode {
		return true
	}
	// Application type change
	if oldApp.Spec.Type != newApp.Spec.Type {
		return true
	}
	// Main application file change
	if !equality.Semantic.DeepEqual(oldApp.Spec.MainApplicationFile, newApp.Spec.MainApplicationFile) {
		return true
	}
	// Main class change
	if !equality.Semantic.DeepEqual(oldApp.Spec.MainClass, newApp.Spec.MainClass) {
		return true
	}
	// Arguments change
	if !equality.Semantic.DeepEqual(oldApp.Spec.Arguments, newApp.Spec.Arguments) {
		return true
	}
	// Dependencies change
	if !equality.Semantic.DeepEqual(oldApp.Spec.Deps, newApp.Spec.Deps) {
		return true
	}
	// Volumes change
	if !equality.Semantic.DeepEqual(oldApp.Spec.Volumes, newApp.Spec.Volumes) {
		return true
	}
	// Image change (if affects both driver and executor)
	if !equality.Semantic.DeepEqual(oldApp.Spec.Image, newApp.Spec.Image) {
		return true
	}
	// Image pull policy change
	if !equality.Semantic.DeepEqual(oldApp.Spec.ImagePullPolicy, newApp.Spec.ImagePullPolicy) {
		return true
	}
	// Image pull secrets change
	if !equality.Semantic.DeepEqual(oldApp.Spec.ImagePullSecrets, newApp.Spec.ImagePullSecrets) {
		return true
	}
	// Python version change
	if !equality.Semantic.DeepEqual(oldApp.Spec.PythonVersion, newApp.Spec.PythonVersion) {
		return true
	}
	// Memory overhead factor change
	if !equality.Semantic.DeepEqual(oldApp.Spec.MemoryOverheadFactor, newApp.Spec.MemoryOverheadFactor) {
		return true
	}
	// Proxy user change
	if !equality.Semantic.DeepEqual(oldApp.Spec.ProxyUser, newApp.Spec.ProxyUser) {
		return true
	}
	// Restart policy change
	if !equality.Semantic.DeepEqual(oldApp.Spec.RestartPolicy, newApp.Spec.RestartPolicy) {
		return true
	}
	// Node selector change (at spec level)
	if !equality.Semantic.DeepEqual(oldApp.Spec.NodeSelector, newApp.Spec.NodeSelector) {
		return true
	}
	// Batch scheduler change
	if !equality.Semantic.DeepEqual(oldApp.Spec.BatchScheduler, newApp.Spec.BatchScheduler) {
		return true
	}
	// Batch scheduler options change
	if !equality.Semantic.DeepEqual(oldApp.Spec.BatchSchedulerOptions, newApp.Spec.BatchSchedulerOptions) {
		return true
	}
	// Spark config map change
	if !equality.Semantic.DeepEqual(oldApp.Spec.SparkConfigMap, newApp.Spec.SparkConfigMap) {
		return true
	}
	// Hadoop config map change
	if !equality.Semantic.DeepEqual(oldApp.Spec.HadoopConfigMap, newApp.Spec.HadoopConfigMap) {
		return true
	}

	return false
}

// hasGlobalConfigChanges checks if global Spark/Hadoop configuration has changed.
func (d *ChangeDetector) hasGlobalConfigChanges(oldApp, newApp *v1beta2.SparkApplication) bool {
	// SparkConf changes typically require full restart
	if !equality.Semantic.DeepEqual(oldApp.Spec.SparkConf, newApp.Spec.SparkConf) {
		// Check if only dynamic allocation related configs changed
		if d.onlyDynamicAllocationSparkConfChanged(oldApp.Spec.SparkConf, newApp.Spec.SparkConf) {
			return false
		}
		return true
	}
	// HadoopConf changes require full restart
	if !equality.Semantic.DeepEqual(oldApp.Spec.HadoopConf, newApp.Spec.HadoopConf) {
		return true
	}

	return false
}

// onlyDynamicAllocationSparkConfChanged checks if only dynamic allocation related spark configs changed.
func (d *ChangeDetector) onlyDynamicAllocationSparkConfChanged(oldConf, newConf map[string]string) bool {
	dynamicAllocationKeys := map[string]bool{
		"spark.dynamicAllocation.enabled":                 true,
		"spark.dynamicAllocation.initialExecutors":        true,
		"spark.dynamicAllocation.minExecutors":            true,
		"spark.dynamicAllocation.maxExecutors":            true,
		"spark.dynamicAllocation.shuffleTracking.enabled": true,
		"spark.dynamicAllocation.shuffleTracking.timeout": true,
		"spark.dynamicAllocation.executorIdleTimeout":     true,
		"spark.dynamicAllocation.schedulerBacklogTimeout": true,
	}

	// Create copies without dynamic allocation keys
	oldFiltered := make(map[string]string)
	newFiltered := make(map[string]string)

	for k, v := range oldConf {
		if !dynamicAllocationKeys[k] {
			oldFiltered[k] = v
		}
	}
	for k, v := range newConf {
		if !dynamicAllocationKeys[k] {
			newFiltered[k] = v
		}
	}

	return equality.Semantic.DeepEqual(oldFiltered, newFiltered)
}

// hasExecutorDynamicChanges checks if executor changes can be handled via dynamic allocation.
func (d *ChangeDetector) hasExecutorDynamicChanges(oldApp, newApp *v1beta2.SparkApplication, changes *ExecutorChanges) bool {
	oldExecutor := &oldApp.Spec.Executor
	newExecutor := &newApp.Spec.Executor

	hasChanges := false

	// Check instance count changes
	oldInstances := int32(1)
	newInstances := int32(1)
	if oldExecutor.Instances != nil {
		oldInstances = *oldExecutor.Instances
	}
	if newExecutor.Instances != nil {
		newInstances = *newExecutor.Instances
	}
	if oldInstances != newInstances {
		changes.InstancesChanged = true
		changes.OldInstances = oldInstances
		changes.NewInstances = newInstances
		hasChanges = true
	}

	// Check resource changes (cores, memory, etc.)
	if !equality.Semantic.DeepEqual(oldExecutor.Cores, newExecutor.Cores) ||
		!equality.Semantic.DeepEqual(oldExecutor.CoreLimit, newExecutor.CoreLimit) ||
		!equality.Semantic.DeepEqual(oldExecutor.CoreRequest, newExecutor.CoreRequest) ||
		!equality.Semantic.DeepEqual(oldExecutor.Memory, newExecutor.Memory) ||
		!equality.Semantic.DeepEqual(oldExecutor.MemoryLimit, newExecutor.MemoryLimit) ||
		!equality.Semantic.DeepEqual(oldExecutor.MemoryOverhead, newExecutor.MemoryOverhead) {
		changes.ResourcesChanged = true
		hasChanges = true
	}

	// Check configuration changes
	if !equality.Semantic.DeepEqual(oldExecutor.PriorityClassName, newExecutor.PriorityClassName) ||
		!equality.Semantic.DeepEqual(oldExecutor.JavaOptions, newExecutor.JavaOptions) ||
		!equality.Semantic.DeepEqual(oldExecutor.DeleteOnTermination, newExecutor.DeleteOnTermination) ||
		!equality.Semantic.DeepEqual(oldExecutor.NodeSelector, newExecutor.NodeSelector) {
		changes.ConfigChanged = true
		hasChanges = true
	}

	// Check dynamic allocation changes
	if !equality.Semantic.DeepEqual(oldApp.Spec.DynamicAllocation, newApp.Spec.DynamicAllocation) {
		hasChanges = true
	}

	// If there are other executor changes not covered above, they require full restart
	if d.hasOtherExecutorChanges(oldExecutor, newExecutor) {
		return false
	}

	return hasChanges
}

// hasOtherExecutorChanges checks for executor changes that require full restart.
func (d *ChangeDetector) hasOtherExecutorChanges(oldExecutor, newExecutor *v1beta2.ExecutorSpec) bool {
	// Check changes that require full restart
	if !equality.Semantic.DeepEqual(oldExecutor.Image, newExecutor.Image) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldExecutor.Env, newExecutor.Env) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldExecutor.EnvVars, newExecutor.EnvVars) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldExecutor.EnvFrom, newExecutor.EnvFrom) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldExecutor.VolumeMounts, newExecutor.VolumeMounts) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldExecutor.Affinity, newExecutor.Affinity) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldExecutor.Tolerations, newExecutor.Tolerations) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldExecutor.SecurityContext, newExecutor.SecurityContext) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldExecutor.PodSecurityContext, newExecutor.PodSecurityContext) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldExecutor.SchedulerName, newExecutor.SchedulerName) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldExecutor.Sidecars, newExecutor.Sidecars) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldExecutor.InitContainers, newExecutor.InitContainers) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldExecutor.HostNetwork, newExecutor.HostNetwork) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldExecutor.DNSConfig, newExecutor.DNSConfig) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldExecutor.Template, newExecutor.Template) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldExecutor.GPU, newExecutor.GPU) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldExecutor.Lifecycle, newExecutor.Lifecycle) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldExecutor.Ports, newExecutor.Ports) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldExecutor.ConfigMaps, newExecutor.ConfigMaps) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldExecutor.Secrets, newExecutor.Secrets) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldExecutor.ServiceAccount, newExecutor.ServiceAccount) {
		return true
	}

	return false
}

// hasServiceChanges checks if service-related configuration has changed.
func (d *ChangeDetector) hasServiceChanges(oldApp, newApp *v1beta2.SparkApplication, changes *ServiceChanges) bool {
	hasChanges := false

	// Check driver service annotations
	if !equality.Semantic.DeepEqual(oldApp.Spec.Driver.ServiceAnnotations, newApp.Spec.Driver.ServiceAnnotations) {
		changes.DriverServiceAnnotationsChanged = true
		hasChanges = true
	}

	// Check driver service labels
	if !equality.Semantic.DeepEqual(oldApp.Spec.Driver.ServiceLabels, newApp.Spec.Driver.ServiceLabels) {
		changes.DriverServiceLabelsChanged = true
		hasChanges = true
	}

	// Check Spark UI options
	if !equality.Semantic.DeepEqual(oldApp.Spec.SparkUIOptions, newApp.Spec.SparkUIOptions) {
		changes.SparkUIOptionsChanged = true
		hasChanges = true
	}

	// Check driver ingress options
	if !equality.Semantic.DeepEqual(oldApp.Spec.DriverIngressOptions, newApp.Spec.DriverIngressOptions) {
		changes.DriverIngressOptionsChanged = true
		hasChanges = true
	}

	// Check monitoring configuration
	if !equality.Semantic.DeepEqual(oldApp.Spec.Monitoring, newApp.Spec.Monitoring) {
		changes.MonitoringChanged = true
		hasChanges = true
	}

	return hasChanges
}

// hasMetadataOnlyChanges checks if only metadata (labels, annotations) changed.
func (d *ChangeDetector) hasMetadataOnlyChanges(oldApp, newApp *v1beta2.SparkApplication) bool {
	// Check labels on driver/executor pods (not the SparkApplication resource itself)
	oldDriverLabels := oldApp.Spec.Driver.Labels
	newDriverLabels := newApp.Spec.Driver.Labels
	oldDriverAnnotations := oldApp.Spec.Driver.Annotations
	newDriverAnnotations := newApp.Spec.Driver.Annotations
	oldExecutorLabels := oldApp.Spec.Executor.Labels
	newExecutorLabels := newApp.Spec.Executor.Labels
	oldExecutorAnnotations := oldApp.Spec.Executor.Annotations
	newExecutorAnnotations := newApp.Spec.Executor.Annotations

	labelsChanged := !reflect.DeepEqual(oldDriverLabels, newDriverLabels) ||
		!reflect.DeepEqual(oldExecutorLabels, newExecutorLabels)
	annotationsChanged := !reflect.DeepEqual(oldDriverAnnotations, newDriverAnnotations) ||
		!reflect.DeepEqual(oldExecutorAnnotations, newExecutorAnnotations)

	return labelsChanged || annotationsChanged
}

// IsDynamicAllocationEnabled checks if dynamic allocation is enabled for the application.
func IsDynamicAllocationEnabled(app *v1beta2.SparkApplication) bool {
	if app.Spec.DynamicAllocation != nil && app.Spec.DynamicAllocation.Enabled {
		return true
	}
	// Also check sparkConf for dynamic allocation
	if app.Spec.SparkConf != nil {
		if enabled, ok := app.Spec.SparkConf["spark.dynamicAllocation.enabled"]; ok && enabled == "true" {
			return true
		}
	}
	return false
}
