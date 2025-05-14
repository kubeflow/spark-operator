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

package util

import (
	"crypto/md5"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/common"
)

// GetDriverPodName returns name of the driver pod of the given spark application.
func GetDriverPodName(app *v1beta2.SparkApplication) string {
	name := app.Spec.Driver.PodName
	if name != nil && len(*name) > 0 {
		return *name
	}

	sparkConf := app.Spec.SparkConf
	if sparkConf[common.SparkKubernetesDriverPodName] != "" {
		return sparkConf[common.SparkKubernetesDriverPodName]
	}

	return fmt.Sprintf("%s-driver", app.Name)
}

// GetApplicationState returns the state of the given SparkApplication.
func GetApplicationState(app *v1beta2.SparkApplication) v1beta2.ApplicationStateType {
	return app.Status.AppState.State
}

// IsTerminated returns whether the given SparkApplication is terminated.
func IsTerminated(app *v1beta2.SparkApplication) bool {
	return app.Status.AppState.State == v1beta2.ApplicationStateCompleted ||
		app.Status.AppState.State == v1beta2.ApplicationStateFailed
}

// IsExpired returns whether the given SparkApplication is expired.
func IsExpired(app *v1beta2.SparkApplication) bool {
	// The application has no TTL defined and will never expire.
	if app.Spec.TimeToLiveSeconds == nil {
		return false
	}

	ttl := time.Duration(*app.Spec.TimeToLiveSeconds) * time.Second
	now := time.Now()
	if !app.Status.TerminationTime.IsZero() && now.Sub(app.Status.TerminationTime.Time) > ttl {
		return true
	}

	return false
}

// IsDriverRunning returns whether the driver pod of the given SparkApplication is running.
func IsDriverRunning(app *v1beta2.SparkApplication) bool {
	return app.Status.AppState.State == v1beta2.ApplicationStateRunning
}

func ShouldRetry(app *v1beta2.SparkApplication) bool {
	switch app.Status.AppState.State {
	case v1beta2.ApplicationStateSucceeding:
		return app.Spec.RestartPolicy.Type == v1beta2.RestartPolicyAlways
	case v1beta2.ApplicationStateFailing:
		if app.Spec.RestartPolicy.Type == v1beta2.RestartPolicyAlways {
			return true
		} else if app.Spec.RestartPolicy.Type == v1beta2.RestartPolicyOnFailure {
			// We retry if we haven't hit the retry limit.
			if app.Spec.RestartPolicy.OnFailureRetries != nil && app.Status.ExecutionAttempts <= *app.Spec.RestartPolicy.OnFailureRetries {
				return true
			}
		}
	case v1beta2.ApplicationStateFailedSubmission:
		if app.Spec.RestartPolicy.Type == v1beta2.RestartPolicyAlways {
			return true
		} else if app.Spec.RestartPolicy.Type == v1beta2.RestartPolicyOnFailure {
			// We retry if we haven't hit the retry limit.
			if app.Spec.RestartPolicy.OnSubmissionFailureRetries != nil && app.Status.SubmissionAttempts <= *app.Spec.RestartPolicy.OnSubmissionFailureRetries {
				return true
			}
		}
	}
	return false
}

func TimeUntilNextRetryDue(app *v1beta2.SparkApplication) (time.Duration, error) {
	var retryInterval *int64
	switch app.Status.AppState.State {
	case v1beta2.ApplicationStateFailedSubmission:
		retryInterval = app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval
	case v1beta2.ApplicationStateFailing:
		retryInterval = app.Spec.RestartPolicy.OnFailureRetryInterval
	}

	attemptsDone := app.Status.SubmissionAttempts
	lastAttemptTime := app.Status.LastSubmissionAttemptTime
	if retryInterval == nil || lastAttemptTime.IsZero() || attemptsDone <= 0 {
		return -1, fmt.Errorf("invalid retry interval (%v), last attempt time (%v) or attemptsDone (%v)", retryInterval, lastAttemptTime, attemptsDone)
	}

	// Retry wait time is attempts*RetryInterval to do a linear backoff.
	interval := time.Duration(*retryInterval) * time.Second * time.Duration(attemptsDone)
	currentTime := time.Now()
	return interval - currentTime.Sub(lastAttemptTime.Time), nil
}

func GetLocalVolumes(app *v1beta2.SparkApplication) map[string]corev1.Volume {
	volumes := make(map[string]corev1.Volume)
	for _, volume := range app.Spec.Volumes {
		if strings.HasPrefix(volume.Name, common.SparkLocalDirVolumePrefix) {
			volumes[volume.Name] = volume
		}
	}
	return volumes
}

func GetDriverLocalVolumeMounts(app *v1beta2.SparkApplication) []corev1.VolumeMount {
	volumeMounts := []corev1.VolumeMount{}
	for _, volumeMount := range app.Spec.Driver.VolumeMounts {
		if strings.HasPrefix(volumeMount.Name, common.SparkLocalDirVolumePrefix) {
			volumeMounts = append(volumeMounts, volumeMount)
		}
	}
	return volumeMounts
}

func GetExecutorLocalVolumeMounts(app *v1beta2.SparkApplication) []corev1.VolumeMount {
	volumeMounts := []corev1.VolumeMount{}
	for _, volumeMount := range app.Spec.Executor.VolumeMounts {
		if strings.HasPrefix(volumeMount.Name, common.SparkLocalDirVolumePrefix) {
			volumeMounts = append(volumeMounts, volumeMount)
		}
	}
	return volumeMounts
}

func generateName(name, suffix string) string {
	// Some resource names are used as DNS labels, so must be 63 characters or shorter
	preferredName := fmt.Sprintf("%s-%s", name, suffix)
	if len(preferredName) <= 63 {
		return preferredName
	}

	// Truncate the name and append a hash to ensure uniqueness while staying below the limit
	maxNameLength := 63 - len(suffix) - 10 // 8 for the hash, 2 for the dash
	hash := fmt.Sprintf("%x", md5.Sum([]byte(preferredName)))
	return fmt.Sprintf("%s-%s-%s", name[:maxNameLength], hash[:8], suffix)
}

func GetDefaultUIServiceName(app *v1beta2.SparkApplication) string {
	return generateName(app.Name, "ui-svc")
}

func GetDefaultUIIngressName(app *v1beta2.SparkApplication) string {
	return generateName(app.Name, "ui-ingress")
}

func GetResourceLabels(app *v1beta2.SparkApplication) map[string]string {
	labels := map[string]string{
		common.LabelSparkAppName: app.Name,
	}
	if app.Status.SubmissionID != "" {
		labels[common.LabelSubmissionID] = app.Status.SubmissionID
	}
	return labels
}

func GetWebUIServiceLabels(app *v1beta2.SparkApplication) map[string]string {
	labels := map[string]string{}
	if app.Spec.SparkUIOptions != nil && app.Spec.SparkUIOptions.ServiceLabels != nil {
		for key, value := range app.Spec.SparkUIOptions.ServiceLabels {
			labels[key] = value
		}
	}
	return labels
}

func GetWebUIServiceAnnotations(app *v1beta2.SparkApplication) map[string]string {
	serviceAnnotations := map[string]string{}
	if app.Spec.SparkUIOptions != nil && app.Spec.SparkUIOptions.ServiceAnnotations != nil {
		for key, value := range app.Spec.SparkUIOptions.ServiceAnnotations {
			serviceAnnotations[key] = value
		}
	}
	return serviceAnnotations
}

func GetWebUIServiceType(app *v1beta2.SparkApplication) corev1.ServiceType {
	if app.Spec.SparkUIOptions != nil && app.Spec.SparkUIOptions.ServiceType != nil {
		return *app.Spec.SparkUIOptions.ServiceType
	}
	return corev1.ServiceTypeClusterIP
}

func GetWebUIIngressAnnotations(app *v1beta2.SparkApplication) map[string]string {
	annotations := map[string]string{}
	if app.Spec.SparkUIOptions != nil && app.Spec.SparkUIOptions.IngressAnnotations != nil {
		for key, value := range app.Spec.SparkUIOptions.IngressAnnotations {
			annotations[key] = value
		}
	}
	return annotations
}

func GetWebUIIngressTLS(app *v1beta2.SparkApplication) []networkingv1.IngressTLS {
	ingressTLSs := []networkingv1.IngressTLS{}
	if app.Spec.SparkUIOptions != nil && app.Spec.SparkUIOptions.IngressTLS != nil {
		ingressTLSs = append(ingressTLSs, app.Spec.SparkUIOptions.IngressTLS...)
	}
	return ingressTLSs
}

// GetPrometheusConfigMapName returns the name of the ConfigMap for Prometheus configuration.
func GetPrometheusConfigMapName(app *v1beta2.SparkApplication) string {
	return fmt.Sprintf("%s-%s", app.Name, common.PrometheusConfigMapNameSuffix)
}

// PrometheusMonitoringEnabled returns if Prometheus monitoring is enabled or not.
func PrometheusMonitoringEnabled(app *v1beta2.SparkApplication) bool {
	return app.Spec.Monitoring != nil && app.Spec.Monitoring.Prometheus != nil
}

// HasPrometheusConfigFile returns if Prometheus monitoring uses a configuration file in the container.
func HasPrometheusConfigFile(app *v1beta2.SparkApplication) bool {
	return PrometheusMonitoringEnabled(app) &&
		app.Spec.Monitoring.Prometheus.ConfigFile != nil &&
		*app.Spec.Monitoring.Prometheus.ConfigFile != ""
}

// HasPrometheusConfig returns if Prometheus monitoring defines metricsProperties in the spec.
func HasMetricsProperties(app *v1beta2.SparkApplication) bool {
	return PrometheusMonitoringEnabled(app) &&
		app.Spec.Monitoring.MetricsProperties != nil &&
		*app.Spec.Monitoring.MetricsProperties != ""
}

// HasPrometheusConfigFile returns if Monitoring defines metricsPropertiesFile in the spec.
func HasMetricsPropertiesFile(app *v1beta2.SparkApplication) bool {
	return PrometheusMonitoringEnabled(app) &&
		app.Spec.Monitoring.MetricsPropertiesFile != nil &&
		*app.Spec.Monitoring.MetricsPropertiesFile != ""
}

// ExposeDriverMetrics returns if driver metrics should be exposed.
func ExposeDriverMetrics(app *v1beta2.SparkApplication) bool {
	return app.Spec.Monitoring != nil && app.Spec.Monitoring.ExposeDriverMetrics
}

// ExposeExecutorMetrics returns if executor metrics should be exposed.
func ExposeExecutorMetrics(app *v1beta2.SparkApplication) bool {
	return app.Spec.Monitoring != nil && app.Spec.Monitoring.ExposeExecutorMetrics
}

// GetOwnerReference returns an OwnerReference pointing to the given app.
func GetOwnerReference(app *v1beta2.SparkApplication) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion:         v1beta2.SchemeGroupVersion.String(),
		Kind:               reflect.TypeOf(v1beta2.SparkApplication{}).Name(),
		Name:               app.Name,
		UID:                app.UID,
		Controller:         BoolPtr(true),
		BlockOwnerDeletion: BoolPtr(true),
	}
}

// GetDriverState returns the driver state from the given driver pod.
func GetDriverState(pod *corev1.Pod) v1beta2.DriverState {
	switch pod.Status.Phase {
	case corev1.PodPending:
		return v1beta2.DriverStatePending
	case corev1.PodRunning:
		state := GetDriverContainerTerminatedState(pod)
		if state != nil {
			if state.ExitCode == 0 {
				return v1beta2.DriverStateCompleted
			}
			return v1beta2.DriverStateFailed
		}
		return v1beta2.DriverStateRunning
	case corev1.PodSucceeded:
		return v1beta2.DriverStateCompleted
	case corev1.PodFailed:
		state := GetDriverContainerTerminatedState(pod)
		if state != nil && state.ExitCode == 0 {
			return v1beta2.DriverStateCompleted
		}
		return v1beta2.DriverStateFailed
	default:
		return v1beta2.DriverStateUnknown
	}
}

// GetExecutorState returns the executor state from the given executor pod.
func GetExecutorState(pod *corev1.Pod) v1beta2.ExecutorState {
	switch pod.Status.Phase {
	case corev1.PodPending:
		return v1beta2.ExecutorStatePending
	case corev1.PodRunning:
		return v1beta2.ExecutorStateRunning
	case corev1.PodSucceeded:
		return v1beta2.ExecutorStateCompleted
	case corev1.PodFailed:
		return v1beta2.ExecutorStateFailed
	default:
		return v1beta2.ExecutorStateUnknown
	}
}

// GetDriverContainerTerminatedState returns the terminated state of the driver container.
func GetDriverContainerTerminatedState(pod *corev1.Pod) *corev1.ContainerStateTerminated {
	return GetContainerTerminatedState(pod, common.SparkDriverContainerName)
}

// GetExecutorContainerTerminatedState returns the terminated state of the executor container.
func GetExecutorContainerTerminatedState(pod *corev1.Pod) *corev1.ContainerStateTerminated {
	state := GetContainerTerminatedState(pod, common.Spark3DefaultExecutorContainerName)
	if state == nil {
		state = GetContainerTerminatedState(pod, common.SparkExecutorContainerName)
	}
	return state
}

// GetContainerTerminatedState returns the terminated state of the container.
func GetContainerTerminatedState(pod *corev1.Pod, container string) *corev1.ContainerStateTerminated {
	for _, c := range pod.Status.ContainerStatuses {
		if c.Name == container {
			if c.State.Terminated != nil {
				return c.State.Terminated
			}
			return nil
		}
	}
	return nil
}

// IsDriverTerminated returns whether the driver state is a terminated state.
func IsDriverTerminated(driverState v1beta2.DriverState) bool {
	return driverState == v1beta2.DriverStateCompleted || driverState == v1beta2.DriverStateFailed
}

// IsExecutorTerminated returns whether the executor state is a terminated state.
func IsExecutorTerminated(executorState v1beta2.ExecutorState) bool {
	return executorState == v1beta2.ExecutorStateCompleted || executorState == v1beta2.ExecutorStateFailed
}

// DriverStateToApplicationState converts driver state to application state.
func DriverStateToApplicationState(driverState v1beta2.DriverState) v1beta2.ApplicationStateType {
	switch driverState {
	case v1beta2.DriverStatePending:
		return v1beta2.ApplicationStateSubmitted
	case v1beta2.DriverStateRunning:
		return v1beta2.ApplicationStateRunning
	case v1beta2.DriverStateCompleted:
		return v1beta2.ApplicationStateSucceeding
	case v1beta2.DriverStateFailed:
		return v1beta2.ApplicationStateFailing
	default:
		return v1beta2.ApplicationStateUnknown
	}
}

// GetDriverRequestResource returns the driver request resource list.
func GetDriverRequestResource(app *v1beta2.SparkApplication) corev1.ResourceList {
	minResource := corev1.ResourceList{}

	// Cores correspond to driver's core request
	if app.Spec.Driver.Cores != nil {
		if value, err := resource.ParseQuantity(fmt.Sprintf("%d", *app.Spec.Driver.Cores)); err == nil {
			minResource[corev1.ResourceCPU] = value
		}
	}

	// CoreLimit correspond to driver's core limit, this attribute will be used only when core request is empty.
	if app.Spec.Driver.CoreLimit != nil {
		if _, ok := minResource[corev1.ResourceCPU]; !ok {
			if value, err := resource.ParseQuantity(*app.Spec.Driver.CoreLimit); err == nil {
				minResource[corev1.ResourceCPU] = value
			}
		}
	}

	// Memory + MemoryOverhead correspond to driver's memory request
	if app.Spec.Driver.Memory != nil {
		if value, err := resource.ParseQuantity(*app.Spec.Driver.Memory); err == nil {
			minResource[corev1.ResourceMemory] = value
		}
	}
	if app.Spec.Driver.MemoryOverhead != nil {
		if value, err := resource.ParseQuantity(*app.Spec.Driver.MemoryOverhead); err == nil {
			if existing, ok := minResource[corev1.ResourceMemory]; ok {
				existing.Add(value)
				minResource[corev1.ResourceMemory] = existing
			}
		}
	}

	return minResource
}

// GetExecutorRequestResource returns the executor request resource list.
func GetExecutorRequestResource(app *v1beta2.SparkApplication) corev1.ResourceList {
	minResource := corev1.ResourceList{}

	// CoreRequest correspond to executor's core request
	if app.Spec.Executor.CoreRequest != nil {
		if value, err := resource.ParseQuantity(*app.Spec.Executor.CoreRequest); err == nil {
			minResource[corev1.ResourceCPU] = value
		}
	}

	// Use Core attribute if CoreRequest is empty
	if app.Spec.Executor.Cores != nil {
		if _, ok := minResource[corev1.ResourceCPU]; !ok {
			if value, err := resource.ParseQuantity(fmt.Sprintf("%d", *app.Spec.Executor.Cores)); err == nil {
				minResource[corev1.ResourceCPU] = value
			}
		}
	}

	// CoreLimit correspond to executor's core limit, this attribute will be used only when core request is empty.
	if app.Spec.Executor.CoreLimit != nil {
		if _, ok := minResource[corev1.ResourceCPU]; !ok {
			if value, err := resource.ParseQuantity(*app.Spec.Executor.CoreLimit); err == nil {
				minResource[corev1.ResourceCPU] = value
			}
		}
	}

	// Memory + MemoryOverhead correspond to executor's memory request
	if app.Spec.Executor.Memory != nil {
		if value, err := resource.ParseQuantity(*app.Spec.Executor.Memory); err == nil {
			minResource[corev1.ResourceMemory] = value
		}
	}
	if app.Spec.Executor.MemoryOverhead != nil {
		if value, err := resource.ParseQuantity(*app.Spec.Executor.MemoryOverhead); err == nil {
			if existing, ok := minResource[corev1.ResourceMemory]; ok {
				existing.Add(value)
				minResource[corev1.ResourceMemory] = existing
			}
		}
	}

	resourceList := []corev1.ResourceList{{}}
	for i := int32(0); i < *app.Spec.Executor.Instances; i++ {
		resourceList = append(resourceList, minResource)
	}
	return SumResourceList(resourceList)
}

// GetInitialExecutorNumber calculates the initial number of executor pods that will be requested by the driver on startup.
func GetInitialExecutorNumber(app *v1beta2.SparkApplication) int32 {
	// The reference for this implementation: https://github.com/apache/spark/blob/ba208b9ca99990fa329c36b28d0aa2a5f4d0a77e/core/src/main/scala/org/apache/spark/scheduler/cluster/SchedulerBackendUtils.scala#L31
	var initialNumExecutors int32

	dynamicAllocationEnabled := app.Spec.DynamicAllocation != nil && app.Spec.DynamicAllocation.Enabled
	if dynamicAllocationEnabled {
		if app.Spec.Executor.Instances != nil {
			initialNumExecutors = max(initialNumExecutors, *app.Spec.Executor.Instances)
		}
		if app.Spec.DynamicAllocation.InitialExecutors != nil {
			initialNumExecutors = max(initialNumExecutors, *app.Spec.DynamicAllocation.InitialExecutors)
		}
		if app.Spec.DynamicAllocation.MinExecutors != nil {
			initialNumExecutors = max(initialNumExecutors, *app.Spec.DynamicAllocation.MinExecutors)
		}
	} else {
		initialNumExecutors = 2
		if app.Spec.Executor.Instances != nil {
			initialNumExecutors = *app.Spec.Executor.Instances
		}
	}

	return initialNumExecutors
}

// IsDynamicAllocationEnabled determines if Spark Dynamic Allocation is enabled in app.Spec.DynamicAllocation or in
// app.Spec.SparkConf. app.Spec.DynamicAllocation configs will take precedence over app.Spec.SparkConf configs.
func IsDynamicAllocationEnabled(app *v1beta2.SparkApplication) bool {
	if app.Spec.DynamicAllocation != nil {
		return app.Spec.DynamicAllocation.Enabled
	}
	dynamicAllocationConfVal, _ := strconv.ParseBool(app.Spec.SparkConf[common.SparkDynamicAllocationEnabled])
	return dynamicAllocationConfVal
}
