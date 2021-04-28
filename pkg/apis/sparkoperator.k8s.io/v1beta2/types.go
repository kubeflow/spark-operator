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

package v1beta2

import (
	apiv1 "k8s.io/api/core/v1"
	extensions "k8s.io/api/extensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SparkApplicationType describes the type of a Spark application.
type SparkApplicationType string

// Different types of Spark applications.
const (
	JavaApplicationType   SparkApplicationType = "Java"
	ScalaApplicationType  SparkApplicationType = "Scala"
	PythonApplicationType SparkApplicationType = "Python"
	RApplicationType      SparkApplicationType = "R"
)

// DeployMode describes the type of deployment of a Spark application.
type DeployMode string

// Different types of deployments.
const (
	ClusterMode         DeployMode = "cluster"
	ClientMode          DeployMode = "client"
	InClusterClientMode DeployMode = "in-cluster-client"
)

// RestartPolicy is the policy of if and in which conditions the controller should restart a terminated application.
// This completely defines actions to be taken on any kind of Failures during an application run.
type RestartPolicy struct {
	// Type specifies the RestartPolicyType.
	// +kubebuilder:validation:Enum={Never,Always,OnFailure}
	Type RestartPolicyType `json:"type,omitempty"`

	// OnSubmissionFailureRetries is the number of times to retry submitting an application before giving up.
	// This is best effort and actual retry attempts can be >= the value specified due to caching.
	// These are required if RestartPolicy is OnFailure.
	// +kubebuilder:validation:Minimum=0
	// +optional
	OnSubmissionFailureRetries *int32 `json:"onSubmissionFailureRetries,omitempty"`

	// OnFailureRetries the number of times to retry running an application before giving up.
	// +kubebuilder:validation:Minimum=0
	// +optional
	OnFailureRetries *int32 `json:"onFailureRetries,omitempty"`

	// OnSubmissionFailureRetryInterval is the interval in seconds between retries on failed submissions.
	// +kubebuilder:validation:Minimum=1
	// +optional
	OnSubmissionFailureRetryInterval *int64 `json:"onSubmissionFailureRetryInterval,omitempty"`

	// OnFailureRetryInterval is the interval in seconds between retries on failed runs.
	// +kubebuilder:validation:Minimum=1
	// +optional
	OnFailureRetryInterval *int64 `json:"onFailureRetryInterval,omitempty"`
}

type RestartPolicyType string

const (
	Never     RestartPolicyType = "Never"
	OnFailure RestartPolicyType = "OnFailure"
	Always    RestartPolicyType = "Always"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:defaulter-gen=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=scheduledsparkapp,singular=scheduledsparkapplication

type ScheduledSparkApplication struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`
	Spec              ScheduledSparkApplicationSpec   `json:"spec"`
	Status            ScheduledSparkApplicationStatus `json:"status,omitempty"`
}

type ConcurrencyPolicy string

const (
	// ConcurrencyAllow allows SparkApplications to run concurrently.
	ConcurrencyAllow ConcurrencyPolicy = "Allow"
	// ConcurrencyForbid forbids concurrent runs of SparkApplications, skipping the next run if the previous
	// one hasn't finished yet.
	ConcurrencyForbid ConcurrencyPolicy = "Forbid"
	// ConcurrencyReplace kills the currently running SparkApplication instance and replaces it with a new one.
	ConcurrencyReplace ConcurrencyPolicy = "Replace"
)

type ScheduledSparkApplicationSpec struct {
	// Schedule is a cron schedule on which the application should run.
	Schedule string `json:"schedule"`
	// Template is a template from which SparkApplication instances can be created.
	Template SparkApplicationSpec `json:"template"`
	// Suspend is a flag telling the controller to suspend subsequent runs of the application if set to true.
	// +optional
	// Defaults to false.
	Suspend *bool `json:"suspend,omitempty"`
	// ConcurrencyPolicy is the policy governing concurrent SparkApplication runs.
	ConcurrencyPolicy ConcurrencyPolicy `json:"concurrencyPolicy,omitempty"`
	// SuccessfulRunHistoryLimit is the number of past successful runs of the application to keep.
	// +optional
	// Defaults to 1.
	SuccessfulRunHistoryLimit *int32 `json:"successfulRunHistoryLimit,omitempty"`
	// FailedRunHistoryLimit is the number of past failed runs of the application to keep.
	// +optional
	// Defaults to 1.
	FailedRunHistoryLimit *int32 `json:"failedRunHistoryLimit,omitempty"`
}

type ScheduleState string

const (
	FailedValidationState ScheduleState = "FailedValidation"
	ScheduledState        ScheduleState = "Scheduled"
)

type ScheduledSparkApplicationStatus struct {
	// LastRun is the time when the last run of the application started.
	// +nullable
	LastRun metav1.Time `json:"lastRun,omitempty"`
	// NextRun is the time when the next run of the application will start.
	// +nullable
	NextRun metav1.Time `json:"nextRun,omitempty"`
	// LastRunName is the name of the SparkApplication for the most recent run of the application.
	LastRunName string `json:"lastRunName,omitempty"`
	// PastSuccessfulRunNames keeps the names of SparkApplications for past successful runs.
	PastSuccessfulRunNames []string `json:"pastSuccessfulRunNames,omitempty"`
	// PastFailedRunNames keeps the names of SparkApplications for past failed runs.
	PastFailedRunNames []string `json:"pastFailedRunNames,omitempty"`
	// ScheduleState is the current scheduling state of the application.
	ScheduleState ScheduleState `json:"scheduleState,omitempty"`
	// Reason tells why the ScheduledSparkApplication is in the particular ScheduleState.
	Reason string `json:"reason,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ScheduledSparkApplicationList carries a list of ScheduledSparkApplication objects.
type ScheduledSparkApplicationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ScheduledSparkApplication `json:"items,omitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:defaulter-gen=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=sparkapp,singular=sparkapplication

// SparkApplication represents a Spark application running on and using Kubernetes as a cluster manager.
type SparkApplication struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`
	Spec              SparkApplicationSpec   `json:"spec"`
	Status            SparkApplicationStatus `json:"status,omitempty"`
}

// SparkApplicationSpec describes the specification of a Spark application using Kubernetes as a cluster manager.
// It carries every pieces of information a spark-submit command takes and recognizes.
type SparkApplicationSpec struct {
	// Type tells the type of the Spark application.
	// +kubebuilder:validation:Enum={Java,Python,Scala,R}
	Type SparkApplicationType `json:"type"`
	// SparkVersion is the version of Spark the application uses.
	SparkVersion string `json:"sparkVersion"`
	// Mode is the deployment mode of the Spark application.
	// +kubebuilder:validation:Enum={cluster,client}
	Mode DeployMode `json:"mode,omitempty"`
	// ProxyUser specifies the user to impersonate when submitting the application.
	// It maps to the command-line flag "--proxy-user" in spark-submit.
	// +optional
	ProxyUser *string `json:"proxyUser,omitempty"`
	// Image is the container image for the driver, executor, and init-container. Any custom container images for the
	// driver, executor, or init-container takes precedence over this.
	// +optional
	Image *string `json:"image,omitempty"`
	// ImagePullPolicy is the image pull policy for the driver, executor, and init-container.
	// +optional
	ImagePullPolicy *string `json:"imagePullPolicy,omitempty"`
	// ImagePullSecrets is the list of image-pull secrets.
	// +optional
	ImagePullSecrets []string `json:"imagePullSecrets,omitempty"`
	// MainClass is the fully-qualified main class of the Spark application.
	// This only applies to Java/Scala Spark applications.
	// +optional
	MainClass *string `json:"mainClass,omitempty"`
	// MainFile is the path to a bundled JAR, Python, or R file of the application.
	// +optional
	MainApplicationFile *string `json:"mainApplicationFile"`
	// Arguments is a list of arguments to be passed to the application.
	// +optional
	Arguments []string `json:"arguments,omitempty"`
	// SparkConf carries user-specified Spark configuration properties as they would use the  "--conf" option in
	// spark-submit.
	// +optional
	SparkConf map[string]string `json:"sparkConf,omitempty"`
	// HadoopConf carries user-specified Hadoop configuration properties as they would use the  the "--conf" option
	// in spark-submit.  The SparkApplication controller automatically adds prefix "spark.hadoop." to Hadoop
	// configuration properties.
	// +optional
	HadoopConf map[string]string `json:"hadoopConf,omitempty"`
	// SparkConfigMap carries the name of the ConfigMap containing Spark configuration files such as log4j.properties.
	// The controller will add environment variable SPARK_CONF_DIR to the path where the ConfigMap is mounted to.
	// +optional
	SparkConfigMap *string `json:"sparkConfigMap,omitempty"`
	// HadoopConfigMap carries the name of the ConfigMap containing Hadoop configuration files such as core-site.xml.
	// The controller will add environment variable HADOOP_CONF_DIR to the path where the ConfigMap is mounted to.
	// +optional
	HadoopConfigMap *string `json:"hadoopConfigMap,omitempty"`
	// Volumes is the list of Kubernetes volumes that can be mounted by the driver and/or executors.
	// +optional
	Volumes []apiv1.Volume `json:"volumes,omitempty"`
	// Driver is the driver specification.
	Driver DriverSpec `json:"driver"`
	// Executor is the executor specification.
	Executor ExecutorSpec `json:"executor"`
	// Deps captures all possible types of dependencies of a Spark application.
	// +optional
	Deps Dependencies `json:"deps,omitempty"`
	// RestartPolicy defines the policy on if and in which conditions the controller should restart an application.
	RestartPolicy RestartPolicy `json:"restartPolicy,omitempty"`
	// NodeSelector is the Kubernetes node selector to be added to the driver and executor pods.
	// This field is mutually exclusive with nodeSelector at podSpec level (driver or executor).
	// This field will be deprecated in future versions (at SparkApplicationSpec level).
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// FailureRetries is the number of times to retry a failed application before giving up.
	// This is best effort and actual retry attempts can be >= the value specified.
	// +optional
	FailureRetries *int32 `json:"failureRetries,omitempty"`
	// RetryInterval is the unit of intervals in seconds between submission retries.
	// +optional
	RetryInterval *int64 `json:"retryInterval,omitempty"`
	// This sets the major Python version of the docker
	// image used to run the driver and executor containers. Can either be 2 or 3, default 2.
	// +optional
	// +kubebuilder:validation:Enum={"2","3"}
	PythonVersion *string `json:"pythonVersion,omitempty"`
	// This sets the Memory Overhead Factor that will allocate memory to non-JVM memory.
	// For JVM-based jobs this value will default to 0.10, for non-JVM jobs 0.40. Value of this field will
	// be overridden by `Spec.Driver.MemoryOverhead` and `Spec.Executor.MemoryOverhead` if they are set.
	// +optional
	MemoryOverheadFactor *string `json:"memoryOverheadFactor,omitempty"`
	// Monitoring configures how monitoring is handled.
	// +optional
	Monitoring *MonitoringSpec `json:"monitoring,omitempty"`
	// BatchScheduler configures which batch scheduler will be used for scheduling
	// +optional
	BatchScheduler *string `json:"batchScheduler,omitempty"`
	// TimeToLiveSeconds defines the Time-To-Live (TTL) duration in seconds for this SparkApplication
	// after its termination.
	// The SparkApplication object will be garbage collected if the current time is more than the
	// TimeToLiveSeconds since its termination.
	// +optional
	TimeToLiveSeconds *int64 `json:"timeToLiveSeconds,omitempty"`
	// BatchSchedulerOptions provides fine-grained control on how to batch scheduling.
	// +optional
	BatchSchedulerOptions *BatchSchedulerConfiguration `json:"batchSchedulerOptions,omitempty"`
	// SparkUIOptions allows configuring the Service and the Ingress to expose the sparkUI
	// +optional
	SparkUIOptions *SparkUIConfiguration `json:"sparkUIOptions,omitempty"`
	// DynamicAllocation configures dynamic allocation that becomes available for the Kubernetes
	// scheduler backend since Spark 3.0.
	// +optional
	DynamicAllocation *DynamicAllocation `json:"dynamicAllocation,omitempty"`
}

// BatchSchedulerConfiguration used to configure how to batch scheduling Spark Application
type BatchSchedulerConfiguration struct {
	// Queue stands for the resource queue which the application belongs to, it's being used in Volcano batch scheduler.
	// +optional
	Queue *string `json:"queue,omitempty"`
	// PriorityClassName stands for the name of k8s PriorityClass resource, it's being used in Volcano batch scheduler.
	// +optional
	PriorityClassName *string `json:"priorityClassName,omitempty"`
	// Resources stands for the resource list custom request for. Usually it is used to define the lower-bound limit.
	// If specified, volcano scheduler will consider it as the resources requested.
	// +optional
	Resources apiv1.ResourceList `json:"resources,omitempty"`
}

// SparkUIConfiguration is for driver UI specific configuration parameters.
type SparkUIConfiguration struct {
	// ServicePort allows configuring the port at service level that might be different from the targetPort.
	// TargetPort should be the same as the one defined in spark.ui.port
	// +optional
	ServicePort *int32 `json:"servicePort"`
	// ServiceType allows configuring the type of the service. Defaults to ClusterIP.
	// +optional
	ServiceType *apiv1.ServiceType `json:"serviceType"`
	// IngressAnnotations is a map of key,value pairs of annotations that might be added to the ingress object. i.e. specify nginx as ingress.class
	// +optional
	IngressAnnotations map[string]string `json:"ingressAnnotations,omitempty"`
	// TlsHosts is useful If we need to declare SSL certificates to the ingress object
	// +optional
	IngressTLS []extensions.IngressTLS `json:"ingressTLS,omitempty"`
}

// ApplicationStateType represents the type of the current state of an application.
type ApplicationStateType string

// Different states an application may have.
const (
	NewState              ApplicationStateType = ""
	SubmittedState        ApplicationStateType = "SUBMITTED"
	RunningState          ApplicationStateType = "RUNNING"
	CompletedState        ApplicationStateType = "COMPLETED"
	FailedState           ApplicationStateType = "FAILED"
	FailedSubmissionState ApplicationStateType = "SUBMISSION_FAILED"
	PendingRerunState     ApplicationStateType = "PENDING_RERUN"
	InvalidatingState     ApplicationStateType = "INVALIDATING"
	SucceedingState       ApplicationStateType = "SUCCEEDING"
	FailingState          ApplicationStateType = "FAILING"
	UnknownState          ApplicationStateType = "UNKNOWN"
)

// ApplicationState tells the current state of the application and an error message in case of failures.
type ApplicationState struct {
	State        ApplicationStateType `json:"state"`
	ErrorMessage string               `json:"errorMessage,omitempty"`
}

// DriverState tells the current state of a spark driver.
type DriverState string

// Different states a spark driver may have.
const (
	DriverPendingState   DriverState = "PENDING"
	DriverRunningState   DriverState = "RUNNING"
	DriverCompletedState DriverState = "COMPLETED"
	DriverFailedState    DriverState = "FAILED"
	DriverUnknownState   DriverState = "UNKNOWN"
)

// ExecutorState tells the current state of an executor.
type ExecutorState string

// Different states an executor may have.
const (
	ExecutorPendingState   ExecutorState = "PENDING"
	ExecutorRunningState   ExecutorState = "RUNNING"
	ExecutorCompletedState ExecutorState = "COMPLETED"
	ExecutorFailedState    ExecutorState = "FAILED"
	ExecutorUnknownState   ExecutorState = "UNKNOWN"
)

// SparkApplicationStatus describes the current status of a Spark application.
type SparkApplicationStatus struct {
	// SparkApplicationID is set by the spark-distribution(via spark.app.id config) on the driver and executor pods
	SparkApplicationID string `json:"sparkApplicationId,omitempty"`
	// SubmissionID is a unique ID of the current submission of the application.
	SubmissionID string `json:"submissionID,omitempty"`
	// LastSubmissionAttemptTime is the time for the last application submission attempt.
	// +nullable
	LastSubmissionAttemptTime metav1.Time `json:"lastSubmissionAttemptTime,omitempty"`
	// CompletionTime is the time when the application runs to completion if it does.
	// +nullable
	TerminationTime metav1.Time `json:"terminationTime,omitempty"`
	// DriverInfo has information about the driver.
	DriverInfo DriverInfo `json:"driverInfo"`
	// AppState tells the overall application state.
	AppState ApplicationState `json:"applicationState,omitempty"`
	// ExecutorState records the state of executors by executor Pod names.
	ExecutorState map[string]ExecutorState `json:"executorState,omitempty"`
	// ExecutionAttempts is the total number of attempts to run a submitted application to completion.
	// Incremented upon each attempted run of the application and reset upon invalidation.
	ExecutionAttempts int32 `json:"executionAttempts,omitempty"`
	// SubmissionAttempts is the total number of attempts to submit an application to run.
	// Incremented upon each attempted submission of the application and reset upon invalidation and rerun.
	SubmissionAttempts int32 `json:"submissionAttempts,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SparkApplicationList carries a list of SparkApplication objects.
type SparkApplicationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SparkApplication `json:"items,omitempty"`
}

// Dependencies specifies all possible types of dependencies of a Spark application.
type Dependencies struct {
	// Jars is a list of JAR files the Spark application depends on.
	// +optional
	Jars []string `json:"jars,omitempty"`
	// Files is a list of files the Spark application depends on.
	// +optional
	Files []string `json:"files,omitempty"`
	// PyFiles is a list of Python files the Spark application depends on.
	// +optional
	PyFiles []string `json:"pyFiles,omitempty"`
	// Packages is a list of maven coordinates of jars to include on the driver and executor
	// classpaths. This will search the local maven repo, then maven central and any additional
	// remote repositories given by the "repositories" option.
	// Each package should be of the form "groupId:artifactId:version".
	// +optional
	Packages []string `json:"packages,omitempty"`
	// ExcludePackages is a list of "groupId:artifactId", to exclude while resolving the
	// dependencies provided in Packages to avoid dependency conflicts.
	// +optional
	ExcludePackages []string `json:"excludePackages,omitempty"`
	// Repositories is a list of additional remote repositories to search for the maven coordinate
	// given with the "packages" option.
	// +optional
	Repositories []string `json:"repositories,omitempty"`
}

// SparkPodSpec defines common things that can be customized for a Spark driver or executor pod.
// TODO: investigate if we should use v1.PodSpec and limit what can be set instead.
type SparkPodSpec struct {
	// Cores maps to `spark.driver.cores` or `spark.executor.cores` for the driver and executors, respectively.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Cores *int32 `json:"cores,omitempty"`
	// CoreLimit specifies a hard limit on CPU cores for the pod.
	// Optional
	CoreLimit *string `json:"coreLimit,omitempty"`
	// Memory is the amount of memory to request for the pod.
	// +optional
	Memory *string `json:"memory,omitempty"`
	// MemoryOverhead is the amount of off-heap memory to allocate in cluster mode, in MiB unless otherwise specified.
	// +optional
	MemoryOverhead *string `json:"memoryOverhead,omitempty"`
	// GPU specifies GPU requirement for the pod.
	// +optional
	GPU *GPUSpec `json:"gpu,omitempty"`
	// Image is the container image to use. Overrides Spec.Image if set.
	// +optional
	Image *string `json:"image,omitempty"`
	// ConfigMaps carries information of other ConfigMaps to add to the pod.
	// +optional
	ConfigMaps []NamePath `json:"configMaps,omitempty"`
	// Secrets carries information of secrets to add to the pod.
	// +optional
	Secrets []SecretInfo `json:"secrets,omitempty"`
	// Env carries the environment variables to add to the pod.
	// +optional
	Env []apiv1.EnvVar `json:"env,omitempty"`
	// EnvVars carries the environment variables to add to the pod.
	// Deprecated. Consider using `env` instead.
	// +optional
	EnvVars map[string]string `json:"envVars,omitempty"`
	// EnvFrom is a list of sources to populate environment variables in the container.
	// +optional
	EnvFrom []apiv1.EnvFromSource `json:"envFrom,omitempty"`
	// EnvSecretKeyRefs holds a mapping from environment variable names to SecretKeyRefs.
	// Deprecated. Consider using `env` instead.
	// +optional
	EnvSecretKeyRefs map[string]NameKey `json:"envSecretKeyRefs,omitempty"`
	// Labels are the Kubernetes labels to be added to the pod.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// Annotations are the Kubernetes annotations to be added to the pod.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
	// VolumeMounts specifies the volumes listed in ".spec.volumes" to mount into the main container's filesystem.
	// +optional
	VolumeMounts []apiv1.VolumeMount `json:"volumeMounts,omitempty"`
	// Affinity specifies the affinity/anti-affinity settings for the pod.
	// +optional
	Affinity *apiv1.Affinity `json:"affinity,omitempty"`
	// Tolerations specifies the tolerations listed in ".spec.tolerations" to be applied to the pod.
	// +optional
	Tolerations []apiv1.Toleration `json:"tolerations,omitempty"`
	// PodSecurityContext specifies the PodSecurityContext to apply.
	// +optional
	PodSecurityContext *apiv1.PodSecurityContext `json:"podSecurityContext,omitempty"`
	// SecurityContext specifies the container's SecurityContext to apply.
	// +optional
	SecurityContext *apiv1.SecurityContext `json:"securityContext,omitempty"`
	// SchedulerName specifies the scheduler that will be used for scheduling
	// +optional
	SchedulerName *string `json:"schedulerName,omitempty"`
	// Sidecars is a list of sidecar containers that run along side the main Spark container.
	// +optional
	Sidecars []apiv1.Container `json:"sidecars,omitempty"`
	// InitContainers is a list of init-containers that run to completion before the main Spark container.
	// +optional
	InitContainers []apiv1.Container `json:"initContainers,omitempty"`
	// HostNetwork indicates whether to request host networking for the pod or not.
	// +optional
	HostNetwork *bool `json:"hostNetwork,omitempty"`
	// NodeSelector is the Kubernetes node selector to be added to the driver and executor pods.
	// This field is mutually exclusive with nodeSelector at SparkApplication level (which will be deprecated).
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	// DnsConfig dns settings for the pod, following the Kubernetes specifications.
	// +optional
	DNSConfig *apiv1.PodDNSConfig `json:"dnsConfig,omitempty"`
	// Termination grace period seconds for the pod
	// +optional
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty"`
	// ServiceAccount is the name of the custom Kubernetes service account used by the pod.
	// +optional
	ServiceAccount *string `json:"serviceAccount,omitempty"`
	// HostAliases settings for the pod, following the Kubernetes specifications.
	// +optional
	HostAliases []apiv1.HostAlias `json:"hostAliases,omitempty"`
}

// DriverSpec is specification of the driver.
type DriverSpec struct {
	SparkPodSpec `json:",inline"`
	// PodName is the name of the driver pod that the user creates. This is used for the
	// in-cluster client mode in which the user creates a client pod where the driver of
	// the user application runs. It's an error to set this field if Mode is not
	// in-cluster-client.
	// +optional
	// +kubebuilder:validation:Pattern=[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*
	PodName *string `json:"podName,omitempty"`
	// CoreRequest is the physical CPU core request for the driver.
	// Maps to `spark.kubernetes.driver.request.cores` that is available since Spark 3.0.
	// +optional
	CoreRequest *string `json:"coreRequest,omitempty"`
	// JavaOptions is a string of extra JVM options to pass to the driver. For instance,
	// GC settings or other logging.
	// +optional
	JavaOptions *string `json:"javaOptions,omitempty"`
	// Lifecycle for running preStop or postStart commands
	// +optional
	Lifecycle *apiv1.Lifecycle `json:"lifecycle,omitempty"`
	// KubernetesMaster is the URL of the Kubernetes master used by the driver to manage executor pods and
	// other Kubernetes resources. Default to https://kubernetes.default.svc.
	// +optional
	KubernetesMaster *string `json:"kubernetesMaster,omitempty"`
	// ServiceAnnotations defines the annotations to be added to the Kubernetes headless service used by
	// executors to connect to the driver.
	// +optional
	ServiceAnnotations map[string]string `json:"serviceAnnotations,omitempty"`
	// Ports settings for the pods, following the Kubernetes specifications.
	// +optional
	Ports []Port `json:"ports,omitempty"`
}

// ExecutorSpec is specification of the executor.
type ExecutorSpec struct {
	SparkPodSpec `json:",inline"`
	// Instances is the number of executor instances.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Instances *int32 `json:"instances,omitempty"`
	// CoreRequest is the physical CPU core request for the executors.
	// Maps to `spark.kubernetes.executor.request.cores` that is available since Spark 2.4.
	// +optional
	CoreRequest *string `json:"coreRequest,omitempty"`
	// JavaOptions is a string of extra JVM options to pass to the executors. For instance,
	// GC settings or other logging.
	// +optional
	JavaOptions *string `json:"javaOptions,omitempty"`
	// DeleteOnTermination specify whether executor pods should be deleted in case of failure or normal termination.
	// Maps to `spark.kubernetes.executor.deleteOnTermination` that is available since Spark 3.0.
	// +optional
	DeleteOnTermination *bool `json:"deleteOnTermination,omitempty"`
	// Ports settings for the pods, following the Kubernetes specifications.
	// +optional
	Ports []Port `json:"ports,omitempty"`
}

// NamePath is a pair of a name and a path to which the named objects should be mounted to.
type NamePath struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

// SecretType tells the type of a secret.
type SecretType string

// An enumeration of secret types supported.
const (
	// GCPServiceAccountSecret is for secrets from a GCP service account Json key file that needs
	// the environment variable GOOGLE_APPLICATION_CREDENTIALS.
	GCPServiceAccountSecret SecretType = "GCPServiceAccount"
	// HadoopDelegationTokenSecret is for secrets from an Hadoop delegation token that needs the
	// environment variable HADOOP_TOKEN_FILE_LOCATION.
	HadoopDelegationTokenSecret SecretType = "HadoopDelegationToken"
	// GenericType is for secrets that needs no special handling.
	GenericType SecretType = "Generic"
)

// DriverInfo captures information about the driver.
type DriverInfo struct {
	WebUIServiceName string `json:"webUIServiceName,omitempty"`
	// UI Details for the UI created via ClusterIP service accessible from within the cluster.
	WebUIPort    int32  `json:"webUIPort,omitempty"`
	WebUIAddress string `json:"webUIAddress,omitempty"`
	// Ingress Details if an ingress for the UI was created.
	WebUIIngressName    string `json:"webUIIngressName,omitempty"`
	WebUIIngressAddress string `json:"webUIIngressAddress,omitempty"`
	PodName             string `json:"podName,omitempty"`
}

// SecretInfo captures information of a secret.
type SecretInfo struct {
	Name string     `json:"name"`
	Path string     `json:"path"`
	Type SecretType `json:"secretType"`
}

// NameKey represents the name and key of a SecretKeyRef.
type NameKey struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

// Port represents the port definition in the pods objects.
type Port struct {
	Name          string `json:"name"`
	Protocol      string `json:"protocol"`
	ContainerPort int32  `json:"containerPort"`
}

// MonitoringSpec defines the monitoring specification.
type MonitoringSpec struct {
	// ExposeDriverMetrics specifies whether to expose metrics on the driver.
	ExposeDriverMetrics bool `json:"exposeDriverMetrics"`
	// ExposeExecutorMetrics specifies whether to expose metrics on the executors.
	ExposeExecutorMetrics bool `json:"exposeExecutorMetrics"`
	// MetricsProperties is the content of a custom metrics.properties for configuring the Spark metric system.
	// +optional
	// If not specified, the content in spark-docker/conf/metrics.properties will be used.
	MetricsProperties *string `json:"metricsProperties,omitempty"`
	// MetricsPropertiesFile is the container local path of file metrics.properties for configuring
	//the Spark metric system. If not specified, value /etc/metrics/conf/metrics.properties will be used.
	// +optional
	MetricsPropertiesFile *string `json:"metricsPropertiesFile,omitempty"`
	// Prometheus is for configuring the Prometheus JMX exporter.
	// +optional
	Prometheus *PrometheusSpec `json:"prometheus,omitempty"`
}

// PrometheusSpec defines the Prometheus specification when Prometheus is to be used for
// collecting and exposing metrics.
type PrometheusSpec struct {
	// JmxExporterJar is the path to the Prometheus JMX exporter jar in the container.
	JmxExporterJar string `json:"jmxExporterJar"`
	// Port is the port of the HTTP server run by the Prometheus JMX exporter.
	// If not specified, 8090 will be used as the default.
	// +kubebuilder:validation:Minimum=1024
	// +kubebuilder:validation:Maximum=49151
	// +optional
	Port *int32 `json:"port,omitempty"`
	// PortName is the port name of prometheus JMX exporter port.
	// If not specified, jmx-exporter will be used as the default.
	// +optional
	PortName *string `json:"portName,omitempty"`
	// ConfigFile is the path to the custom Prometheus configuration file provided in the Spark image.
	// ConfigFile takes precedence over Configuration, which is shown below.
	// +optional
	ConfigFile *string `json:"configFile,omitempty"`
	// Configuration is the content of the Prometheus configuration needed by the Prometheus JMX exporter.
	// If not specified, the content in spark-docker/conf/prometheus.yaml will be used.
	// Configuration has no effect if ConfigFile is set.
	// +optional
	Configuration *string `json:"configuration,omitempty"`
}

type GPUSpec struct {
	// Name is GPU resource name, such as: nvidia.com/gpu or amd.com/gpu
	Name string `json:"name"`
	// Quantity is the number of GPUs to request for driver or executor.
	Quantity int64 `json:"quantity"`
}

// DynamicAllocation contains configuration options for dynamic allocation.
type DynamicAllocation struct {
	// Enabled controls whether dynamic allocation is enabled or not.
	Enabled bool `json:"enabled,omitempty"`
	// InitialExecutors is the initial number of executors to request. If .spec.executor.instances
	// is also set, the initial number of executors is set to the bigger of that and this option.
	// +optional
	InitialExecutors *int32 `json:"initialExecutors,omitempty"`
	// MinExecutors is the lower bound for the number of executors if dynamic allocation is enabled.
	// +optional
	MinExecutors *int32 `json:"minExecutors,omitempty"`
	// MaxExecutors is the upper bound for the number of executors if dynamic allocation is enabled.
	// +optional
	MaxExecutors *int32 `json:"maxExecutors,omitempty"`
	// ShuffleTrackingTimeout controls the timeout in milliseconds for executors that are holding
	// shuffle data if shuffle tracking is enabled (true by default if dynamic allocation is enabled).
	// +optional
	ShuffleTrackingTimeout *int64 `json:"shuffleTrackingTimeout,omitempty"`
}

// PrometheusMonitoringEnabled returns if Prometheus monitoring is enabled or not.
func (s *SparkApplication) PrometheusMonitoringEnabled() bool {
	return s.Spec.Monitoring != nil && s.Spec.Monitoring.Prometheus != nil
}

// HasPrometheusConfigFile returns if Prometheus monitoring uses a configuration file in the container.
func (s *SparkApplication) HasPrometheusConfigFile() bool {
	return s.PrometheusMonitoringEnabled() &&
		s.Spec.Monitoring.Prometheus.ConfigFile != nil &&
		*s.Spec.Monitoring.Prometheus.ConfigFile != ""
}

// HasPrometheusConfig returns if Prometheus monitoring defines metricsProperties in the spec.
func (s *SparkApplication) HasMetricsProperties() bool {
	return s.PrometheusMonitoringEnabled() &&
		s.Spec.Monitoring.MetricsProperties != nil &&
		*s.Spec.Monitoring.MetricsProperties != ""
}

// HasPrometheusConfigFile returns if Monitoring defines metricsPropertiesFile in the spec.
func (s *SparkApplication) HasMetricsPropertiesFile() bool {
	return s.PrometheusMonitoringEnabled() &&
		s.Spec.Monitoring.MetricsPropertiesFile != nil &&
		*s.Spec.Monitoring.MetricsPropertiesFile != ""
}

// ExposeDriverMetrics returns if driver metrics should be exposed.
func (s *SparkApplication) ExposeDriverMetrics() bool {
	return s.Spec.Monitoring != nil && s.Spec.Monitoring.ExposeDriverMetrics
}

// ExposeExecutorMetrics returns if executor metrics should be exposed.
func (s *SparkApplication) ExposeExecutorMetrics() bool {
	return s.Spec.Monitoring != nil && s.Spec.Monitoring.ExposeExecutorMetrics
}
