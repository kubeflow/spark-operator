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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func init() {
	SchemeBuilder.Register(&SparkCluster{}, &SparkClusterList{})
}

// +kubebuilder:object:root=true
// +kubebuilder:metadata:annotations="api-approved.kubernetes.io=https://github.com/kubeflow/spark-operator/pull/1298"
// +kubebuilder:resource:scope=Namespaced,shortName=sparkcluster,singular=sparkcluster
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:JSONPath=.metadata.creationTimestamp,name=Age,type=date
// +kubebuilder:printcolumn:JSONPath=.status.state,name="Status",type=string
// +kubebuilder:printcolumn:JSONPath=.status.master.podName,name="Master",type=string

// SparkCluster is the Schema for deploying Spark Standalone clusters on Kubernetes.
type SparkCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec   SparkClusterSpec   `json:"spec"`
	Status SparkClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SparkClusterList contains a list of SparkCluster.
type SparkClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SparkCluster `json:"items"`
}

// SparkClusterSpec defines the desired state of SparkCluster.
type SparkClusterSpec struct {
	// SparkVersion is the version of Spark used by the cluster.
	SparkVersion string `json:"sparkVersion"`

	// Image is the default container image for master and worker pods.
	// Can be overridden by pod templates.
	// +optional
	Image *string `json:"image,omitempty"`

	// SparkConf carries user-specified Spark configuration properties.
	// +optional
	SparkConf map[string]string `json:"sparkConf,omitempty"`

	// Master is the Spark master specification.
	Master MasterSpec `json:"master"`

	// WorkerGroups defines groups of Spark workers with varying configurations.
	// +optional
	WorkerGroups []WorkerGroupSpec `json:"workerGroups,omitempty"`
}

// MasterSpec defines the Spark master pod specification.
type MasterSpec struct {
	// Template is the pod template for the Spark master.
	// +optional
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:validation:Type:=object
	// +kubebuilder:pruning:PreserveUnknownFields
	Template *SparkClusterPodTemplateSpec `json:"template,omitempty"`
}

// WorkerGroupSpec defines a group of Spark worker pods.
type WorkerGroupSpec struct {
	// Name is the name of this worker group. Must be unique within a SparkCluster.
	Name string `json:"name"`

	// Replicas is the number of worker pods in this group.
	// +optional
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	Replicas *int32 `json:"replicas,omitempty"`

	// Template is the pod template for workers in this group.
	// +optional
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:validation:Type:=object
	// +kubebuilder:pruning:PreserveUnknownFields
	Template *SparkClusterPodTemplateSpec `json:"template,omitempty"`
}

// SparkClusterPodTemplateSpec wraps a PodTemplateSpec for Spark cluster pods.
type SparkClusterPodTemplateSpec struct {
	// Metadata of the pods created from this template.
	// +optional
	Metadata *SparkClusterPodTemplateMeta `json:"metadata,omitempty"`

	// Spec defines the desired state of the pod.
	// +optional
	Spec *SparkClusterPodSpec `json:"spec,omitempty"`
}

// SparkClusterPodTemplateMeta contains metadata for pod templates.
type SparkClusterPodTemplateMeta struct {
	// Labels to add to the pod.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// Annotations to add to the pod.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// SparkClusterPodSpec contains the pod spec fields for Spark cluster pods.
type SparkClusterPodSpec struct {
	// Containers is a list of containers in the pod.
	Containers []SparkClusterContainer `json:"containers"`

	// NodeSelector is a selector for scheduling pods to nodes.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// Tolerations is a list of tolerations for the pod.
	// +optional
	Tolerations []Toleration `json:"tolerations,omitempty"`

	// ServiceAccountName is the name of the ServiceAccount to use.
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty"`
}

// SparkClusterContainer defines a container within a Spark cluster pod.
type SparkClusterContainer struct {
	// Name of the container.
	Name string `json:"name"`

	// Image is the container image.
	// +optional
	Image string `json:"image,omitempty"`

	// ImagePullPolicy defines the pull policy.
	// +optional
	ImagePullPolicy string `json:"imagePullPolicy,omitempty"`

	// Resources defines the resource requirements.
	// +optional
	Resources *ResourceRequirements `json:"resources,omitempty"`

	// SecurityContext defines the security options.
	// +optional
	SecurityContext *SecurityContext `json:"securityContext,omitempty"`

	// Env is a list of environment variables.
	// +optional
	Env []EnvVar `json:"env,omitempty"`

	// Ports is a list of ports to expose.
	// +optional
	Ports []ContainerPort `json:"ports,omitempty"`
}

// ResourceRequirements describes compute resource requirements.
type ResourceRequirements struct {
	// Requests describes the minimum amount of compute resources required.
	// +optional
	Requests map[string]string `json:"requests,omitempty"`

	// Limits describes the maximum amount of compute resources allowed.
	// +optional
	Limits map[string]string `json:"limits,omitempty"`
}

// SecurityContext holds security configuration.
type SecurityContext struct {
	// AllowPrivilegeEscalation controls whether a process can gain more privileges.
	// +optional
	AllowPrivilegeEscalation *bool `json:"allowPrivilegeEscalation,omitempty"`

	// Capabilities to add/drop.
	// +optional
	Capabilities *Capabilities `json:"capabilities,omitempty"`

	// RunAsNonRoot indicates that the container must run as a non-root user.
	// +optional
	RunAsNonRoot *bool `json:"runAsNonRoot,omitempty"`

	// RunAsUser is the UID to run the entrypoint of the container process.
	// +optional
	RunAsUser *int64 `json:"runAsUser,omitempty"`

	// RunAsGroup is the GID to run the entrypoint of the container process.
	// +optional
	RunAsGroup *int64 `json:"runAsGroup,omitempty"`

	// SeccompProfile defines the seccomp profile settings.
	// +optional
	SeccompProfile *SeccompProfile `json:"seccompProfile,omitempty"`
}

// Capabilities defines Linux capabilities to add/drop.
type Capabilities struct {
	// Drop is a list of capabilities to drop.
	// +optional
	Drop []string `json:"drop,omitempty"`

	// Add is a list of capabilities to add.
	// +optional
	Add []string `json:"add,omitempty"`
}

// SeccompProfile defines the seccomp profile settings.
type SeccompProfile struct {
	// Type indicates which kind of seccomp profile will be applied.
	Type string `json:"type"`
}

// Toleration defines a toleration.
type Toleration struct {
	// Key is the taint key that the toleration applies to.
	// +optional
	Key string `json:"key,omitempty"`

	// Operator represents a key's relationship to the value.
	// +optional
	Operator string `json:"operator,omitempty"`

	// Value is the taint value the toleration matches to.
	// +optional
	Value string `json:"value,omitempty"`

	// Effect indicates the taint effect to match.
	// +optional
	Effect string `json:"effect,omitempty"`
}

// EnvVar represents an environment variable.
type EnvVar struct {
	// Name of the environment variable.
	Name string `json:"name"`

	// Value of the environment variable.
	// +optional
	Value string `json:"value,omitempty"`
}

// ContainerPort represents a network port in a container.
type ContainerPort struct {
	// Name of the port.
	// +optional
	Name string `json:"name,omitempty"`

	// ContainerPort number of the port.
	ContainerPort int32 `json:"containerPort"`

	// Protocol for the port. Defaults to TCP.
	// +optional
	Protocol string `json:"protocol,omitempty"`
}

// SparkClusterStatus defines the observed state of SparkCluster.
type SparkClusterStatus struct {
	// Represents the latest available observations of a SparkCluster's current state.
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty" patchMergeKey:"type" patchStrategy:"merge"`

	// State represents the current state of the SparkCluster.
	State SparkClusterState `json:"state,omitempty"`

	// Master represents the current state of the master pod.
	Master SparkClusterMasterStatus `json:"master,omitempty"`

	// Workers contains the count of worker pods by phase.
	Workers map[string]int `json:"workers,omitempty"`

	// StartTime is the time at which the controller started processing the SparkCluster.
	StartTime metav1.Time `json:"startTime,omitempty"`

	// LastUpdateTime is the time at which the controller last updated the SparkCluster.
	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty"`
}

// SparkClusterConditionType represents the condition types.
type SparkClusterConditionType string

const (
	SparkClusterConditionMasterPodReady SparkClusterConditionType = "MasterPodReady"
)

// SparkClusterConditionReason represents the reason of conditions.
type SparkClusterConditionReason string

const (
	SparkClusterConditionReasonMasterPodReady    SparkClusterConditionReason = "MasterPodReady"
	SparkClusterConditionReasonMasterPodNotReady SparkClusterConditionReason = "MasterPodNotReady"
)

// SparkClusterState represents the current state of the SparkCluster.
type SparkClusterState string

const (
	SparkClusterStateNew          SparkClusterState = ""
	SparkClusterStateProvisioning SparkClusterState = "Provisioning"
	SparkClusterStateReady        SparkClusterState = "Ready"
	SparkClusterStateNotReady     SparkClusterState = "NotReady"
	SparkClusterStateFailed       SparkClusterState = "Failed"
)

// SparkClusterMasterStatus contains the master pod status.
type SparkClusterMasterStatus struct {
	// PodName is the name of the master pod.
	PodName string `json:"podName,omitempty"`

	// PodIP is the IP address of the master pod.
	PodIP string `json:"podIp,omitempty"`

	// ServiceName is the name of the master service.
	ServiceName string `json:"serviceName,omitempty"`
}
