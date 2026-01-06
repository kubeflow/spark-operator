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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func init() {
	SchemeBuilder.Register(&SparkConnect{}, &SparkConnectList{})
}

// +kubebuilder:object:root=true
// +kubebuilder:metadata:annotations="api-approved.kubernetes.io=https://github.com/kubeflow/spark-operator/pull/1298"
// +kubebuilder:resource:scope=Namespaced,shortName=sparkconn,singular=sparkconnect
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:JSONPath=.metadata.creationTimestamp,name=Age,type=date
// +kubebuilder:printcolumn:JSONPath=.status.state,name="Status",type=string
// +kubebuilder:printcolumn:JSONPath=.status.server.podName,name="PodName",type=string

// SparkConnect is the Schema for the sparkconnections API.
type SparkConnect struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec   SparkConnectSpec   `json:"spec"`
	Status SparkConnectStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SparkConnectList contains a list of SparkConnect.
type SparkConnectList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SparkConnect `json:"items"`
}

// SparkConnectSpec defines the desired state of SparkConnect.
type SparkConnectSpec struct {
	// SparkVersion is the version of Spark the spark connect use.
	SparkVersion string `json:"sparkVersion"`

	// Image is the container image for the driver, executor, and init-container. Any custom container images for the
	// driver, executor, or init-container takes precedence over this.
	// +optional
	Image *string `json:"image,omitempty"`

	// HadoopConf carries user-specified Hadoop configuration properties as they would use the "--conf" option
	// in spark-submit. The SparkApplication controller automatically adds prefix "spark.hadoop." to Hadoop
	// configuration properties.
	// +optional
	HadoopConf map[string]string `json:"hadoopConf,omitempty"`

	// SparkConf carries user-specified Spark configuration properties as they would use the "--conf" option in
	// spark-submit.
	// +optional
	SparkConf map[string]string `json:"sparkConf,omitempty"`

	// Server is the Spark connect server specification.
	Server ServerSpec `json:"server"`

	// Executor is the Spark executor specification.
	Executor ExecutorSpec `json:"executor"`

	// DynamicAllocation configures dynamic allocation that becomes available for the Kubernetes
	// scheduler backend since Spark 3.0.
	// +optional
	DynamicAllocation *DynamicAllocation `json:"dynamicAllocation,omitempty"`
}

// ServerSpec is specification of the Spark connect server.
type ServerSpec struct {
	SparkPodSpec `json:",inline"`

	// Service exposes the Spark connect server.
	// +optional
	Service *corev1.Service `json:"service,omitempty"`
}

// ExecutorSpec is specification of the executor.
type ExecutorSpec struct {
	SparkPodSpec `json:",inline"`

	// Instances is the number of executor instances.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Instances *int32 `json:"instances,omitempty"`
}

// SparkPodSpec defines common things that can be customized for a Spark driver or executor pod.
type SparkPodSpec struct {
	// Cores maps to `spark.driver.cores` or `spark.executor.cores` for the driver and executors, respectively.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Cores *int32 `json:"cores,omitempty"`

	// Memory is the amount of memory to request for the pod.
	// +optional
	Memory *string `json:"memory,omitempty"`

	// Template is a pod template that can be used to define the driver or executor pod configurations that Spark configurations do not support.
	// Spark version >= 3.0.0 is required.
	// Ref: https://spark.apache.org/docs/latest/running-on-kubernetes.html#pod-template.
	// +optional
	// +kubebuilder:validation:Schemaless
	// +kubebuilder:validation:Type:=object
	// +kubebuilder:pruning:PreserveUnknownFields
	Template *corev1.PodTemplateSpec `json:"template,omitempty"`
}

// SparkConnectStatus defines the observed state of SparkConnect.
type SparkConnectStatus struct {
	// Represents the latest available observations of a SparkConnect's current state.
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty" patchMergeKey:"type" patchStrategy:"merge"`

	// State represents the current state of the SparkConnect.
	State SparkConnectState `json:"state,omitempty"`

	// Server represents the current state of the SparkConnect server.
	Server SparkConnectServerStatus `json:"server,omitempty"`

	// Executors represents the current state of the SparkConnect executors.
	Executors map[string]int `json:"executors,omitempty"`

	// StartTime is the time at which the SparkConnect controller started processing the SparkConnect.
	StartTime metav1.Time `json:"startTime,omitempty"`

	// LastUpdateTime is the time at which the SparkConnect controller last updated the SparkConnect.
	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty"`
}

// SparkConnectConditionType represents the condition types of the SparkConnect.
type SparkConnectConditionType string

// All possible condition types of the SparkConnect.
const (
	SparkConnectConditionServerPodReady   SparkConnectConditionType = "ServerPodReady"
	SparkConnectConditionServerPodUpdating SparkConnectConditionType = "ServerPodUpdating"
)

// SparkConnectConditionReason represents the reason of SparkConnect conditions.
type SparkConnectConditionReason string

// All possible reasons of SparkConnect conditions.
const (
	SparkConnectConditionReasonServerPodReady      SparkConnectConditionReason = "ServerPodReady"
	SparkConnectConditionReasonServerPodNotReady   SparkConnectConditionReason = "ServerPodNotReady"
	SparkConnectConditionReasonServerPodSpecChanged SparkConnectConditionReason = "ServerPodSpecChanged"
)

// SparkConnectState represents the current state of the SparkConnect.
type SparkConnectState string

// All possible states of the SparkConnect.
const (
	SparkConnectStateNew          SparkConnectState = ""
	SparkConnectStateProvisioning SparkConnectState = "Provisioning"
	SparkConnectStateReady        SparkConnectState = "Ready"
	SparkConnectStateNotReady     SparkConnectState = "NotReady"
	SparkConnectStateFailed       SparkConnectState = "Failed"
)

type SparkConnectServerStatus struct {
	// PodName is the name of the pod that is running the Spark Connect server.
	PodName string `json:"podName,omitempty"`

	// PodIP is the IP address of the pod that is running the Spark Connect server.
	PodIP string `json:"podIp,omitempty"`

	// ServiceName is the name of the service that is exposing the Spark Connect server.
	ServiceName string `json:"serviceName,omitempty"`
}
