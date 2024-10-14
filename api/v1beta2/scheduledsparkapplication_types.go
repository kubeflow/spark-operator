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

package v1beta2

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

func init() {
	SchemeBuilder.Register(&ScheduledSparkApplication{}, &ScheduledSparkApplicationList{})
}

// ScheduledSparkApplicationSpec defines the desired state of ScheduledSparkApplication.
type ScheduledSparkApplicationSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make generate" to regenerate code after modifying this file

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

// ScheduledSparkApplicationStatus defines the observed state of ScheduledSparkApplication.
type ScheduledSparkApplicationStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make generate" to regenerate code after modifying this file

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

// +kubebuilder:object:root=true
// +kubebuilder:metadata:annotations="api-approved.kubernetes.io=https://github.com/kubeflow/spark-operator/pull/1298"
// +kubebuilder:resource:scope=Namespaced,shortName=scheduledsparkapp,singular=scheduledsparkapplication
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:JSONPath=.spec.schedule,name=Schedule,type=string
// +kubebuilder:printcolumn:JSONPath=.spec.suspend,name=Suspend,type=string
// +kubebuilder:printcolumn:JSONPath=.status.lastRun,name=Last Run,type=date
// +kubebuilder:printcolumn:JSONPath=.status.lastRunName,name=Last Run Name,type=string
// +kubebuilder:printcolumn:JSONPath=.metadata.creationTimestamp,name=Age,type=date

// ScheduledSparkApplication is the Schema for the scheduledsparkapplications API.
type ScheduledSparkApplication struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec   ScheduledSparkApplicationSpec   `json:"spec"`
	Status ScheduledSparkApplicationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ScheduledSparkApplicationList contains a list of ScheduledSparkApplication.
type ScheduledSparkApplicationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ScheduledSparkApplication `json:"items"`
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

type ScheduleState string

const (
	ScheduleStateNew              ScheduleState = ""
	ScheduleStateValidating       ScheduleState = "Validating"
	ScheduleStateScheduled        ScheduleState = "Scheduled"
	ScheduleStateFailedValidation ScheduleState = "FailedValidation"
)
