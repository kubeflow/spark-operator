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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// +kubebuilder:skip

func init() {
	SchemeBuilder.Register(&ScheduledSparkApplication{}, &ScheduledSparkApplicationList{})
}

// ScheduledSparkApplicationSpec defines the desired state of ScheduledSparkApplication
type ScheduledSparkApplicationSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make generate" to regenerate code after modifying this file

	// Schedule is a cron schedule on which the application should run.
	Schedule string `json:"schedule"`
	// Template is a template from which SparkApplication instances can be created.
	Template SparkApplicationSpec `json:"template"`
	// Suspend is a flag telling the controller to suspend subsequent runs of the application if set to true.
	// Optional.
	// Defaults to false.
	Suspend *bool `json:"suspend,omitempty"`
	// ConcurrencyPolicy is the policy governing concurrent SparkApplication runs.
	ConcurrencyPolicy ConcurrencyPolicy `json:"concurrencyPolicy,omitempty"`
	// SuccessfulRunHistoryLimit is the number of past successful runs of the application to keep.
	// Optional.
	// Defaults to 1.
	SuccessfulRunHistoryLimit *int32 `json:"successfulRunHistoryLimit,omitempty"`
	// FailedRunHistoryLimit is the number of past failed runs of the application to keep.
	// Optional.
	// Defaults to 1.
	FailedRunHistoryLimit *int32 `json:"failedRunHistoryLimit,omitempty"`
}

// ScheduledSparkApplicationStatus defines the observed state of ScheduledSparkApplication
type ScheduledSparkApplicationStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make generate" to regenerate code after modifying this file

	// LastRun is the time when the last run of the application started.
	LastRun metav1.Time `json:"lastRun,omitempty"`
	// NextRun is the time when the next run of the application will start.
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
// +kubebuilder:subresource:status

// ScheduledSparkApplication is the Schema for the scheduledsparkapplications API
type ScheduledSparkApplication struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ScheduledSparkApplicationSpec   `json:"spec,omitempty"`
	Status ScheduledSparkApplicationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ScheduledSparkApplicationList contains a list of ScheduledSparkApplication
type ScheduledSparkApplicationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ScheduledSparkApplication `json:"items"`
}

type ScheduleState string

const (
	FailedValidationState ScheduleState = "FailedValidation"
	ScheduledState        ScheduleState = "Scheduled"
)
