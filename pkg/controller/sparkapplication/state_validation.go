/*
Copyright 2018 Google LLC

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
	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
)

var (
	// Valid Driver State Transitions: map with valid pairs of oldState-> {newStates}
	validDriverStateTransitions = map[v1alpha1.ApplicationStateType][]v1alpha1.ApplicationStateType{
		"": {v1alpha1.NewState, v1alpha1.UnknownState, v1alpha1.SubmittedState,
			v1alpha1.RunningState, v1alpha1.CompletedState, v1alpha1.FailedSubmissionState, v1alpha1.FailedState},

		v1alpha1.NewState: {v1alpha1.UnknownState, v1alpha1.SubmittedState,
			v1alpha1.RunningState, v1alpha1.CompletedState, v1alpha1.FailedSubmissionState, v1alpha1.FailedState},

		v1alpha1.UnknownState: {v1alpha1.NewState, v1alpha1.SubmittedState,
			v1alpha1.RunningState, v1alpha1.CompletedState, v1alpha1.FailedSubmissionState, v1alpha1.FailedState},

		v1alpha1.FailedSubmissionState: {v1alpha1.UnknownState, v1alpha1.SubmittedState,
			v1alpha1.RunningState, v1alpha1.CompletedState, v1alpha1.FailedState},

		v1alpha1.SubmittedState: {v1alpha1.RunningState, v1alpha1.CompletedState, v1alpha1.FailedState},
		v1alpha1.RunningState:   {v1alpha1.CompletedState, v1alpha1.FailedState},
		// The application can be potentially restarted based on the RestartPolicy
		v1alpha1.FailedState:    {v1alpha1.SubmittedState},
		v1alpha1.CompletedState: {v1alpha1.SubmittedState},
	}

	// Valid Executor State Transitions: map with valid paris of oldState-> {newStates}
	validExecutorStateTransitions = map[v1alpha1.ExecutorState][]v1alpha1.ExecutorState{
		"": {v1alpha1.ExecutorUnknownState, v1alpha1.ExecutorRunningState, v1alpha1.ExecutorPendingState,
			v1alpha1.ExecutorFailedState, v1alpha1.ExecutorCompletedState},

		v1alpha1.ExecutorUnknownState: {v1alpha1.ExecutorRunningState, v1alpha1.ExecutorPendingState,
			v1alpha1.ExecutorFailedState, v1alpha1.ExecutorCompletedState},

		v1alpha1.ExecutorPendingState: {v1alpha1.ExecutorUnknownState, v1alpha1.ExecutorRunningState,
			v1alpha1.ExecutorFailedState, v1alpha1.ExecutorCompletedState},

		v1alpha1.ExecutorRunningState: {v1alpha1.ExecutorFailedState, v1alpha1.ExecutorCompletedState},
	}
)

// isValidDriverStatusTransition determines if the driver state transition is valid
func isValidDriverStatusTransition(oldStatus, newStatus v1alpha1.ApplicationStateType) bool {
	for _, validStatus := range validDriverStateTransitions[oldStatus] {
		if newStatus == validStatus {
			return true
		}
	}
	return false
}

// isValidExecutorStatusTransition determines if the executor state transition is valid
func isValidExecutorStatusTransition(oldStatus, newStatus v1alpha1.ExecutorState) bool {
	for _, validStatus := range validExecutorStateTransitions[oldStatus] {
		if newStatus == validStatus {
			return true
		}
	}
	return false
}