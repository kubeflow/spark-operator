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
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
)

var (
	// Valid driver state transitions: map with valid pairs of current state -> set of new states.
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

		v1alpha1.RunningState: {v1alpha1.CompletedState, v1alpha1.FailedState},

		// The application can be potentially restarted based on the RestartPolicy
		v1alpha1.FailedState: {v1alpha1.SubmittedState},

		v1alpha1.CompletedState: {v1alpha1.SubmittedState},
	}

	// Valid executor state transitions: map with valid pairs of current state -> set of new states.
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

// isValidDriverStateTransition determines if the driver state transition is valid
func isValidDriverStateTransition(oldState, newState v1alpha1.ApplicationStateType) bool {
	for _, validState := range validDriverStateTransitions[oldState] {
		if newState == validState {
			return true
		}
	}
	return false
}

// isValidExecutorStateTransition determines if the executor state transition is valid
func isValidExecutorStateTransition(oldState, newState v1alpha1.ExecutorState) bool {
	for _, validState := range validExecutorStateTransitions[oldState] {
		if newState == validState {
			return true
		}
	}
	return false
}
