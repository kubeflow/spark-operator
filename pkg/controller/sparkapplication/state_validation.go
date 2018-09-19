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
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta1"
)

var (
	// Valid driver state transitions: map with valid pairs of current state -> set of new states.
	validDriverStateTransitions = map[v1beta1.ApplicationStateType][]v1beta1.ApplicationStateType{
		"": {v1beta1.NewState, v1beta1.UnknownState, v1beta1.SubmittedState,
			v1beta1.RunningState, v1beta1.CompletedState, v1beta1.FailedSubmissionState, v1beta1.FailedState},

		v1beta1.NewState: {v1beta1.UnknownState, v1beta1.SubmittedState,
			v1beta1.RunningState, v1beta1.CompletedState, v1beta1.FailedSubmissionState, v1beta1.FailedState},

		v1beta1.UnknownState: {v1beta1.NewState, v1beta1.SubmittedState,
			v1beta1.RunningState, v1beta1.CompletedState, v1beta1.FailedSubmissionState, v1beta1.FailedState},

		v1beta1.FailedSubmissionState: {v1beta1.UnknownState, v1beta1.SubmittedState,
			v1beta1.RunningState, v1beta1.CompletedState, v1beta1.FailedState},

		v1beta1.SubmittedState: {v1beta1.RunningState, v1beta1.CompletedState, v1beta1.FailedState},

		v1beta1.RunningState: {v1beta1.CompletedState, v1beta1.FailedState},

		// The application can be potentially restarted based on the RestartPolicy
		v1beta1.FailedState: {v1beta1.SubmittedState},

		v1beta1.CompletedState: {v1beta1.SubmittedState},
	}

	// Valid executor state transitions: map with valid pairs of current state -> set of new states.
	validExecutorStateTransitions = map[v1beta1.ExecutorState][]v1beta1.ExecutorState{
		"": {v1beta1.ExecutorUnknownState, v1beta1.ExecutorRunningState, v1beta1.ExecutorPendingState,
			v1beta1.ExecutorFailedState, v1beta1.ExecutorCompletedState},

		v1beta1.ExecutorUnknownState: {v1beta1.ExecutorRunningState, v1beta1.ExecutorPendingState,
			v1beta1.ExecutorFailedState, v1beta1.ExecutorCompletedState},

		v1beta1.ExecutorPendingState: {v1beta1.ExecutorUnknownState, v1beta1.ExecutorRunningState,
			v1beta1.ExecutorFailedState, v1beta1.ExecutorCompletedState},

		v1beta1.ExecutorRunningState: {v1beta1.ExecutorFailedState, v1beta1.ExecutorCompletedState},
	}
)

// isValidDriverStateTransition determines if the driver state transition is valid
func isValidDriverStateTransition(oldState, newState v1beta1.ApplicationStateType) bool {
	for _, validState := range validDriverStateTransitions[oldState] {
		if newState == validState {
			return true
		}
	}
	return false
}

// isValidExecutorStateTransition determines if the executor state transition is valid
func isValidExecutorStateTransition(oldState, newState v1beta1.ExecutorState) bool {
	for _, validState := range validExecutorStateTransitions[oldState] {
		if newState == validState {
			return true
		}
	}
	return false
}
