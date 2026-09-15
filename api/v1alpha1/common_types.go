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

// DeployMode describes the type of deployment of a Spark application.
type DeployMode string

// Different types of deployments.
const (
	DeployModeCluster DeployMode = "cluster"
	DeployModeClient  DeployMode = "client"
)

// DriverState tells the current state of a spark driver.
type DriverState string

// Different states a spark driver may have.
const (
	DriverStatePending   DriverState = "PENDING"
	DriverStateRunning   DriverState = "RUNNING"
	DriverStateCompleted DriverState = "COMPLETED"
	DriverStateFailed    DriverState = "FAILED"
	DriverStateUnknown   DriverState = "UNKNOWN"
)

// ExecutorState tells the current state of an executor.
type ExecutorState string

// Different states an executor may have.
const (
	ExecutorStatePending   ExecutorState = "PENDING"
	ExecutorStateRunning   ExecutorState = "RUNNING"
	ExecutorStateCompleted ExecutorState = "COMPLETED"
	ExecutorStateFailed    ExecutorState = "FAILED"
	ExecutorStateUnknown   ExecutorState = "UNKNOWN"
)

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
	// Archives is a list of archives to be extracted into the working directory of each executor.
	// +optional
	Archives []string `json:"archives,omitempty"`
}

// DynamicAllocation contains configuration options for dynamic allocation.
type DynamicAllocation struct {
	// Enabled controls whether dynamic allocation is enabled or not.
	// +optional
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

	// ShuffleTrackingEnabled enables shuffle file tracking for executors, which allows dynamic allocation without
	// the need for an external shuffle service. This option will try to keep alive executors that are storing
	// shuffle data for active jobs. If external shuffle service is enabled, set ShuffleTrackingEnabled to false.
	// ShuffleTrackingEnabled is true by default if dynamicAllocation.enabled is true.
	// +optional
	ShuffleTrackingEnabled *bool `json:"shuffleTrackingEnabled,omitempty"`

	// ShuffleTrackingTimeout controls the timeout in milliseconds for executors that are holding
	// shuffle data if shuffle tracking is enabled (true by default if dynamic allocation is enabled).
	// +optional
	ShuffleTrackingTimeout *int64 `json:"shuffleTrackingTimeout,omitempty"`
}
