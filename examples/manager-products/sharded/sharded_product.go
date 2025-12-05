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

package sharded

import (
	shardlease "github.com/timebertt/kubernetes-controller-sharding/pkg/shard/lease"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"

	sparkcontroller "github.com/kubeflow/spark-operator/v2/cmd/operator/controller"
)

func init() {
	// Register the sharded product so the operator can pick it up via --manager-product=sharded.
	sparkcontroller.RegisterManagerProduct("sharded", Product{})
}

// Product implements controller.ManagerProduct and configures a shard lease-backed manager.
type Product struct{}

// CreateManager prepares controller-runtime options for sharded operation and returns the manager.
func (Product) CreateManager(cfg *rest.Config, opts ctrl.Options) (ctrl.Manager, error) {
	shardLease, err := shardlease.NewResourceLock(cfg, shardlease.Options{
		ControllerRingName: "spark-operator-ring",
	})
	if err != nil {
		return nil, err
	}

	opts.LeaderElection = true
	opts.LeaderElectionResourceLockInterface = shardLease
	opts.LeaderElectionReleaseOnCancel = true

	return ctrl.NewManager(cfg, opts)
}
