/*
Copyright 2019 Google LLC

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

package scheduler

import (
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/kubeflow/spark-operator/api/v1beta2"
)

var (
	logger = log.Log.WithName("")
)

// Interface defines the interface of a batch scheduler.
type Interface interface {
	Name() string
	ShouldSchedule(app *v1beta2.SparkApplication) bool
	Schedule(app *v1beta2.SparkApplication) error
	Cleanup(app *v1beta2.SparkApplication) error
}

// Config defines the configuration of a batch scheduler.
type Config interface{}

// Factory defines the factory of a batch scheduler.
type Factory func(config Config) (Interface, error)
