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

package batchscheduler

import (
	"fmt"
	"sync"

	"k8s.io/client-go/rest"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/batchscheduler/interface"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/batchscheduler/volcano"
)

type schedulerInitializeFunc func(config *rest.Config) (schedulerinterface.BatchScheduler, error)

var (
	manageMutex         sync.Mutex
	schedulerContainers map[string]schedulerInitializeFunc
)

func init() {
	schedulerContainers = make(map[string]schedulerInitializeFunc)
	registerBatchScheduler(volcano.GetPluginName(), volcano.New)
}

func registerBatchScheduler(name string, iniFunc schedulerInitializeFunc) {
	manageMutex.Lock()
	defer manageMutex.Unlock()
	schedulerContainers[name] = iniFunc
}

func GetBatchScheduler(name string, config *rest.Config) (schedulerinterface.BatchScheduler, error) {
	manageMutex.Lock()
	defer manageMutex.Unlock()
	for n, fc := range schedulerContainers {
		if n == name {
			return fc(config)
		}
	}
	return nil, fmt.Errorf("failed to find batch scheduler named with %s", name)
}
