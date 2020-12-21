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

var schedulerContainers = map[string]schedulerInitializeFunc{
	volcano.GetPluginName(): volcano.New,
}

func GetRegisteredNames() []string {
	var pluginNames []string
	for key := range schedulerContainers {
		pluginNames = append(pluginNames, key)
	}
	return pluginNames
}

type SchedulerManager struct {
	sync.Mutex
	config  *rest.Config
	plugins map[string]schedulerinterface.BatchScheduler
}

func NewSchedulerManager(config *rest.Config) *SchedulerManager {
	manager := SchedulerManager{
		config:  config,
		plugins: make(map[string]schedulerinterface.BatchScheduler),
	}
	return &manager
}

func (batch *SchedulerManager) GetScheduler(schedulerName string) (schedulerinterface.BatchScheduler, error) {
	iniFunc, registered := schedulerContainers[schedulerName]
	if !registered {
		return nil, fmt.Errorf("unregistered scheduler plugin %s", schedulerName)
	}

	batch.Lock()
	defer batch.Unlock()

	if plugin, existed := batch.plugins[schedulerName]; existed && plugin != nil {
		return plugin, nil
	} else if existed && plugin == nil {
		return nil, fmt.Errorf(
			"failed to get scheduler plugin %s, previous initialization has failed", schedulerName)
	} else {
		if plugin, err := iniFunc(batch.config); err != nil {
			batch.plugins[schedulerName] = nil
			return nil, err
		} else {
			batch.plugins[schedulerName] = plugin
			return plugin, nil
		}
	}
}
