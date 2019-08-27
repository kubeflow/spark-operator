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
	"github.com/golang/glog"
	"sync"

	"k8s.io/client-go/rest"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta1"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/batchscheduler/interface"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/batchscheduler/volcano"
)

type SchedulerManager interface {
	ShouldSchedule(app *v1beta1.SparkApplication) bool
	DoBatchSchedulingOnSubmission(app *v1beta1.SparkApplication) (*v1beta1.SparkApplication, error)
}

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

type SchedulerManagerImpl struct {
	sync.Mutex
	config  *rest.Config
	plugins map[string]schedulerinterface.BatchScheduler
}

func NewSchedulerManager(config *rest.Config) SchedulerManager {
	manager := SchedulerManagerImpl{
		config:  config,
		plugins: make(map[string]schedulerinterface.BatchScheduler),
	}
	return &manager
}

func (batch *SchedulerManagerImpl) ShouldSchedule(app *v1beta1.SparkApplication) bool {
	if app.Spec.BatchScheduler == nil || *app.Spec.BatchScheduler == "" {
		return false
	}
	plugin, err := batch.prepareScheduler(app)
	if err == nil {
		return plugin.ShouldSchedule(app)
	} else {
		glog.Errorf("failed to get batch scheduler %s for scheduling", err)
		return false
	}
}

func (batch *SchedulerManagerImpl) DoBatchSchedulingOnSubmission(app *v1beta1.SparkApplication) (*v1beta1.SparkApplication, error) {
	plugin, err := batch.prepareScheduler(app)
	if err == nil {
		return plugin.DoBatchSchedulingOnSubmission(app)
	} else {
		return nil, err
	}
}

func (batch *SchedulerManagerImpl) prepareScheduler(app *v1beta1.SparkApplication) (schedulerinterface.BatchScheduler, error) {
	iniFunc, registered := schedulerContainers[*app.Spec.BatchScheduler]
	if !registered {
		return nil, fmt.Errorf("unregistered scheduler plugin %s", *app.Spec.BatchScheduler)
	}

	batch.Lock()
	defer batch.Unlock()

	if plugin, existed := batch.plugins[*app.Spec.BatchScheduler]; existed && plugin != nil {
		return plugin, nil
	} else if existed && plugin == nil {
		return nil, fmt.Errorf(
			"failed to get scheduler plugin %s, previous initialization has failed", *app.Spec.BatchScheduler)
	} else {
		if plugin, err := iniFunc(batch.config); err != nil {
			batch.plugins[*app.Spec.BatchScheduler] = nil
			return nil, err
		} else {
			batch.plugins[*app.Spec.BatchScheduler] = plugin
			return plugin, nil
		}
	}
}
