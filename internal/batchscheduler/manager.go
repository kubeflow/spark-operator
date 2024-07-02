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
)

type SchedulerManager struct {
	schedulers map[string]Interface

	mu sync.Mutex
}

var schedulerManager *SchedulerManager

func NewSchedulerManager() *SchedulerManager {
	if schedulerManager == nil {
		schedulerManager = &SchedulerManager{
			schedulers: make(map[string]Interface),
		}
	}
	return schedulerManager
}

func (m *SchedulerManager) GetScheduler(name string) (Interface, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	scheduler := m.schedulers[name]
	if scheduler == nil {
		return nil, fmt.Errorf("scheduler %s not found", name)
	}
	return scheduler, nil
}

// RegisterScheduler registers a scheduler to the manager.
func (m *SchedulerManager) RegisterScheduler(name string, scheduler Interface) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if scheduler == nil {
		return fmt.Errorf("scheduler %s is nil", name)
	}

	if _, ok := m.schedulers[name]; ok {
		return fmt.Errorf("scheduler %s is already registered", name)
	}

	m.schedulers[name] = scheduler

	return nil
}

// GetRegisteredSchedulerNames gets the registered scheduler names.
func (m *SchedulerManager) GetRegisteredSchedulerNames() []string {
	var names []string
	for name := range m.schedulers {
		names = append(names, name)
	}
	return names
}
