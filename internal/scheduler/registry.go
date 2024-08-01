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
	"fmt"
	"sync"
)

var registry *Registry

// Registry is a registry of scheduler factories.
type Registry struct {
	factories map[string]Factory

	mu sync.Mutex
}

func GetRegistry() *Registry {
	if registry == nil {
		registry = &Registry{
			factories: make(map[string]Factory),
		}
	}
	return registry
}

func (r *Registry) GetScheduler(name string, config Config) (Interface, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	factory, exists := r.factories[name]
	if !exists {
		return nil, fmt.Errorf("scheduler %s not found", name)
	}

	return factory(config)
}

// RegisterScheduler registers a scheduler to the manager.
func (r *Registry) Register(name string, factory Factory) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.factories[name]; ok {
		return fmt.Errorf("scheduler %s is already registered", name)
	}

	r.factories[name] = factory
	logger.Info("Registered scheduler", "name", name)
	return nil
}

// GetRegisteredSchedulerNames gets the registered scheduler names.
func (r *Registry) GetRegisteredSchedulerNames() []string {
	var names []string
	for name := range r.factories {
		names = append(names, name)
	}
	return names
}
