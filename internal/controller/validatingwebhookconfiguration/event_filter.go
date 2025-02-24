/*
Copyright 2024 The Kubeflow authors.

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

package validatingwebhookconfiguration

import (
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// EventFilter filters events for the ValidatingWebhookConfiguration.
type EventFilter struct {
	name string
}

func NewEventFilter(name string) *EventFilter {
	return &EventFilter{
		name: name,
	}
}

// ValidatingWebhookConfigurationEventFilter implements predicate.Predicate interface.
var _ predicate.Predicate = &EventFilter{}

// Create implements predicate.Predicate.
func (f *EventFilter) Create(e event.CreateEvent) bool {
	return e.Object.GetName() == f.name
}

// Update implements predicate.Predicate.
func (f *EventFilter) Update(e event.UpdateEvent) bool {
	return e.ObjectOld.GetName() == f.name
}

// Delete implements predicate.Predicate.
func (f *EventFilter) Delete(event.DeleteEvent) bool {
	return false
}

// Generic implements predicate.Predicate.
func (f *EventFilter) Generic(event.GenericEvent) bool {
	return false
}
