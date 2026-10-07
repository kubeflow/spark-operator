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

package mutatingwebhookconfiguration

import (
	"testing"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func webhookConfig(name string) *admissionregistrationv1.MutatingWebhookConfiguration {
	return &admissionregistrationv1.MutatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func TestEventFilterCreate(t *testing.T) {
	f := NewEventFilter("watched")

	if !f.Create(event.CreateEvent{Object: webhookConfig("watched")}) {
		t.Error("expected Create to return true for the watched name")
	}
	if f.Create(event.CreateEvent{Object: webhookConfig("other")}) {
		t.Error("expected Create to return false for a different name")
	}
}

func TestEventFilterUpdate(t *testing.T) {
	f := NewEventFilter("watched")

	if !f.Update(event.UpdateEvent{ObjectOld: webhookConfig("watched"), ObjectNew: webhookConfig("watched")}) {
		t.Error("expected Update to return true when the old object has the watched name")
	}
	if f.Update(event.UpdateEvent{ObjectOld: webhookConfig("other"), ObjectNew: webhookConfig("watched")}) {
		t.Error("expected Update to return false when the old object has a different name")
	}
}

func TestEventFilterDeleteAndGeneric(t *testing.T) {
	f := NewEventFilter("watched")

	if f.Delete(event.DeleteEvent{Object: webhookConfig("watched")}) {
		t.Error("expected Delete to always return false")
	}
	if f.Generic(event.GenericEvent{Object: webhookConfig("watched")}) {
		t.Error("expected Generic to always return false")
	}
}
