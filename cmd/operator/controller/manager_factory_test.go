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

package controller

import (
	"errors"
	"strings"
	"testing"

	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
)

var errDummyFactory = errors.New("dummy factory invoked")

func dummyFactory(_ *rest.Config, _ ctrl.Options) (ctrl.Manager, error) {
	return nil, errDummyFactory
}

func resetManagerFactoryState() {
	managerFactory = defaultManagerFactory
	defaultManagerProduct = controllerRuntimeManagerProductInstance
	managerProductStore = map[string]ManagerProduct{
		"default": controllerRuntimeManagerProductInstance,
	}
	productManuallySet = false
}

func TestSetManagerFactoryOverride(t *testing.T) {
	resetManagerFactoryState()
	defer resetManagerFactoryState()

	SetManagerFactory(dummyFactory)

	_, err := managerFactory(nil, ctrl.Options{})
	if !errors.Is(err, errDummyFactory) {
		t.Fatalf("expected managerFactory to use custom implementation, got err: %v", err)
	}

	SetManagerFactory(nil)

	if managerFactory == nil {
		t.Fatalf("managerFactory should reset to default, got nil")
	}
}

func TestApplyManagerProductUsesRegisteredProduct(t *testing.T) {
	resetManagerFactoryState()
	defer resetManagerFactoryState()

	stub := &stubManagerProduct{err: errDummyFactory}
	if err := RegisterManagerProduct("custom", stub); err != nil {
		t.Fatalf("RegisterManagerProduct returned error: %v", err)
	}

	if err := applyManagerProduct("custom"); err != nil {
		t.Fatalf("applyManagerProduct returned error: %v", err)
	}

	if productManuallySet {
		t.Fatalf("expected productManuallySet to be false after apply")
	}

	_, err := defaultManagerProduct.CreateManager(nil, ctrl.Options{})
	if !errors.Is(err, errDummyFactory) {
		t.Fatalf("expected defaultManagerProduct to use custom product, got err: %v", err)
	}

	if !stub.called {
		t.Fatalf("expected custom product to be invoked")
	}
}

func TestApplyManagerProductRespectsManualOverride(t *testing.T) {
	resetManagerFactoryState()
	defer resetManagerFactoryState()

	stub := &stubManagerProduct{err: errDummyFactory}
	SetDefaultManagerProduct(stub)

	if err := applyManagerProduct("default"); err != nil {
		t.Fatalf("applyManagerProduct returned error: %v", err)
	}

	if !productManuallySet {
		t.Fatalf("expected manual product flag to remain true")
	}

	_, err := defaultManagerProduct.CreateManager(nil, ctrl.Options{})
	if !errors.Is(err, errDummyFactory) {
		t.Fatalf("expected manually set product to remain active, got err: %v", err)
	}

	if !stub.called {
		t.Fatalf("expected manually set product to be invoked")
	}
}

func TestApplyManagerProductUnknownIncludesRegistered(t *testing.T) {
	resetManagerFactoryState()
	defer resetManagerFactoryState()

	err := applyManagerProduct("missing")
	if err == nil {
		t.Fatalf("expected error for missing product")
	}
	if !strings.Contains(err.Error(), "registered products") {
		t.Fatalf("expected error to list registered products, got %v", err)
	}
}

type stubManagerProduct struct {
	err    error
	called bool
}

func (s *stubManagerProduct) CreateManager(_ *rest.Config, _ ctrl.Options) (ctrl.Manager, error) {
	s.called = true
	return nil, s.err
}

func TestDefaultManagerFactoryUsesProduct(t *testing.T) {
	resetManagerFactoryState()
	defer resetManagerFactoryState()

	stub := &stubManagerProduct{err: errDummyFactory}
	SetDefaultManagerProduct(stub)

	_, err := defaultManagerFactory(nil, ctrl.Options{})
	if !errors.Is(err, errDummyFactory) {
		t.Fatalf("expected default factory to propagate product error, got %v", err)
	}

	if !stub.called {
		t.Fatalf("expected stub product to be invoked")
	}
}
