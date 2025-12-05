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
	"fmt"
	"slices"
	"strings"

	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
)

var (
	managerFactory ManagerFactory = defaultManagerFactory

	defaultManagerProduct ManagerProduct = controllerRuntimeManagerProductInstance
	managerProductStore                  = map[string]ManagerProduct{
		"default": controllerRuntimeManagerProductInstance,
	}
	productManuallySet bool
)

// ManagerFactory constructs a controller-runtime manager with the supplied configuration and options.
type ManagerFactory func(cfg *rest.Config, opts ctrl.Options) (ctrl.Manager, error)

// ManagerProduct defines how a controller manager instance is created.
type ManagerProduct interface {
	CreateManager(cfg *rest.Config, opts ctrl.Options) (ctrl.Manager, error)
}

// controllerRuntimeManagerProduct implements the ManagerProduct using controller-runtime's default manager creation.
type controllerRuntimeManagerProduct struct{}

// CreateManager builds a controller manager using controller-runtime defaults.
func (controllerRuntimeManagerProduct) CreateManager(cfg *rest.Config, opts ctrl.Options) (ctrl.Manager, error) {
	return ctrl.NewManager(cfg, opts)
}

var controllerRuntimeManagerProductInstance ManagerProduct = controllerRuntimeManagerProduct{}

// SetDefaultManagerProduct allows overriding the product used by the default manager factory.
func SetDefaultManagerProduct(product ManagerProduct) {
	if product == nil {
		defaultManagerProduct = controllerRuntimeManagerProductInstance
		productManuallySet = false
		return
	}
	defaultManagerProduct = product
	productManuallySet = true
}

// RegisterManagerProduct adds a named manager product that can be selected via configuration.
func RegisterManagerProduct(name string, product ManagerProduct) error {
	if name == "" {
		return fmt.Errorf("manager product name cannot be empty")
	}
	if product == nil {
		return fmt.Errorf("manager product %q cannot be nil", name)
	}
	if _, exists := managerProductStore[name]; exists {
		return fmt.Errorf("manager product %q already registered", name)
	}
	managerProductStore[name] = product
	return nil
}

func applyManagerProduct(name string) error {
	if productManuallySet && (name == "" || name == "default") {
		return nil
	}
	if name == "" || name == "default" {
		defaultManagerProduct = controllerRuntimeManagerProductInstance
		productManuallySet = false
		return nil
	}
	product, exists := managerProductStore[name]
	if !exists {
		return fmt.Errorf("manager product %q not registered (registered products: %s)", name, strings.Join(registeredManagerProductNames(), ", "))
	}
	defaultManagerProduct = product
	productManuallySet = false
	return nil
}

// SetManagerFactory allows callers to override how the manager is constructed.
func SetManagerFactory(factory ManagerFactory) {
	if factory == nil {
		managerFactory = defaultManagerFactory
		return
	}
	managerFactory = factory
}

func defaultManagerFactory(cfg *rest.Config, opts ctrl.Options) (ctrl.Manager, error) {
	return defaultManagerProduct.CreateManager(cfg, opts)
}

func registeredManagerProductNames() []string {
	keys := make([]string, 0, len(managerProductStore))
	for name := range managerProductStore {
		keys = append(keys, name)
	}
	slices.Sort(keys)
	return keys
}
