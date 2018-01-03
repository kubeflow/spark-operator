/*
Copyright 2017 Google LLC

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

package crd

import (
	"sync"

	"k8s.io/spark-on-k8s-operator/pkg/apis/v1alpha1"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/rest/fake"
)

type fakeClientImpl struct {
	restClient rest.Interface
	sparkApps  sync.Map
}

// NewFakeClient returns a fake client implementation for testing.
func NewFakeClient() ClientInterface {
	return &fakeClientImpl{
		restClient: &fake.RESTClient{},
	}
}

func (f *fakeClientImpl) RESTClient() rest.Interface {
	return f.restClient
}

func (f *fakeClientImpl) Create(app *v1alpha1.SparkApplication) (*v1alpha1.SparkApplication, error) {
	f.sparkApps.Store(app.Name, app)
	return app, nil
}

func (f *fakeClientImpl) Delete(name string, namespace string) error {
	f.sparkApps.Delete(name)
	return nil
}

func (f *fakeClientImpl) Get(name string, namespace string) (*v1alpha1.SparkApplication, error) {
	app, ok := f.sparkApps.Load(name)
	if !ok {
		return nil, nil
	}
	return app.(*v1alpha1.SparkApplication), nil
}

func (f *fakeClientImpl) Update(app *v1alpha1.SparkApplication) (*v1alpha1.SparkApplication, error) {
	f.sparkApps.Store(app.Name, app)
	return app, nil
}

func (f *fakeClientImpl) UpdateStatus(app *v1alpha1.SparkApplication) (*v1alpha1.SparkApplication, error) {
	f.sparkApps.Store(app.Name, app)
	return app, nil
}
