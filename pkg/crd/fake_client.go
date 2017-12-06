package crd

import (
	"sync"

	"github.com/liyinan926/spark-operator/pkg/apis/v1alpha1"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/rest/fake"
)

type fakeClientImpl struct {
	restClient rest.Interface
	sparkApps sync.Map
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

