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
	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/rest"
)

// ClientInterface is the interface for client implementations for talking to the API server for
// CRUD operations on the SparkApplication custom resource objects.
type ClientInterface interface {
	RESTClient() rest.Interface
	Create(app *v1alpha1.SparkApplication) (*v1alpha1.SparkApplication, error)
	Delete(name string, namespace string) error
	Get(name string, namespace string) (*v1alpha1.SparkApplication, error)
	Update(app *v1alpha1.SparkApplication) (*v1alpha1.SparkApplication, error)
	UpdateStatus(app *v1alpha1.SparkApplication) (*v1alpha1.SparkApplication, error)
}

// clientImpl is a real implementation of ClientInterface for talking to the API server for CRUD
// operations on the SparkApplication custom resource objects.
type clientImpl struct {
	restClient rest.Interface
}

// SchemeGroupVersion is the group version used to register the SparkApplication custom resource objects.
var schemeGroupVersion = schema.GroupVersion{Group: Group, Version: Version}

// NewClient creates a new real client for the SparkApplication CRD.
func NewClient(cfg *rest.Config) (ClientInterface, error) {
	scheme := runtime.NewScheme()
	schemaBuilder := runtime.NewSchemeBuilder(addKnownTypes)
	if err := schemaBuilder.AddToScheme(scheme); err != nil {
		return nil, err
	}

	config := *cfg
	config.GroupVersion = &schemeGroupVersion
	config.APIPath = "/apis"
	config.ContentType = runtime.ContentTypeJSON
	config.NegotiatedSerializer = serializer.DirectCodecFactory{
		CodecFactory: serializer.NewCodecFactory(scheme)}

	client, err := rest.RESTClientFor(&config)
	if err != nil {
		return nil, err
	}
	return &clientImpl{restClient: client}, nil
}

// addKnownTypes adds the set of types defined in this package to the supplied scheme.
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(schemeGroupVersion,
		&v1alpha1.SparkApplication{},
		&v1alpha1.SparkApplicationList{},
	)
	metav1.AddToGroupVersion(scheme, schemeGroupVersion)
	return nil
}

func (c *clientImpl) RESTClient() rest.Interface {
	return c.restClient
}

// Create creates a new SparkApplication through an HTTP POST request.
func (c *clientImpl) Create(app *v1alpha1.SparkApplication) (*v1alpha1.SparkApplication, error) {
	var result v1alpha1.SparkApplication
	err := c.restClient.Post().
		Namespace(app.Namespace).
		Name(app.Name).
		Resource(Plural).
		Body(app).
		Do().
		Into(&result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// Delete deletes an existing SparkApplication through an HTTP DELETE request.
func (c *clientImpl) Delete(name string, namespace string) error {
	return c.restClient.Delete().
		Namespace(namespace).
		Name(name).
		Resource(Plural).
		Do().
		Error()
}

// Update updates an existing SparkApplication through an HTTP PUT request.
func (c *clientImpl) Update(app *v1alpha1.SparkApplication) (*v1alpha1.SparkApplication, error) {
	var result v1alpha1.SparkApplication
	err := c.restClient.Put().
		Namespace(app.Namespace).
		Name(app.Name).
		Resource(Plural).
		Body(app).
		Do().
		Into(&result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// UpdateStatus updates the status of an existing SparkApplication through an HTTP PUT request.
func (c *clientImpl) UpdateStatus(app *v1alpha1.SparkApplication) (*v1alpha1.SparkApplication, error) {
	var result v1alpha1.SparkApplication
	err := c.restClient.Put().
		Namespace(app.Namespace).
		Name(app.Name).
		Resource(Plural).
		SubResource("status").
		Body(app).
		Do().
		Into(&result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// Get gets a SparkApplication through an HTTP GET request.
func (c *clientImpl) Get(name string, namespace string) (*v1alpha1.SparkApplication, error) {
	var result v1alpha1.SparkApplication
	err := c.restClient.Get().
		Namespace(namespace).
		Name(name).
		Resource(Plural).
		Do().
		Into(&result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}
