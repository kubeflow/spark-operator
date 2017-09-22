package crd

import (
	"github.com/liyinan926/spark-operator/pkg/apis/v1alpha1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/rest"
)

// Client is the client for talking to the API server for CRUD operations on the SparkApplication custom resource objects.
type Client struct {
	Client *rest.RESTClient
	Scheme *runtime.Scheme
}

// SchemeGroupVersion is the group version used to register the SparkApplication custom resource objects.
var schemeGroupVersion = schema.GroupVersion{Group: CRDGroup, Version: CRDVersion}

// NewClient creates a new client for the CRD.
func NewClient(cfg *rest.Config) (*Client, error) {
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
	return &Client{Client: client, Scheme: scheme}, nil
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

// Create creates a new SparkApplication through an HTTP POST request.
func (c *Client) Create(app *v1alpha1.SparkApplication) (*v1alpha1.SparkApplication, error) {
	var result v1alpha1.SparkApplication
	err := c.Client.Post().
		Namespace(app.Namespace).
		Name(app.Name).
		Resource(CRDPlural).
		Body(app).
		Do().
		Into(&result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// Delete deletes an existing SparkApplication through an HTTP DELETE request.
func (c *Client) Delete(name string, namespace string, options metav1.DeleteOptions) error {
	return c.Client.Delete().
		Namespace(namespace).
		Name(name).
		Resource(CRDPlural).
		Body(options).
		Do().
		Error()
}

// Update updates an existing SparkApplication through an HTTP PUT request.
func (c *Client) Update(app *v1alpha1.SparkApplication) (*v1alpha1.SparkApplication, error) {
	var result v1alpha1.SparkApplication
	err := c.Client.Put().
		Namespace(app.Namespace).
		Name(app.Name).
		Resource(CRDPlural).
		Body(app).
		Do().
		Into(&result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// Get gets a SparkApplication through an HTTP GET request.
func (c *Client) Get(name string, namespace string, options metav1.GetOptions) (*v1alpha1.SparkApplication, error) {
	var result v1alpha1.SparkApplication
	err := c.Client.Get().
		Namespace(namespace).
		Name(name).
		Resource(CRDPlural).
		Body(options).
		Do().
		Into(&result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}
