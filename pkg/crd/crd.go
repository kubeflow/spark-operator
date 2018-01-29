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
	"fmt"
	"reflect"
	"time"

	"github.com/golang/glog"

	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/wait"

	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io"
	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
)

// CRD metadata.
const (
	Plural    = "sparkapplications"
	Singular  = "sparkapplication"
	ShortName = "sparkapp"
	Group     = sparkoperator.GroupName
	Version   = "v1alpha1"
	FullName  = Plural + "." + Group
)

// CreateCRD creates a Kubernetes CustomResourceDefinition (CRD) for SparkApplication.
// An error is returned if it fails to create the CustomResourceDefinition before it times out.
func CreateCRD(clientset apiextensionsclient.Interface) error {
	// The CustomResourceDefinition is not found, create it now.
	sparkAppCrd := &apiextensionsv1beta1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: FullName,
		},
		Spec: apiextensionsv1beta1.CustomResourceDefinitionSpec{
			Group:   Group,
			Version: Version,
			Scope:   apiextensionsv1beta1.NamespaceScoped,
			Names: apiextensionsv1beta1.CustomResourceDefinitionNames{
				Plural:     Plural,
				Singular:   Singular,
				ShortNames: []string{ShortName},
				Kind:       reflect.TypeOf(v1alpha1.SparkApplication{}).Name(),
			},
		},
	}
	_, err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Create(sparkAppCrd)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			glog.Warningf("CustomResourceDefinition %s already exists", FullName)
			return nil
		}
		return err
	}

	// Wait for the CustomResourceDefinition to become registered.
	err = waitForCRDEstablishment(clientset)
	// Try deleting the CustomResourceDefinition if it fails to be registered on time.
	if err != nil {
		deleteErr := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Delete(FullName, &metav1.DeleteOptions{})
		if deleteErr != nil {
			return errors.NewAggregate([]error{err, deleteErr})
		}
		return err
	}

	return nil
}

func DeleteCRD(clientset apiextensionsclient.Interface) error {
	var zero int64 = 0
	err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Delete(FullName,
		&metav1.DeleteOptions{GracePeriodSeconds: &zero})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	return nil
}

func getCRD(clientset apiextensionsclient.Interface) (*apiextensionsv1beta1.CustomResourceDefinition, error) {
	return clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Get(FullName, metav1.GetOptions{})
}

// waitForCRDEstablishment waits for the CRD to be registered and established until it times out.
func waitForCRDEstablishment(clientset apiextensionsclient.Interface) error {
	return wait.Poll(500*time.Millisecond, 60*time.Second, func() (bool, error) {
		sparkAppCrd, err := getCRD(clientset)
		for _, cond := range sparkAppCrd.Status.Conditions {
			switch cond.Type {
			case apiextensionsv1beta1.Established:
				if cond.Status == apiextensionsv1beta1.ConditionTrue {
					return true, err
				}
			case apiextensionsv1beta1.NamesAccepted:
				if cond.Status == apiextensionsv1beta1.ConditionFalse {
					fmt.Printf("Name conflict: %v\n", cond.Reason)
				}
			}
		}
		return false, err
	})
}
