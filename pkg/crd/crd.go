/*
Copyright 2018 Google LLC

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
)

func CreateOrUpdateCRD(
	clientset apiextensionsclient.Interface,
	definition *apiextensionsv1beta1.CustomResourceDefinition) error {
	existing, err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Get(definition.Name,
		metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		// Failed to get the CRD object and the failure was not because the object cannot be found.
		return err
	}

	if err == nil && existing != nil {
		// Update case.
		if !reflect.DeepEqual(existing.Spec, definition.Spec) {
			existing.Spec = definition.Spec
			glog.Infof("Updating CustomResourceDefinition %s", definition.Name)
			if _, err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Update(existing); err != nil {
				return err
			}
		}
	} else {
		// Create case.
		glog.Infof("Creating CustomResourceDefinition %s", definition.Name)
		if _, err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Create(definition); err != nil {
			return err
		}
	}

	// Wait for the CustomResourceDefinition to become registered.
	err = waitForCRDEstablishment(clientset, definition.Name)
	// Try deleting the CustomResourceDefinition if it fails to be registered on time.
	if err != nil {
		deleteErr := deleteCRD(clientset, definition.Name)
		if deleteErr != nil {
			return errors.NewAggregate([]error{err, deleteErr})
		}
		return err
	}

	return nil
}

// waitForCRDEstablishment waits for the CRD to be registered and established until it times out.
func waitForCRDEstablishment(clientset apiextensionsclient.Interface, name string) error {
	return wait.Poll(500*time.Millisecond, 60*time.Second, func() (bool, error) {
		crd, err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Get(name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for _, cond := range crd.Status.Conditions {
			switch cond.Type {
			case apiextensionsv1beta1.Established:
				if cond.Status == apiextensionsv1beta1.ConditionTrue {
					return true, nil
				}
			case apiextensionsv1beta1.NamesAccepted:
				if cond.Status == apiextensionsv1beta1.ConditionFalse {
					fmt.Printf("Name conflict: %v\n", cond.Reason)
				}
			}
		}
		return false, nil
	})
}

func deleteCRD(clientset apiextensionsclient.Interface, name string) error {
	err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Delete(name, metav1.NewDeleteOptions(0))
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	return nil
}
