package crd

import (
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/liyinan926/spark-operator/pkg/apis/v1alpha1"

	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/wait"
)

// CRD metadata.
const (
	CRDPlural   = "sparkapps"
	CRDSingular = "sparkapp"
	CRDKind     = "SparkApp"
	CRDGroup    = v1alpha1.GroupName
	CRDVersion  = "v1alpha1"
	CRDFullName = CRDPlural + "." + CRDGroup
)

// CreateCRD creates a Kubernetes CustomResourceDefinition (CRD) for SparkApplication.
// An error is returned if it fails to create the CustomResourceDefinition before it times out.
func CreateCRD(clientset apiextensionsclient.Interface) error {
	// The CustomResourceDefinition is not found, create it now.
	crd := &apiextensionsv1beta1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: CRDFullName,
		},
		Spec: apiextensionsv1beta1.CustomResourceDefinitionSpec{
			Group:   CRDGroup,
			Version: CRDVersion,
			Scope:   apiextensionsv1beta1.NamespaceScoped,
			Names: apiextensionsv1beta1.CustomResourceDefinitionNames{
				Plural: CRDPlural,
				Kind:   reflect.TypeOf(v1alpha1.SparkApplication{}).Name(),
			},
		},
	}
	_, err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Create(crd)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return err
	}

	// Wait for the CustomResourceDefinition to become registered.
	err = waitForCRDEstablishment(clientset)
	// Try deleting the CustomResourceDefinition if it fails to be registered on time.
	if err != nil {
		deleteErr := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Delete(CRDFullName, &metav1.DeleteOptions{})
		if deleteErr != nil {
			return errors.NewAggregate([]error{err, deleteErr})
		}
		return err
	}

	return nil
}

func getCRD(clientset apiextensionsclient.Interface) (*apiextensionsv1beta1.CustomResourceDefinition, error) {
	return clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Get(CRDFullName, metav1.GetOptions{})
}

// waitForCRDEstablishment waits for the CRD to be registered and established until it times out.
func waitForCRDEstablishment(clientset apiextensionsclient.Interface) error {
	return wait.Poll(500*time.Millisecond, 60*time.Second, func() (bool, error) {
		crd, err := getCRD(clientset)
		for _, cond := range crd.Status.Conditions {
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
