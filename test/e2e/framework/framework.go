/*
Copyright 2018 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package framework

import (
	"fmt"
	"time"

	crdclientset "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/pkg/errors"
)

// Framework contains all components required to run the test framework.
type Framework struct {
	KubeClient             kubernetes.Interface
	SparkApplicationClient crdclientset.Interface
	MasterHost             string
	Namespace              *v1.Namespace
	OperatorPod            *v1.Pod
	DefaultTimeout         time.Duration
}

// Sets up a test framework and returns it.
func New(ns, kubeconfig, opImage string) (*Framework, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, errors.Wrap(err, "build config from flags failed")
	}

	cli, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "creating new kube-client failed")
	}

	namespace, err := CreateNamespace(cli, ns)
	if err != nil {
		fmt.Println(nil, err, namespace)
	}

	saClient, err := crdclientset.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create SparkApplication client")
	}

	f := &Framework{
		MasterHost:             config.Host,
		KubeClient:             cli,
		SparkApplicationClient: saClient,
		Namespace:              namespace,
		DefaultTimeout:         time.Minute,
	}

	err = f.Setup(opImage)
	if err != nil {
		return nil, errors.Wrap(err, "setup test environment failed")
	}

	return f, nil
}

func (f *Framework) Setup(opImage string) error {
	if err := f.setupOperator(opImage); err != nil {
		return errors.Wrap(err, "setup operator failed")
	}

	return nil
}

func (f *Framework) setupOperator(opImage string) error {
	if _, err := CreateServiceAccount(f.KubeClient, f.Namespace.Name, "../../manifest/spark-operator-rbac.yaml"); err != nil && !apierrors.IsAlreadyExists(err) {
		return errors.Wrap(err, "failed to create operator service account")
	}

	if err := CreateClusterRole(f.KubeClient, "../../manifest/spark-operator-rbac.yaml"); err != nil && !apierrors.IsAlreadyExists(err) {
		return errors.Wrap(err, "failed to create cluster role")
	}

	if _, err := CreateClusterRoleBinding(f.KubeClient, f.Namespace.Name, "../../manifest/spark-operator-rbac.yaml"); err != nil && !apierrors.IsAlreadyExists(err) {
		return errors.Wrap(err, "failed to create cluster role binding")
	}

	deploy, err := MakeDeployment("../../manifest/spark-operator.yaml")
	if err != nil {
		return err
	}

	if opImage != "" {
		// Override operator image used, if specified when running tests.
		deploy.Spec.Template.Spec.Containers[0].Image = opImage
	}

	err = CreateDeployment(f.KubeClient, f.Namespace.Name, deploy)
	if err != nil {
		return err
	}

	opts := metav1.ListOptions{LabelSelector: fields.SelectorFromSet(fields.Set(deploy.Spec.Template.ObjectMeta.Labels)).String()}
	err = WaitForPodsReady(f.KubeClient, f.Namespace.Name, f.DefaultTimeout, 1, opts)
	if err != nil {
		return errors.Wrap(err, "failed to wait for operator to become ready")
	}

	pl, err := f.KubeClient.CoreV1().Pods(f.Namespace.Name).List(opts)
	if err != nil {
		return err
	}
	f.OperatorPod = &pl.Items[0]
	return nil
}

// Teardown tears down a previously initialized test environment.
func (f *Framework) Teardown() error {
	if err := DeleteClusterRole(f.KubeClient, "../../manifest/spark-operator-rbac.yaml"); err != nil && !apierrors.IsAlreadyExists(err) {
		return errors.Wrap(err, "failed to delete operator cluster role")
	}

	if err := DeleteClusterRoleBinding(f.KubeClient, "../../manifest/spark-operator-rbac.yaml"); err != nil && !apierrors.IsAlreadyExists(err) {
		return errors.Wrap(err, "failed to delete operator cluster role binding")
	}

	if err := f.KubeClient.AppsV1().Deployments(f.Namespace.Name).Delete("sparkoperator", nil); err != nil {
		return err
	}

	if err := DeleteNamespace(f.KubeClient, f.Namespace.Name); err != nil {
		return err
	}

	return nil
}
