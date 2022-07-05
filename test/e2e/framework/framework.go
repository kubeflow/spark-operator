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
	"context"
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
	SparkTestNamespace     *v1.Namespace
	OperatorPod            *v1.Pod
	DefaultTimeout         time.Duration
}

var SparkTestNamespace = ""
var SparkTestServiceAccount = ""
var SparkTestImage = ""

// Sets up a test framework and returns it.
func New(ns, sparkNs, kubeconfig, opImage string, opImagePullPolicy string) (*Framework, error) {
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

	sparkTestNamespace, err := CreateNamespace(cli, sparkNs)
	if err != nil {
		fmt.Println(nil, err, sparkNs)
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
		SparkTestNamespace:     sparkTestNamespace,
		DefaultTimeout:         time.Minute,
	}

	err = f.Setup(sparkNs, opImage, opImagePullPolicy)
	if err != nil {
		return nil, errors.Wrap(err, "setup test environment failed")
	}

	return f, nil
}

func (f *Framework) Setup(sparkNs, opImage string, opImagePullPolicy string) error {
	if err := f.setupOperator(sparkNs, opImage, opImagePullPolicy); err != nil {
		return errors.Wrap(err, "setup operator failed")
	}

	return nil
}

func (f *Framework) setupOperator(sparkNs, opImage string, opImagePullPolicy string) error {
	if _, err := CreateServiceAccount(f.KubeClient, f.Namespace.Name, "../../manifest/spark-operator-install/spark-operator-rbac.yaml"); err != nil && !apierrors.IsAlreadyExists(err) {
		return errors.Wrap(err, "failed to create operator service account")
	}

	if err := CreateClusterRole(f.KubeClient, "../../manifest/spark-operator-install/spark-operator-rbac.yaml"); err != nil && !apierrors.IsAlreadyExists(err) {
		return errors.Wrap(err, "failed to create cluster role")
	}

	if _, err := CreateClusterRoleBinding(f.KubeClient, "../../manifest/spark-operator-install/spark-operator-rbac.yaml"); err != nil && !apierrors.IsAlreadyExists(err) {
		return errors.Wrap(err, "failed to create cluster role binding")
	}

	if _, err := CreateServiceAccount(f.KubeClient, f.SparkTestNamespace.Name, "../../manifest/spark-application-rbac/spark-application-rbac.yaml"); err != nil && !apierrors.IsAlreadyExists(err) {
		return errors.Wrap(err, "failed to create Spark service account")
	}

	if err := CreateRole(f.KubeClient, f.SparkTestNamespace.Name, "../../manifest/spark-application-rbac/spark-application-rbac.yaml"); err != nil && !apierrors.IsAlreadyExists(err) {
		return errors.Wrap(err, "failed to create role")
	}

	if _, err := CreateRoleBinding(f.KubeClient, f.SparkTestNamespace.Name, "../../manifest/spark-application-rbac/spark-application-rbac.yaml"); err != nil && !apierrors.IsAlreadyExists(err) {
		return errors.Wrap(err, "failed to create role binding")
	}

	job, err := MakeJob("../../manifest/spark-operator-with-webhook-install/spark-operator-webhook.yaml")
	if err != nil {
		return err
	}

	if opImage != "" {
		// Override operator image used, if specified when running tests.
		job.Spec.Template.Spec.Containers[0].Image = opImage
	}

	for _, container := range job.Spec.Template.Spec.Containers {
		container.ImagePullPolicy = v1.PullPolicy(opImagePullPolicy)
	}

	err = CreateJob(f.KubeClient, f.Namespace.Name, job)
	if err != nil {
		return errors.Wrap(err, "failed to create job that creates the webhook secret")
	}

	err = WaitUntilJobCompleted(f.KubeClient, f.Namespace.Name, job.Name, time.Minute)
	if err != nil {
		return errors.Wrap(err, "The gencert job failed or timed out")
	}

	if err := DeleteJob(f.KubeClient, f.Namespace.Name, job.Name); err != nil {
		return errors.Wrap(err, "failed to delete the init job")
	}

	if _, err := CreateService(f.KubeClient, f.Namespace.Name, "../../manifest/spark-operator-with-webhook-install/spark-operator-webhook.yaml"); err != nil {
		return errors.Wrap(err, "failed to create webhook service")
	}

	deploy, err := MakeDeployment("../../manifest/spark-operator-with-webhook-install/spark-operator-with-webhook.yaml")
	if err != nil {
		return err
	}

	if opImage != "" {
		// Override operator image used, if specified when running tests.
		deploy.Spec.Template.Spec.Containers[0].Image = opImage
	}

	for _, container := range deploy.Spec.Template.Spec.Containers {
		container.ImagePullPolicy = v1.PullPolicy(opImagePullPolicy)
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

	pl, err := f.KubeClient.CoreV1().Pods(f.Namespace.Name).List(context.TODO(), opts)
	if err != nil {
		return err
	}
	f.OperatorPod = &pl.Items[0]
	return nil
}

// Teardown tears down a previously initialized test environment.
func (f *Framework) Teardown() error {
	if err := DeleteClusterRole(f.KubeClient, "../../manifest/spark-operator-install/spark-operator-rbac.yaml"); err != nil && !apierrors.IsAlreadyExists(err) {
		return errors.Wrap(err, "failed to delete operator cluster role")
	}

	if err := DeleteClusterRoleBinding(f.KubeClient, "../../manifest/spark-operator-install/spark-operator-rbac.yaml"); err != nil && !apierrors.IsAlreadyExists(err) {
		return errors.Wrap(err, "failed to delete operator cluster role binding")
	}

	if err := f.KubeClient.AppsV1().Deployments(f.Namespace.Name).Delete(context.TODO(), "sparkoperator", metav1.DeleteOptions{}); err != nil {
		return err
	}

	if err := DeleteNamespace(f.KubeClient, f.Namespace.Name); err != nil {
		return err
	}

	if err := DeleteNamespace(f.KubeClient, f.SparkTestNamespace.Name); err != nil {
		return err
	}

	return nil
}
