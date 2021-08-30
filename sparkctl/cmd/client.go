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

package cmd

import (
	"context"
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	crdclientset "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
)

func buildConfig(kubeConfig string) (*rest.Config, error) {
	// Check if kubeConfig exist
	if _, err := os.Stat(kubeConfig); os.IsNotExist(err) {
		// Try InClusterConfig for sparkctl running in a pod
		config, err := rest.InClusterConfig()
		if err != nil {
			return nil, err
		}

		return config, nil
	}

	return clientcmd.BuildConfigFromFlags("", kubeConfig)
}

func getKubeClient() (clientset.Interface, error) {
	config, err := buildConfig(KubeConfig)
	if err != nil {
		return nil, err
	}
	return getKubeClientForConfig(config)
}

func getKubeClientForConfig(config *rest.Config) (clientset.Interface, error) {
	return clientset.NewForConfig(config)
}

func getSparkApplicationClient() (crdclientset.Interface, error) {
	config, err := buildConfig(KubeConfig)
	if err != nil {
		return nil, err
	}
	return getSparkApplicationClientForConfig(config)
}

func getSparkApplicationClientForConfig(config *rest.Config) (crdclientset.Interface, error) {
	return crdclientset.NewForConfig(config)
}

func getSparkApplication(name string, crdClientset crdclientset.Interface) (*v1beta2.SparkApplication, error) {
	app, err := crdClientset.SparkoperatorV1beta2().SparkApplications(Namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return app, nil
}
