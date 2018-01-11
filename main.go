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

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/golang/glog"

	"k8s.io/spark-on-k8s-operator/pkg/controller"
	"k8s.io/spark-on-k8s-operator/pkg/crd"
	"k8s.io/spark-on-k8s-operator/pkg/initializer"

	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	kubeConfig := flag.String("kubeConfig", "", "Path to a kube config. Only required if out-of-cluster.")
	initializerThreads := flag.Int("initializer-threads", 10, "Number of worker threads used by the initializer controller.")
	submissionRunnerThreads := flag.Int("submission-threads", 3, "Number of worker threads used by the submission runner.")
	flag.Parse()

	// Create the client config. Use kubeConfig if given, otherwise assume in-cluster.
	config, err := buildConfig(*kubeConfig)
	if err != nil {
		panic(err)
	}
	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	glog.Info("Checking the kube-dns add-on")
	if err = checkKubeDNS(kubeClient); err != nil {
		return
	}

	glog.Info("Starting the Spark operator")

	initializerController := initializer.New(kubeClient)
	stopCh := make(chan struct{})
	errCh := make(chan error, 1)
	go initializerController.Run(*initializerThreads, stopCh, errCh)

	crdClient, err := crd.NewClient(config)
	if err != nil {
		panic(err)
	}
	apiExtensionsClient, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	sparkApplicationController := controller.New(crdClient, kubeClient, apiExtensionsClient, *submissionRunnerThreads)
	go sparkApplicationController.Run(stopCh, errCh)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-signalCh:
		break
	case err = <-errCh:
		if err != nil {
			glog.Errorf("Spark operator failed with error: %v", err)
		}
	}

	glog.Info("Shutting down the Spark operator")
	// This causes the custom controller and initializer to stop.
	close(stopCh)
}

func buildConfig(kubeConfig string) (*rest.Config, error) {
	if kubeConfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeConfig)
	}
	return rest.InClusterConfig()
}

func checkKubeDNS(kubeClient clientset.Interface) error {
	endpoints, err := kubeClient.CoreV1().Endpoints("kube-system").Get("kube-dns", metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			glog.Error("no endpoints for kube-dns found in namespace kube-system")
		} else {
			glog.Errorf("failed to get endpoints for kube-dns in namespace kube-system: %v", err)
		}
		glog.Error("cluster add-on kube-dns is required to run Spark applications")
		return err
	}

	if len(endpoints.Subsets) == 0 {
		glog.Error("cluster add-on kube-dns is required to run Spark applications")
		return fmt.Errorf("no endpoints for kube-dns available in namespace kube-system")
	}

	return nil
}
