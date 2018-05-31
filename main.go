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
	"time"

	"github.com/golang/glog"

	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/clock"
	clientset "k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	crdclientset "k8s.io/spark-on-k8s-operator/pkg/client/clientset/versioned"
	crdinformers "k8s.io/spark-on-k8s-operator/pkg/client/informers/externalversions"
	"k8s.io/spark-on-k8s-operator/pkg/controller/scheduledsparkapplication"
	"k8s.io/spark-on-k8s-operator/pkg/controller/sparkapplication"
	"k8s.io/spark-on-k8s-operator/pkg/crd"
	ssacrd "k8s.io/spark-on-k8s-operator/pkg/crd/scheduledsparkapplication"
	sacrd "k8s.io/spark-on-k8s-operator/pkg/crd/sparkapplication"
	"k8s.io/spark-on-k8s-operator/pkg/initializer"
)

var (
	master = flag.String("master", "", "The address of the Kubernetes API server. "+
		"Overrides any value in kubeconfig. Only required if out-of-cluster.")
	kubeConfig = flag.String("kubeConfig", "", "Path to a kube config. Only required if "+
		"out-of-cluster.")
	enableInitializer = flag.Bool("enable-initializer", true, "Whether to enable the "+
		"Spark pod initializer.")
	installCRDs        = flag.Bool("install-crds", true, "Whether to install CRDs")
	initializerThreads = flag.Int("initializer-threads", 10, "Number of worker threads "+
		"used by the Spark Pod initializer (if it's enabled).")
	controllerThreads = flag.Int("controller-threads", 10, "Number of worker threads "+
		"used by the SparkApplication controller.")
	submissionRunnerThreads = flag.Int("submission-threads", 3, "Number of worker threads "+
		"used by the SparkApplication submission runner.")
	resyncInterval = flag.Int("resync-interval", 30, "Informer resync interval in seconds")
	namespace = flag.String("namespace", "", "The Kubernetes namespace to manage. "+
		"Will manage CRDs for the whole cluster if unset.")
)

func main() {
	flag.Parse()

	// Create the client config. Use kubeConfig if given, otherwise assume in-cluster.
	config, err := buildConfig(*master, *kubeConfig)
	if err != nil {
		glog.Fatal(err)
	}
	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		glog.Fatal(err)
	}

	glog.Info("Checking the kube-dns add-on")
	if err = checkKubeDNS(kubeClient); err != nil {
		glog.Fatal(err)
	}

	glog.Info("Starting the Spark operator")

	stopCh := make(chan struct{})

	crdClient, err := crdclientset.NewForConfig(config)
	if err != nil {
		glog.Fatal(err)
	}
	apiExtensionsClient, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		glog.Fatal(err)
	}

	if *installCRDs {
		err = crd.CreateOrUpdateCRD(apiExtensionsClient, sacrd.GetCRD())
		if err != nil {
			glog.Fatalf("failed to create or update CustomResourceDefinition %s: %v", sacrd.FullName, err)
		}

		err = crd.CreateOrUpdateCRD(apiExtensionsClient, ssacrd.GetCRD())
		if err != nil {
			glog.Fatalf("failed to create or update CustomResourceDefinition %s: %v", ssacrd.FullName, err)
		}
	}

	var factoryOpts []crdinformers.SharedInformerOption
	if *namespace != "" {
		factoryOpts = append(factoryOpts, crdinformers.WithNamespace(*namespace))
	}
	factory := crdinformers.NewSharedInformerFactoryWithOptions(
		crdClient,
		// resyncPeriod. Every resyncPeriod, all resources in the cache will re-trigger events.
		time.Duration(*resyncInterval)*time.Second,
		factoryOpts...)
	applicationController := sparkapplication.NewController(
		crdClient, kubeClient, apiExtensionsClient, factory, *submissionRunnerThreads, *namespace)
	scheduledApplicationController := scheduledsparkapplication.NewController(
		crdClient, kubeClient, apiExtensionsClient, factory, clock.RealClock{})

	// Start the informer factory that in turn starts the informer.
	go factory.Start(stopCh)
	if err = applicationController.Start(*controllerThreads, stopCh); err != nil {
		glog.Fatal(err)
	}
	if err = scheduledApplicationController.Start(*controllerThreads, stopCh); err != nil {
		glog.Fatal(err)
	}

	var sparkPodInitializer *initializer.SparkPodInitializer
	if *enableInitializer {
		sparkPodInitializer = initializer.New(kubeClient)
		if err = sparkPodInitializer.Start(*initializerThreads, stopCh); err != nil {
			glog.Fatal(err)
		}
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	<-signalCh

	// This causes the workers of the initializer and SparkApplication controller to stop.
	close(stopCh)

	glog.Info("Shutting down the Spark operator")
	applicationController.Stop()
	scheduledApplicationController.Stop()
	if *enableInitializer {
		sparkPodInitializer.Stop()
	}
}

func buildConfig(masterUrl string, kubeConfig string) (*rest.Config, error) {
	if kubeConfig != "" {
		return clientcmd.BuildConfigFromFlags(masterUrl, kubeConfig)
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
