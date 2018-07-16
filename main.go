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

//go:generate hack/update-codegen.sh

package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/golang/glog"

	apiv1 "k8s.io/api/core/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
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
	"k8s.io/spark-on-k8s-operator/pkg/webhook"
)

var (
	master                  = flag.String("master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	kubeConfig              = flag.String("kubeConfig", "", "Path to a kube config. Only required if out-of-cluster.")
	installCRDs             = flag.Bool("install-crds", true, "Whether to install CRDs")
	controllerThreads       = flag.Int("controller-threads", 10, "Number of worker threads used by the SparkApplication controller.")
	submissionRunnerThreads = flag.Int("submission-threads", 3, "Number of worker threads used by the SparkApplication submission runner.")
	resyncInterval          = flag.Int("resync-interval", 30, "Informer resync interval in seconds")
	namespace               = flag.String("namespace", apiv1.NamespaceAll, "The Kubernetes namespace to manage. Will manage custom resource objects of the managed CRD types for the whole cluster if unset.")
	enableWebhook           = flag.Bool("enable-webhook", false, "Whether to enable the mutating admission webhook for admitting and patching Spark pods")
	webhookCertDir          = flag.String("webhook-cert-dir", "/etc/webhook-certs", "The directory where x509 certificate and key files are stored")
	webhookSvcNamespace     = flag.String("webhook-svc-namespace", "sparkoperator", "The namespace of the Service for the webhook server")
	webhookSvcName          = flag.String("webhook-svc-name", "spark-webhook", "The name of the Service for the webhook server")
	webhookPort             = flag.Int("webhook-port", 8080, "Service port of the webhook server")
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
	if *namespace != apiv1.NamespaceAll {
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

	var hook *webhook.WebHook
	if *enableWebhook {
		var err error
		hook, err = webhook.New(kubeClient, *webhookCertDir, *webhookSvcNamespace, *webhookSvcName, *webhookPort)
		if err != nil {
			glog.Fatal(err)
		}
		if err = hook.Start(); err != nil {
			glog.Fatal(err)
		}
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	<-signalCh

	close(stopCh)

	glog.Info("Shutting down the Spark operator")
	applicationController.Stop()
	scheduledApplicationController.Stop()
	if *enableWebhook {
		if err := hook.Stop(); err != nil {
			glog.Fatal(err)
		}
	}
}

func buildConfig(masterUrl string, kubeConfig string) (*rest.Config, error) {
	if kubeConfig != "" {
		return clientcmd.BuildConfigFromFlags(masterUrl, kubeConfig)
	}
	return rest.InClusterConfig()
}
