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
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/golang/glog"
	apiv1 "k8s.io/api/core/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	crclientset "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
	crinformers "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/informers/externalversions"
	operatorConfig "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/controller/scheduledsparkapplication"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/controller/sparkapplication"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/crd"
	ssacrd "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/crd/scheduledsparkapplication"
	sacrd "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/crd/sparkapplication"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/util"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/webhook"
)

var (
	master              = flag.String("master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	kubeConfig          = flag.String("kubeConfig", "", "Path to a kube config. Only required if out-of-cluster.")
	installCRDs         = flag.Bool("install-crds", true, "Whether to install CRDs")
	controllerThreads   = flag.Int("controller-threads", 10, "Number of worker threads used by the SparkApplication controller.")
	resyncInterval      = flag.Int("resync-interval", 30, "Informer resync interval in seconds.")
	namespace           = flag.String("namespace", apiv1.NamespaceAll, "The Kubernetes namespace to manage. Will manage custom resource objects of the managed CRD types for the whole cluster if unset.")
	enableWebhook       = flag.Bool("enable-webhook", false, "Whether to enable the mutating admission webhook for admitting and patching Spark pods.")
	webhookConfigName   = flag.String("webhook-config-name", "spark-webhook-config", "The name of the MutatingWebhookConfiguration object to create.")
	webhookCertDir      = flag.String("webhook-cert-dir", "/etc/webhook-certs", "The directory where x509 certificate and key files are stored.")
	webhookSvcNamespace = flag.String("webhook-svc-namespace", "spark-operator", "The namespace of the Service for the webhook server.")
	webhookSvcName      = flag.String("webhook-svc-name", "spark-webhook", "The name of the Service for the webhook server.")
	webhookPort         = flag.Int("webhook-port", 8080, "Service port of the webhook server.")
	enableMetrics       = flag.Bool("enable-metrics", false, "Whether to enable the metrics endpoint.")
	metricsPort         = flag.String("metrics-port", "10254", "Port for the metrics endpoint.")
	metricsEndpoint     = flag.String("metrics-endpoint", "/metrics", "Metrics endpoint.")
	metricsPrefix       = flag.String("metrics-prefix", "", "Prefix for the metrics.")
	ingressUrlFormat    = flag.String("ingress-url-format", "", "Ingress URL format.")
)

func main() {
	var metricsLabels util.ArrayFlags
	flag.Var(&metricsLabels, "metrics-labels", "Labels for the metrics")
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

	var metricConfig *util.MetricConfig
	if *enableMetrics {
		metricConfig = &util.MetricConfig{
			MetricsEndpoint: *metricsEndpoint,
			MetricsPort:     *metricsPort,
			MetricsPrefix:   *metricsPrefix,
			MetricsLabels:   metricsLabels,
		}

		glog.Info("Enabling metrics collecting and exporting to Prometheus")
		util.InitializeMetrics(metricConfig)
	}

	glog.Info("Starting the Spark Operator")

	stopCh := make(chan struct{})

	crClient, err := crclientset.NewForConfig(config)
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

	crInformerFactory := buildCustomResourceInformerFactory(crClient)
	podInformerFactory := buildPodInformerFactory(kubeClient)
	applicationController := sparkapplication.NewController(
		crClient, kubeClient, crInformerFactory, podInformerFactory, metricConfig, *namespace, *ingressUrlFormat)
	scheduledApplicationController := scheduledsparkapplication.NewController(
		crClient, kubeClient, apiExtensionsClient, crInformerFactory, clock.RealClock{})

	// Start the informer factory that in turn starts the informer.
	go crInformerFactory.Start(stopCh)
	go podInformerFactory.Start(stopCh)

	if err = applicationController.Start(*controllerThreads, stopCh); err != nil {
		glog.Fatal(err)
	}
	if err = scheduledApplicationController.Start(*controllerThreads, stopCh); err != nil {
		glog.Fatal(err)
	}

	var hook *webhook.WebHook
	if *enableWebhook {
		var err error
		hook, err = webhook.New(kubeClient, crInformerFactory, *webhookCertDir, *webhookSvcNamespace, *webhookSvcName, *webhookPort, *namespace)
		if err != nil {
			glog.Fatal(err)
		}

		if err = hook.Start(*webhookConfigName); err != nil {
			glog.Fatal(err)
		}
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	<-signalCh

	close(stopCh)

	glog.Info("Shutting down the Spark Operator")
	applicationController.Stop()
	scheduledApplicationController.Stop()
	if *enableWebhook {
		if err := hook.Stop(*webhookConfigName); err != nil {
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

func buildCustomResourceInformerFactory(crClient crclientset.Interface) crinformers.SharedInformerFactory {
	var factoryOpts []crinformers.SharedInformerOption
	if *namespace != apiv1.NamespaceAll {
		factoryOpts = append(factoryOpts, crinformers.WithNamespace(*namespace))
	}
	return crinformers.NewSharedInformerFactoryWithOptions(
		crClient,
		// resyncPeriod. Every resyncPeriod, all resources in the cache will re-trigger events.
		time.Duration(*resyncInterval)*time.Second,
		factoryOpts...)
}

func buildPodInformerFactory(kubeClient clientset.Interface) informers.SharedInformerFactory {
	var podFactoryOpts []informers.SharedInformerOption
	if *namespace != apiv1.NamespaceAll {
		podFactoryOpts = append(podFactoryOpts, informers.WithNamespace(*namespace))
	}
	tweakListOptionsFunc := func(options *metav1.ListOptions) {
		options.LabelSelector = fmt.Sprintf("%s,%s", operatorConfig.SparkRoleLabel, operatorConfig.LaunchedBySparkOperatorLabel)
	}
	podFactoryOpts = append(podFactoryOpts, informers.WithTweakListOptions(tweakListOptionsFunc))
	return informers.NewSharedInformerFactoryWithOptions(kubeClient, time.Duration(*resyncInterval)*time.Second, podFactoryOpts...)
}
