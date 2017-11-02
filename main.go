package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/golang/glog"

	"github.com/liyinan926/spark-operator/pkg/controller"
	"github.com/liyinan926/spark-operator/pkg/crd"
	"github.com/liyinan926/spark-operator/pkg/initializer"

	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	clientset "k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	kubeconfig := flag.String("kubeconfig", "", "Path to a kube config. Only required if out-of-cluster.")
	initializerThreads := flag.Int("initializer-threads", 10, "Number of worker threads used by the initializer controller.")
	submissionRunnerThreads := flag.Int("", 3, "Number of worker threads used by the submission runner.")
	flag.Parse()

	glog.Info("Starting the Spark operator...")

	// Create the client config. Use kubeconfig if given, otherwise assume in-cluster.
	config, err := buildConfig(*kubeconfig)
	if err != nil {
		panic(err)
	}

	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		panic(err)
	}
	crdClient, err := crd.NewClient(config)
	if err != nil {
		panic(err)
	}
	apiExtensionsClient, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	stopCh := make(chan struct{})
	errCh := make(chan error)

	initializerController := initializer.NewController(kubeClient)
	go initializerController.Run(*initializerThreads, stopCh, errCh)

	sparkApplicationController := controller.NewSparkApplicationController(crdClient, kubeClient, apiExtensionsClient, *submissionRunnerThreads)
	go sparkApplicationController.Run(stopCh, errCh)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	<-signalCh

	glog.Info("Shutting down the Spark operator...")
	// This causes the custom controller and initializer to stop.
	close(stopCh)

	err = <-errCh
	if err != nil {
		glog.Errorf("Spark operator failed with error: %v", err)
	}
}

func buildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}
