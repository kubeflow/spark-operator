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

	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	// Uncomment the following line to load the gcp plugin (only required to authenticate against GKE clusters).
	// _ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
)

func main() {
	kubeconfig := flag.String("kubeconfig", "", "Path to a kube config. Only required if out-of-cluster.")
	initializerThreads := flag.Int("initializer-threads", 10, "Number of worker threads in the initializer controller.")
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

	stopCh := make(chan struct{})

	appController := controller.NewSparkApplicationController(crdClient, kubeClient)
	go appController.Run(stopCh)

	initializerController := initializer.NewController(kubeClient)
	go initializerController.Run(*initializerThreads, stopCh)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	<-signalCh

	glog.Info("Shutting down the Spark operator...")
	// This causes the custom controller and initializer to stop.
	close(stopCh)
}

func buildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}
