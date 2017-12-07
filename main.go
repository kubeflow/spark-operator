package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/golang/glog"

	"github.com/liyinan926/spark-operator/pkg/controller"
	"github.com/liyinan926/spark-operator/pkg/crd"
	"github.com/liyinan926/spark-operator/pkg/initializer"

	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	clientset "k8s.io/client-go/kubernetes"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/errors"
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
	errCh := make(chan error)
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
	<-signalCh

	glog.Info("Shutting down the Spark operator")
	// This causes the custom controller and initializer to stop.
	close(stopCh)

	err = <-errCh
	if err != nil {
		glog.Errorf("Spark operator failed with error: %v", err)
	}
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
