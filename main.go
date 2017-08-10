package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/liyinan926/spark-operator/pkg/controller"
	"github.com/liyinan926/spark-operator/pkg/initializer"

	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	// Uncomment the following line to load the gcp plugin (only required to authenticate against GKE clusters).
	// _ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
)

const (
	SparkRoleLabel    = "spark-role"
	SparkDriverRole   = "driver"
	SparkExecutorRole = "executor"
)

func main() {
	kubeconfig := flag.String("kubeconfig", "", "Path to a kube config. Only required if out-of-cluster.")
	flag.Parse()

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

	appController := controller.NewSparkApplicationController(crdClient, kubeClient)
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	go appController.Run(ctx)

	labelSelector := fmt.Sprintf("%s in (%s, %s)", SparkRoleLabel, SparkDriverRole, SparkExecutorRole)
	initializerController := initializer.NewController(kubeClient, labelSelector)
	go initializerController.Run(1, ctx.Done())
}

func buildConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}
