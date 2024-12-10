package workaround_plugin

import (
	"fmt"
	"github.com/kubeflow/spark-operator/api/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type AltSparkSubmit struct{}

func (a *AltSparkSubmit) LaunchSparkApplication(app *v1beta2.SparkApplication, cl client.Client) error {
	fmt.Println("Launching spark application")
	// ... custom logic to launch a spark application in the kubernetes spark cluster
	return nil
}

func New() v1beta2.SparkAppLauncher {
	return &AltSparkSubmit{}
}
