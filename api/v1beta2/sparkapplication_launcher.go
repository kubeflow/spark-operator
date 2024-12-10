package v1beta2

import (
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type SparkAppLauncher interface {
	LaunchSparkApplication(app *SparkApplication, cl client.Client) error
}
