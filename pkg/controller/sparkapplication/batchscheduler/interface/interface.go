package schedulerinterface

import "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta1"

type BatchScheduler interface {
	Name() string

	ShouldSchedule(app *v1beta1.SparkApplication) bool

	OnSubmitSparkApplication(app *v1beta1.SparkApplication) (*v1beta1.SparkApplication, error)
	OnSparkDriverPodScheduled(app *v1beta1.SparkApplication) (*v1beta1.SparkApplication, error)
}
