package schedulerinterface

import (
	"github.com/kubeflow/spark-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
)

type BatchScheduler interface {
	Name() string

	ShouldSchedule(app *v1beta2.SparkApplication) bool
	DoBatchSchedulingOnSubmission(app *v1beta2.SparkApplication) error
	CleanupOnCompletion(app *v1beta2.SparkApplication) error
}
