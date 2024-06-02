package yunikorn

import (
	"k8s.io/client-go/rest"

	"github.com/kubeflow/spark-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	schedulerinterface "github.com/kubeflow/spark-operator/pkg/batchscheduler/interface"
)

type YunikornBatchScheduler struct{}

func New(config *rest.Config) (schedulerinterface.BatchScheduler, error) {
	return &YunikornBatchScheduler{}, nil
}

func (y *YunikornBatchScheduler) ShouldSchedule(app *v1beta2.SparkApplication) bool {
	return true
}

func (y *YunikornBatchScheduler) DoBatchSchedulingOnSubmission(app *v1beta2.SparkApplication) error {
	return nil
}

func (y *YunikornBatchScheduler) CleanupOnCompletion(app *v1beta2.SparkApplication) error {
	return nil
}
