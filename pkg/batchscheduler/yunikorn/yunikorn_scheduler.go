package yunikorn

import (
	"github.com/kubeflow/spark-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	schedulerinterface "github.com/kubeflow/spark-operator/pkg/batchscheduler/interface"
	"k8s.io/client-go/rest"
)

func GetPluginName() string {
	return "yunikorn"
}

type YunikornBatchScheduler struct{}

func New(_ *rest.Config) (schedulerinterface.BatchScheduler, error) {
	return &YunikornBatchScheduler{}, nil
}

func (y *YunikornBatchScheduler) Name() string {
	return GetPluginName()
}

func (y *YunikornBatchScheduler) ShouldSchedule(_ *v1beta2.SparkApplication) bool {
	return true
}

func (y *YunikornBatchScheduler) DoBatchSchedulingOnSubmission(app *v1beta2.SparkApplication) error {
	return nil
}

func (y *YunikornBatchScheduler) CleanupOnCompletion(_ *v1beta2.SparkApplication) error {
	return nil
}
