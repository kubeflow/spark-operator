package yunikorn

import (
	"k8s.io/client-go/rest"

	"github.com/apache/yunikorn-k8shim/pkg/cache"

	"github.com/kubeflow/spark-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	schedulerinterface "github.com/kubeflow/spark-operator/pkg/batchscheduler/interface"
)

const (
	DriverTaskGroupName   = "spark-driver"
	ExecutorTaskGroupName = "spark-executor"
)

type YunikornBatchScheduler struct{}

func New(config *rest.Config) (schedulerinterface.BatchScheduler, error) {
	return &YunikornBatchScheduler{}, nil
}

func (y *YunikornBatchScheduler) ShouldSchedule(app *v1beta2.SparkApplication) bool {
	return true
}

func (y *YunikornBatchScheduler) CleanupOnCompletion(app *v1beta2.SparkApplication) error {
	return nil
}

func (y *YunikornBatchScheduler) DoBatchSchedulingOnSubmission(app *v1beta2.SparkApplication) error {
	taskGroups := []cache.TaskGroup{
		{
			Name:         DriverTaskGroupName,
			MinMember:    1,
			MinResource:  nil,
			NodeSelector: app.Spec.Driver.NodeSelector,
			Tolerations:  app.Spec.Driver.Tolerations,
			Affinity:     app.Spec.Driver.Affinity,
			Labels:       app.Spec.Driver.Labels,
		},
	}

	if initialExecutors := getInitialExecutors(app); initialExecutors > 0 {
		taskGroups = append(taskGroups, cache.TaskGroup{
			Name:         ExecutorTaskGroupName,
			MinMember:    initialExecutors,
			MinResource:  nil,
			NodeSelector: app.Spec.Executor.NodeSelector,
			Tolerations:  app.Spec.Executor.Tolerations,
			Affinity:     app.Spec.Executor.Affinity,
			Labels:       app.Spec.Executor.Labels,
		})
	}

	return nil
}

func getInitialExecutors(app *v1beta2.SparkApplication) int32 {
	// https://github.com/apache/spark/blob/bc187013da821eba0ffff2408991e8ec6d2749fe/core/src/main/scala/org/apache/spark/util/Utils.scala#L2539-L2542
	initialExecutors := int32(0)

	if app.Spec.Executor.Instances != nil {
		initialExecutors = max(initialExecutors, *app.Spec.Executor.Instances)
	}

	if app.Spec.DynamicAllocation != nil {
		if app.Spec.DynamicAllocation.MinExecutors != nil {
			initialExecutors = max(initialExecutors, *app.Spec.DynamicAllocation.MinExecutors)
		}
		if app.Spec.DynamicAllocation.InitialExecutors != nil {
			initialExecutors = max(initialExecutors, *app.Spec.DynamicAllocation.InitialExecutors)
		}
	}

	return initialExecutors
}
