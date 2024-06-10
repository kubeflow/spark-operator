package yunikorn

import (
	"encoding/json"
	"fmt"
	"k8s.io/client-go/rest"

	"github.com/apache/yunikorn-k8shim/pkg/cache"

	"github.com/kubeflow/spark-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	schedulerinterface "github.com/kubeflow/spark-operator/pkg/batchscheduler/interface"
)

const (
	DriverTaskGroupName   = "spark-driver"
	ExecutorTaskGroupName = "spark-executor"

	TaskGroupNameAnnotation = "yunikorn.apache.org/task-group-name"
	TaskGroupsAnnotation    = "yunikorn.apache.org/task-groups"

	QueueLabel = "queue"
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
			MinResource:  nil, // TODO
			NodeSelector: mergeMaps(app.Spec.NodeSelector, app.Spec.Driver.NodeSelector),
			Tolerations:  app.Spec.Driver.Tolerations,
			Affinity:     app.Spec.Driver.Affinity,
			Labels:       app.Spec.Driver.Labels,
		},
	}

	// A minMember of zero is not a valid config for a Yunikorn task group,
	// so we should leave out the executor task group completely
	// if the initial number of executors is zero
	initialExecutors := getInitialExecutors(app)
	if initialExecutors > 0 {
		taskGroups = append(taskGroups, cache.TaskGroup{
			Name:         ExecutorTaskGroupName,
			MinMember:    initialExecutors,
			MinResource:  nil, // TODO
			NodeSelector: mergeMaps(app.Spec.NodeSelector, app.Spec.Executor.NodeSelector),
			Tolerations:  app.Spec.Executor.Tolerations,
			Affinity:     app.Spec.Executor.Affinity,
			Labels:       app.Spec.Executor.Labels,
		})
	}

	if err := addTaskGroupAnnotations(app, taskGroups); err != nil {
		return fmt.Errorf("failed to add task group annotations: %v", err)
	}

	addQueueLabels(app)

	return nil
}

func getInitialExecutors(app *v1beta2.SparkApplication) int32 {
	// Logic copied from https://github.com/apache/spark/blob/bc187013da821eba0ffff2408991e8ec6d2749fe/core/src/main/scala/org/apache/spark/util/Utils.scala#L2539-L2542
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

func addTaskGroupAnnotations(app *v1beta2.SparkApplication, taskGroups []cache.TaskGroup) error {
	marshalledTaskGroups, err := json.Marshal(taskGroups)
	if err != nil {
		return fmt.Errorf("failed to marshal taskGroups: %v", err)
	}

	if app.Spec.Driver.Annotations == nil {
		app.Spec.Driver.Annotations = make(map[string]string)
	}

	if app.Spec.Executor.Annotations == nil {
		app.Spec.Executor.Annotations = make(map[string]string)
	}

	app.Spec.Driver.Annotations[TaskGroupNameAnnotation] = DriverTaskGroupName
	app.Spec.Executor.Annotations[TaskGroupNameAnnotation] = ExecutorTaskGroupName

	app.Spec.Driver.Annotations[TaskGroupsAnnotation] = string(marshalledTaskGroups)

	return nil
}

func addQueueLabels(app *v1beta2.SparkApplication) {
	if app.Spec.BatchSchedulerOptions != nil && app.Spec.BatchSchedulerOptions.Queue != nil {
		if app.Spec.Driver.Labels == nil {
			app.Spec.Driver.Labels = make(map[string]string)
		}

		if app.Spec.Executor.Labels == nil {
			app.Spec.Executor.Labels = make(map[string]string)
		}

		app.Spec.Driver.Labels[QueueLabel] = *app.Spec.BatchSchedulerOptions.Queue
		app.Spec.Executor.Labels[QueueLabel] = *app.Spec.BatchSchedulerOptions.Queue
	}
}
