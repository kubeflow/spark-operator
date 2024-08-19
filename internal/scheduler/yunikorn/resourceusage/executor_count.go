package resourceusage

import "github.com/kubeflow/spark-operator/api/v1beta2"

func NumInitialExecutors(app *v1beta2.SparkApplication) int32 {
	initialExecutors := int32(0)

	// Take the max of these three fields while guarding against nil pointers
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
