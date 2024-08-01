package yunikorn

import "github.com/kubeflow/spark-operator/api/v1beta2"

func getInitialExecutors(app *v1beta2.SparkApplication) int32 {
	// Take the max of the number of executors and both the initial and minimum number of executors from
	// dynamic allocation. See the upstream Spark code below for reference.
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

func driverPodResourceUsage(_ *v1beta2.SparkApplication) (map[string]string, error) {
	return nil, nil
}

func executorPodResourceUsage(_ *v1beta2.SparkApplication) (map[string]string, error) {
	return nil, nil
}
