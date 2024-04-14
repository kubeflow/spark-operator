package v1beta1

// SetSparkApplicationDefaults sets default values for certain fields of a SparkApplication.
func SetSparkApplicationDefaults(app *SparkApplication) {
	if app == nil {
		return
	}

	if app.Spec.Mode == "" {
		app.Spec.Mode = ClusterMode
	}

	if app.Spec.RestartPolicy.Type == "" {
		app.Spec.RestartPolicy.Type = Never
	}

	if app.Spec.RestartPolicy.Type != Never {
		// Default to 5 sec if the RestartPolicy is OnFailure or Always and these values aren't specified.
		if app.Spec.RestartPolicy.OnFailureRetryInterval == nil {
			app.Spec.RestartPolicy.OnFailureRetryInterval = new(int64)
			*app.Spec.RestartPolicy.OnFailureRetryInterval = 5
		}

		if app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval == nil {
			app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval = new(int64)
			*app.Spec.RestartPolicy.OnSubmissionFailureRetryInterval = 5
		}
	}

	setDriverSpecDefaults(&app.Spec.Driver, app.Spec.SparkConf)
	setExecutorSpecDefaults(&app.Spec.Executor, app.Spec.SparkConf)
}

func setDriverSpecDefaults(spec *DriverSpec, sparkConf map[string]string) {
	if _, exists := sparkConf["spark.driver.cores"]; !exists && spec.Cores == nil {
		spec.Cores = new(float32)
		*spec.Cores = 1
	}
	if _, exists := sparkConf["spark.driver.memory"]; !exists && spec.Memory == nil {
		spec.Memory = new(string)
		*spec.Memory = "1g"
	}
}

func setExecutorSpecDefaults(spec *ExecutorSpec, sparkConf map[string]string) {
	if _, exists := sparkConf["spark.executor.cores"]; !exists && spec.Cores == nil {
		spec.Cores = new(float32)
		*spec.Cores = 1
	}
	if _, exists := sparkConf["spark.executor.memory"]; !exists && spec.Memory == nil {
		spec.Memory = new(string)
		*spec.Memory = "1g"
	}
	if _, exists := sparkConf["spark.executor.instances"]; !exists && spec.Instances == nil {
		spec.Instances = new(int32)
		*spec.Instances = 1
	}
}
