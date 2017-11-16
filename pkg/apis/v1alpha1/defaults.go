package v1alpha1

func SetDefaults_SparkApplication(app *SparkApplication) {
	if app == nil {
		return
	}

	if app.Spec.Mode == "" {
		app.Spec.Mode = ClusterMode
	}

	setDriverSpecDefaults(app.Spec.Driver)
	setExecutorSpecDefaults(app.Spec.Executor)
}

func setDriverSpecDefaults(spec DriverSpec) {
	if spec.Cores == nil {
		oneCore := "1"
		spec.Cores = &oneCore
	}
	if spec.Memory == nil {
		oneGMemory := "1g"
		spec.Memory = &oneGMemory
	}
}

func setExecutorSpecDefaults(spec ExecutorSpec) {
	if spec.Cores == nil {
		oneCore := "1"
		spec.Cores = &oneCore
	}
	if spec.Memory == nil {
		oneGMemory := "1g"
		spec.Memory = &oneGMemory
	}
	if spec.Instances == nil {
		one := int32(1)
		spec.Instances = &one
	}
}
