package sparkapplication

import "github.com/kubeflow/spark-operator/internal/metrics"

// Options defines the options of the controller.
type Options struct {
	Namespaces            []string
	EnableUIService       bool
	IngressClassName      string
	IngressURLFormat      string
	DefaultBatchScheduler string

	KubeSchedulerNames []string

	SparkApplicationMetrics *metrics.SparkApplicationMetrics
	SparkExecutorMetrics    *metrics.SparkExecutorMetrics

	MaxTrackedExecutorPerApp int

	sparkSubmitter sparkSubmitter
}

func (o *Options) getSparkSubmitter() sparkSubmitter {
	if o.sparkSubmitter == nil {
		o.sparkSubmitter = newSparkSubmitterHandler()
	}
	return o.sparkSubmitter
}
