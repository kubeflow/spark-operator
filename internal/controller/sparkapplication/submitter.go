package sparkapplication

import (
	"fmt"

	"github.com/kubeflow/spark-operator/api/v1beta2"
)

type sparkSubmitter interface {
	submit(*v1beta2.SparkApplication) error
}

type sparkSubmitterHandler struct {
}

func newSparkSubmitterHandler() sparkSubmitter {
	return &sparkSubmitterHandler{}
}

func (s *sparkSubmitterHandler) submit(app *v1beta2.SparkApplication) error {
	sparkSubmitArgs, err := buildSparkSubmitArgs(app)
	if err != nil {
		return fmt.Errorf("failed to build spark-submit arguments: %v", err)
	}

	// Try submitting the application by running spark-submit.
	logger.Info("Running spark-submit for SparkApplication", "name", app.Name, "namespace", app.Namespace, "arguments", sparkSubmitArgs)
	return runSparkSubmit(newSubmission(sparkSubmitArgs, app))
}
