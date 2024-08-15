package resourceusage

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/kubeflow/spark-operator/api/v1beta2"
)

func cpuRequest(cores *int32, coreRequest *string) (string, error) {
	if coreRequest != nil {
		if _, err := resource.ParseQuantity(*coreRequest); err != nil {
			return "", fmt.Errorf("failed to parse %s: %w", *coreRequest, err)
		}
		return *coreRequest, nil
	}
	if cores != nil {
		return fmt.Sprintf("%d", *cores), nil
	}
	return "1", nil
}

func DriverPodRequests(app *v1beta2.SparkApplication) (map[string]string, error) {
	cpuValue, err := cpuRequest(app.Spec.Driver.Cores, app.Spec.Driver.CoreRequest)
	if err != nil {
		return nil, err
	}

	memoryValue, err := driverMemoryRequest(app)
	if err != nil {
		return nil, err
	}

	return map[string]string{
		"cpu":    cpuValue,
		"memory": memoryValue,
	}, nil
}

func ExecutorPodRequests(app *v1beta2.SparkApplication) (map[string]string, error) {
	cpuValue, err := cpuRequest(app.Spec.Executor.Cores, app.Spec.Executor.CoreRequest)
	if err != nil {
		return nil, err
	}

	memoryValue, err := executorMemoryRequest(app)
	if err != nil {
		return nil, err
	}

	return map[string]string{
		"cpu":    cpuValue,
		"memory": memoryValue,
	}, nil
}
