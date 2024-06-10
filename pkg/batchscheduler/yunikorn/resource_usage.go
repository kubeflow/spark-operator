package yunikorn

import (
	"github.com/kubeflow/spark-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"k8s.io/apimachinery/pkg/api/resource"
)

func driverResourceUsage(app *v1beta2.SparkApplication) (map[string]resource.Quantity, error) {
	// TODO
	return nil, nil
}

func executorResourceUsage(app *v1beta2.SparkApplication) (map[string]resource.Quantity, error) {
	// TODO
	return nil, nil
}
