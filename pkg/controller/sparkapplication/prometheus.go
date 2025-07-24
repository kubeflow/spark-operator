package sparkapplication

import (
	"github.com/kubeflow/spark-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
)

func (sm *sparkAppMetrics) HandleSparkApplicationCreate(app *v1beta2.SparkApplication) {
	oldApp := &v1beta2.SparkApplication{
		Status: v1beta2.SparkApplicationStatus{
			AppState: v1beta2.ApplicationState{State: v1beta2.NewState},
		},
	}
	sm.exportMetrics(oldApp, app)
}
 
func (sm *sparkAppMetrics) HandleSparkApplicationUpdate(oldApp, newApp *v1beta2.SparkApplication) {
	sm.exportMetrics(oldApp, newApp)
}

func (sm *sparkAppMetrics) HandleSparkApplicationDelete(app *v1beta2.SparkApplication) {
	sm.exportMetricsOnDelete(app)
}