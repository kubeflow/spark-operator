package util

import (
	"github.com/prometheus/client_golang/prometheus"
	prometheus_model "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"net/http"
	"sync"
	"testing"
)

func fetchCounterValue(m *prometheus.CounterVec, labels map[string]string) float64 {
	// TODO: Revisit this once client_golang offers better testing tools(https://github.com/prometheus/client_golang/issues/422)
	pb := &prometheus_model.Metric{}

	m.With(labels).Write(pb)
	return pb.GetCounter().GetValue()
}

func TestPositiveGauge_EmptyLabels(t *testing.T) {

	gauge := NewPositiveGauge("testGauge", "test-description", []string{})
	emptyMap := map[string]string{}
	gauge.Dec(emptyMap)
	assert.Equal(t, fetchGaugeValue(gauge.gaugeMetric, emptyMap), float64(0))

	gauge.Inc(emptyMap)
	assert.Equal(t, fetchGaugeValue(gauge.gaugeMetric, emptyMap), float64(1))
	gauge.Dec(map[string]string{})
	assert.Equal(t, fetchGaugeValue(gauge.gaugeMetric, emptyMap), float64(0))
}

func TestPositiveGauge_WithLabels(t *testing.T) {

	gauge := NewPositiveGauge("testGauge1", "test-description-1", []string{"app_name"})
	app1 := map[string]string{"app_name": "test1"}
	app2 := map[string]string{"app_name": "test2"}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		for i := 0; i < 10; i++ {
			gauge.Inc(app1)
		}
		for i := 0; i < 5; i++ {
			gauge.Dec(app1)
		}
		wg.Done()
	}()
	go func() {
		for i := 0; i < 5; i++ {
			gauge.Inc(app2)
		}
		for i := 0; i < 10; i++ {
			gauge.Dec(app2)
		}
		wg.Done()
	}()

	wg.Wait()
	assert.Equal(t, float64(5), fetchGaugeValue(gauge.gaugeMetric, app1))
	// Always Positive Gauge.
	assert.Equal(t, float64(0), fetchGaugeValue(gauge.gaugeMetric, app2))
}

func TestPrometheusMetrics(t *testing.T) {
	http.DefaultServeMux = new(http.ServeMux)
	metrics := NewPrometheusMetrics("/metrics-test", ":10254", "", []string{"app_name"})
	app1 := map[string]string{"app_name": "test1"}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for i := 0; i < 10; i++ {
			metrics.SparkAppSubmitCount.With(app1).Inc()
			metrics.SparkAppRunningCount.Inc(app1)
			metrics.SparkAppFailureCount.With(app1).Inc()
			metrics.SparkAppSuccessCount.With(app1).Inc()
			metrics.SparkAppExecutorFailureCount.With(app1).Inc()
			metrics.SparkAppSuccessExecutionTime.With(app1).Observe(float64(100 * i))
			metrics.SparkAppFailureExecutionTime.With(app1).Observe(float64(500 * i))
			metrics.SparkAppExecutorRunningCount.Inc(app1)
			metrics.SparkAppExecutorSuccessCount.With(app1).Inc()

		}
		for i := 0; i < 5; i++ {
			metrics.SparkAppRunningCount.Dec(app1)
			metrics.SparkAppExecutorRunningCount.Dec(app1)
		}
		wg.Done()
	}()

	wg.Wait()
	assert.Equal(t, float64(10), fetchCounterValue(metrics.SparkAppSubmitCount, app1))
	assert.Equal(t, float64(10), fetchCounterValue(metrics.SparkAppFailureCount, app1))
	assert.Equal(t, float64(10), fetchCounterValue(metrics.SparkAppSuccessCount, app1))
	assert.Equal(t, float64(10), fetchCounterValue(metrics.SparkAppExecutorFailureCount, app1))
	assert.Equal(t, float64(10), fetchCounterValue(metrics.SparkAppExecutorSuccessCount, app1))
	assert.Equal(t, float64(5), fetchGaugeValue(metrics.SparkAppExecutorRunningCount.gaugeMetric, app1))
	assert.Equal(t, float64(5), fetchGaugeValue(metrics.SparkAppRunningCount.gaugeMetric, app1))

}
