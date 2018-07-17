package util

import (
	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	prometheus_model "github.com/prometheus/client_model/go"
	"k8s.io/client-go/util/workqueue"
	"net/http"
	"strings"
	"sync"
)

type PrometheusMetrics struct {
	Labels               []string
	prefix               string
	SparkAppSubmitCount  *prometheus.CounterVec
	SparkAppSuccessCount *prometheus.CounterVec
	SparkAppFailureCount *prometheus.CounterVec
	SparkAppRunningCount *PositiveGauge

	SparkAppSuccessExecutionTime *prometheus.SummaryVec
	SparkAppFailureExecutionTime *prometheus.SummaryVec

	SparkAppExecutorRunningCount *PositiveGauge
	SparkAppExecutorFailureCount *prometheus.CounterVec
	SparkAppExecutorSuccessCount *prometheus.CounterVec
}

func CreateValidMetric(prefix, name string) string {
	// "-" aren't valid characters for prometheus metric names
	return strings.Replace(prefix+name, "-", "_", -1)
}

type PositiveGauge struct {
	mux         sync.RWMutex
	name        string
	gaugeMetric *prometheus.GaugeVec
}

// Gauge with conditional decrement ensuring its value is never negative.
func NewPositiveGauge(name string, description string, labels []string) *PositiveGauge {
	gauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: name,
			Help: description,
		},
		labels,
	)

	prometheus.Register(gauge)

	return &PositiveGauge{
		gaugeMetric: gauge,
		name:        name,
	}
}


func fetchGaugeValue(m *prometheus.GaugeVec, labels map[string]string) float64 {
	// Hack to get the current value of the metric to support PositiveGauge
	pb := &prometheus_model.Metric{}

	m.With(labels).Write(pb)
	return pb.GetGauge().GetValue()
}

func (c *PositiveGauge) Value(labelMap map[string]string) float64 {
	c.mux.RLock()
	defer c.mux.RUnlock()
	return fetchGaugeValue(c.gaugeMetric, labelMap)
}

// Increment the Metric for the labels specified
func (c *PositiveGauge) Inc(labelMap map[string]string) {

	c.mux.Lock()
	defer c.mux.Unlock()

	if m, err := c.gaugeMetric.GetMetricWith(labelMap); err != nil {
		glog.Errorf("Error while posting metrics: %v", err)

	} else {
		glog.V(2).Infof("Incrementing %s with labels %s", c.name, labelMap)
		m.Inc()
	}
}

// Decrement the metric only if its positive for the labels specified
func (c *PositiveGauge) Dec(labelMap map[string]string) {

	c.mux.Lock()
	defer c.mux.Unlock()

	// Decrement only if positive
	val := fetchGaugeValue(c.gaugeMetric, labelMap)
	if val > 0 {
		glog.V(2).Infof("Decrementing %s with labels %s metricVal to %v", c.name, labelMap, val-1)
		if m, err := c.gaugeMetric.GetMetricWith(labelMap); err != nil {
			glog.Errorf("Error while posting metrics: %v", err)
		} else {
			m.Dec()
		}
	}
}

func NewPrometheusMetrics(endpoint string, port string, prefix string, labels []string) *PrometheusMetrics {

	sparkAppSubmitCount := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: CreateValidMetric(prefix, "spark_app_submit_count"),
			Help: "Spark App Submits via the Operator",
		},
		labels,
	)

	sparkAppSuccessCount := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: CreateValidMetric(prefix, "spark_app_success_count"),
			Help: "Spark App Success Count via the Operator",
		},
		labels,
	)

	sparkAppFailureCount := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: CreateValidMetric(prefix, "spark_app_failure_count"),
			Help: "Spark App Failure Count via the Operator",
		},
		labels,
	)

	sparkAppSuccessExecutionTime := prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: CreateValidMetric(prefix, "spark_app_success_execution_time_microseconds"),
			Help: "Spark App Runtime via the Operator",
		},
		labels,
	)

	sparkAppFailureExecutionTime := prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: CreateValidMetric(prefix, "spark_app_failure_execution_time_microseconds"),
			Help: "Spark App Runtime via the Operator",
		},
		labels,
	)

	sparkAppExecutorFailureCount := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: CreateValidMetric(prefix, "spark_app_executor_failure_count"),
			Help: "Spark App Failed Executor Count via the Operator",
		},
		labels,
	)

	sparkAppExecutorSuccessCount := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: CreateValidMetric(prefix, "spark_app_executor_success_count"),
			Help: "Spark App Failed Executor Count via the Operator",
		},
		labels,
	)

	sparkAppRunningCount := NewPositiveGauge(CreateValidMetric(prefix, "spark_app_running_count"),
		"Spark App Running Count via the Operator", labels)

	sparkAppExecutorRunningCount := NewPositiveGauge(CreateValidMetric(prefix,
		"spark_app_executor_running_count"), "Spark App Executor Running Count via the Operator", labels)

	// Start the metrics endpoint for Prometheus to scrape
	http.Handle(endpoint, promhttp.Handler())
	go http.ListenAndServe(port, nil)
	glog.Infof("Started Metrics server at localhost%s%s", port, endpoint)

	prometheus.Register(sparkAppSubmitCount)
	prometheus.Register(sparkAppSuccessCount)
	prometheus.Register(sparkAppFailureCount)
	prometheus.Register(sparkAppSuccessExecutionTime)
	prometheus.Register(sparkAppFailureExecutionTime)
	prometheus.Register(sparkAppExecutorFailureCount)
	prometheus.Register(sparkAppExecutorSuccessCount)

	metricBundle := &PrometheusMetrics{
		labels,
		prefix,
		sparkAppSubmitCount,
		sparkAppSuccessCount,
		sparkAppFailureCount,
		sparkAppRunningCount,
		sparkAppSuccessExecutionTime,
		sparkAppFailureExecutionTime,
		sparkAppExecutorRunningCount,
		sparkAppExecutorFailureCount,
		sparkAppExecutorSuccessCount,
	}
	workqueue.SetProvider(metricBundle)
	return metricBundle
}

// Depth Metric for kubernetes workqueue
func (p *PrometheusMetrics) NewDepthMetric(name string) workqueue.GaugeMetric {
	depthMetric := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: CreateValidMetric(p.prefix, name+"_depth"),
		Help: "Current depth of workqueue: " + name,
	},
	)
	prometheus.Register(depthMetric)
	return depthMetric
}

// Adds Count Metrics for kubernetes workqueue
func (p *PrometheusMetrics) NewAddsMetric(name string) workqueue.CounterMetric {
	addsMetric := prometheus.NewCounter(prometheus.CounterOpts{
		Name: CreateValidMetric(p.prefix, name+"_adds"),
		Help: "Total number of adds handled by workqueue: " + name,
	})
	prometheus.Register(addsMetric)
	return addsMetric
}

// Latency Metric for kubernetes workqueue
func (p *PrometheusMetrics) NewLatencyMetric(name string) workqueue.SummaryMetric {
	latencyMetric := prometheus.NewSummary(prometheus.SummaryOpts{
		Name: CreateValidMetric(p.prefix, name+"_latency"),
		Help: "Latency workqueue: " + name,
	})
	prometheus.Register(latencyMetric)
	return latencyMetric
}

// WorkDuration Metric for kubernetes workqueue
func (p *PrometheusMetrics) NewWorkDurationMetric(name string) workqueue.SummaryMetric {
	workDurationMetric := prometheus.NewSummary(prometheus.SummaryOpts{
		Name: CreateValidMetric(p.prefix, name+"_work_duration"),
		Help: "How long processing an item from workqueue" + name + " takes.",
	})
	prometheus.Register(workDurationMetric)
	return workDurationMetric
}

// Retry Metric for kubernetes workqueue
func (p *PrometheusMetrics) NewRetriesMetric(name string) workqueue.CounterMetric {
	retriesMetrics := prometheus.NewCounter(prometheus.CounterOpts{
		Name: CreateValidMetric(p.prefix, name+"_retries"),
		Help: "Total number of retries handled by workqueue: " + name,
	})
	prometheus.Register(retriesMetrics)
	return retriesMetrics
}
