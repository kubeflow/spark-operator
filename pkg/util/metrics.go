package util

import (
	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/client-go/util/workqueue"
	"net/http"
	"strings"
)

type PrometheusMetrics struct {
	Labels               []string
	prefix               string
	SparkAppSubmitCount  *prometheus.CounterVec
	SparkAppSuccessCount *prometheus.CounterVec
	SparkAppFailureCount *prometheus.CounterVec
	SparkAppRunningCount *prometheus.GaugeVec

	SparkAppSuccessExecutionTime *prometheus.SummaryVec
	SparkAppFailureExecutionTime *prometheus.SummaryVec

	SparkAppRunningExecutorCount *prometheus.GaugeVec
	SparkAppFailedExecutorCount  *prometheus.GaugeVec
}

func CreateValidMetric(prefix, name string) string {
	// "-" aren't valid characters for metric names
	return strings.Replace(prefix + name, "-", "_", -1)
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

	sparkAppRunningCount := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: CreateValidMetric(prefix, "spark_app_running_count"),
			Help: "Spark App Running Count via the Operator",
		},
		labels,
	)

	sparkAppSuccessExecutionTime := prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: CreateValidMetric(prefix, "spark_app_success_execution_time_secs"),
			Help: "Spark App Runtime via the Operator",
		},
		labels,
	)

	sparkAppFailureExecutionTime := prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: CreateValidMetric(prefix, "spark_app_failure_execution_time_secs"),
			Help: "Spark App Runtime via the Operator",
		},
		labels,
	)

	sparkAppRunningExecutorCount := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: CreateValidMetric(prefix, "spark_app_running_executor_count"),
			Help: "Spark App Running Executor Count via the Operator",
		},
		labels,
	)

	sparkAppFailedExecutorCount := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: CreateValidMetric(prefix, "spark_app_failed_executor_count"),
			Help: "Spark App Failed Executor Count via the Operator",
		},
		labels,
	)

	// Start the metrics endpoint for Prometheus to scrape
	http.Handle(endpoint, promhttp.Handler())
	go http.ListenAndServe(port, nil)
	glog.Infof("Started Metrics server at localhost%s%s", port, endpoint)

	prometheus.MustRegister(sparkAppSubmitCount)
	prometheus.MustRegister(sparkAppSuccessCount)
	prometheus.MustRegister(sparkAppFailureCount)
	prometheus.MustRegister(sparkAppRunningCount)
	prometheus.MustRegister(sparkAppSuccessExecutionTime)
	prometheus.MustRegister(sparkAppFailureExecutionTime)
	prometheus.MustRegister(sparkAppRunningExecutorCount)
	prometheus.MustRegister(sparkAppFailedExecutorCount)

	metricBundle := &PrometheusMetrics{
		labels,
		prefix,
		sparkAppSubmitCount,
		sparkAppSuccessCount,
		sparkAppFailureCount,
		sparkAppRunningCount,
		sparkAppSuccessExecutionTime,
		sparkAppFailureExecutionTime,
		sparkAppRunningExecutorCount,
		sparkAppFailedExecutorCount,
	}
	workqueue.SetProvider(metricBundle)
	return metricBundle
}

func (p *PrometheusMetrics) NewDepthMetric(name string) workqueue.GaugeMetric {
	depthMetric := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: CreateValidMetric(p.prefix, name+"_depth"),
		Help: "Current depth of workqueue: " + name,
	},
	)
	prometheus.MustRegister(depthMetric)
	return depthMetric
}

func (p *PrometheusMetrics) NewAddsMetric(name string) workqueue.CounterMetric {
	name = strings.Replace(p.prefix+name, "-", "_", -1)

	addsMetric := prometheus.NewCounter(prometheus.CounterOpts{
		Name: CreateValidMetric(p.prefix, name+"_adds"),
		Help: "Total number of adds handled by workqueue: " + name,
	})
	prometheus.MustRegister(addsMetric)
	return addsMetric
}

func (p *PrometheusMetrics) NewLatencyMetric(name string) workqueue.SummaryMetric {
	latencyMetric := prometheus.NewSummary(prometheus.SummaryOpts{
		Name: CreateValidMetric(p.prefix, name+"_latency"),
		Help: "Latency workqueue: " + name,
	})
	prometheus.MustRegister(latencyMetric)
	return latencyMetric
}

func (p *PrometheusMetrics) NewWorkDurationMetric(name string) workqueue.SummaryMetric {
	name = strings.Replace(p.prefix+name, "-", "_", -1)

	workDurationMetric := prometheus.NewSummary(prometheus.SummaryOpts{
		Name: CreateValidMetric(p.prefix, name+"_work_duration"),
		Help: "How long processing an item from workqueue" + name + " takes.",
	})
	prometheus.MustRegister(workDurationMetric)
	return workDurationMetric
}

func (p *PrometheusMetrics) NewRetriesMetric(name string) workqueue.CounterMetric {
	name = strings.Replace(p.prefix+name, "-", "_", -1)

	retriesMetrics := prometheus.NewCounter(prometheus.CounterOpts{
		Name: CreateValidMetric(p.prefix, name+"_retries"),
		Help: "Total number of retries handled by workqueue: " + name,
	})
	prometheus.MustRegister(retriesMetrics)
	return retriesMetrics
}
