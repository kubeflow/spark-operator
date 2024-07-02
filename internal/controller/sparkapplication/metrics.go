/*
Copyright 2018 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sparkapplication

import (
	"time"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/util"
)

type Metrics struct {
	labels []string
	prefix string

	sparkAppCount                 *prometheus.CounterVec
	sparkAppSubmitCount           *prometheus.CounterVec
	sparkAppFailedSubmissionCount *prometheus.CounterVec
	sparkAppRunningCount          *util.PositiveGauge
	sparkAppSuccessCount          *prometheus.CounterVec
	sparkAppFailureCount          *prometheus.CounterVec

	sparkAppSuccessExecutionTime  *prometheus.SummaryVec
	sparkAppFailureExecutionTime  *prometheus.SummaryVec
	sparkAppStartLatency          *prometheus.SummaryVec
	sparkAppStartLatencyHistogram *prometheus.HistogramVec

	sparkAppExecutorRunningCount *util.PositiveGauge
	sparkAppExecutorSuccessCount *prometheus.CounterVec
	sparkAppExecutorFailureCount *prometheus.CounterVec
}

func NewMetrics(metricsConfig *util.MetricConfig) *Metrics {
	prefix := metricsConfig.MetricsPrefix
	labels := metricsConfig.MetricsLabels
	validLabels := make([]string, len(labels))
	for i, label := range labels {
		validLabels[i] = util.CreateValidMetricNameLabel("", label)
	}

	sparkAppCount := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: util.CreateValidMetricNameLabel(prefix, "spark_app_count"),
			Help: "Total Number of Spark Apps Handled by the Operator",
		},
		validLabels,
	)

	sparkAppSubmitCount := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: util.CreateValidMetricNameLabel(prefix, "spark_app_submit_count"),
			Help: "Spark App Submits via the Operator",
		},
		validLabels,
	)

	sparkAppSuccessCount := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: util.CreateValidMetricNameLabel(prefix, "spark_app_success_count"),
			Help: "Spark App Success Count via the Operator",
		},
		validLabels,
	)

	sparkAppFailureCount := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: util.CreateValidMetricNameLabel(prefix, "spark_app_failure_count"),
			Help: "Spark App Failure Count via the Operator",
		},
		validLabels,
	)

	sparkAppFailedSubmissionCount := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: util.CreateValidMetricNameLabel(prefix, "spark_app_failed_submission_count"),
			Help: "Spark App Failed Submission Count via the Operator",
		},
		validLabels,
	)

	sparkAppSuccessExecutionTime := prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: util.CreateValidMetricNameLabel(prefix, "spark_app_success_execution_time_microseconds"),
			Help: "Spark App Successful Execution Runtime via the Operator",
		},
		validLabels,
	)

	sparkAppFailureExecutionTime := prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: util.CreateValidMetricNameLabel(prefix, "spark_app_failure_execution_time_microseconds"),
			Help: "Spark App Failed Execution Runtime via the Operator",
		},
		validLabels,
	)

	sparkAppStartLatency := prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: util.CreateValidMetricNameLabel(prefix, "spark_app_start_latency_microseconds"),
			Help: "Spark App Start Latency via the Operator",
		},
		validLabels,
	)

	sparkAppStartLatencyHistogram := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    util.CreateValidMetricNameLabel(prefix, "spark_app_start_latency_seconds"),
			Help:    "Spark App Start Latency counts in buckets via the Operator",
			Buckets: metricsConfig.MetricsJobStartLatencyBuckets,
		},
		validLabels,
	)

	sparkAppExecutorSuccessCount := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: util.CreateValidMetricNameLabel(prefix, "spark_app_executor_success_count"),
			Help: "Spark App Successful Executor Count via the Operator",
		},
		validLabels,
	)

	sparkAppExecutorFailureCount := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: util.CreateValidMetricNameLabel(prefix, "spark_app_executor_failure_count"),
			Help: "Spark App Failed Executor Count via the Operator",
		},
		validLabels,
	)

	sparkAppRunningCount := util.NewPositiveGauge(util.CreateValidMetricNameLabel(prefix, "spark_app_running_count"),
		"Spark App Running Count via the Operator", validLabels)

	sparkAppExecutorRunningCount := util.NewPositiveGauge(util.CreateValidMetricNameLabel(prefix,
		"spark_app_executor_running_count"), "Spark App Running Executor Count via the Operator", validLabels)

	return &Metrics{
		labels:                        validLabels,
		prefix:                        prefix,
		sparkAppCount:                 sparkAppCount,
		sparkAppSubmitCount:           sparkAppSubmitCount,
		sparkAppRunningCount:          sparkAppRunningCount,
		sparkAppSuccessCount:          sparkAppSuccessCount,
		sparkAppFailureCount:          sparkAppFailureCount,
		sparkAppFailedSubmissionCount: sparkAppFailedSubmissionCount,
		sparkAppSuccessExecutionTime:  sparkAppSuccessExecutionTime,
		sparkAppFailureExecutionTime:  sparkAppFailureExecutionTime,
		sparkAppStartLatency:          sparkAppStartLatency,
		sparkAppStartLatencyHistogram: sparkAppStartLatencyHistogram,
		sparkAppExecutorRunningCount:  sparkAppExecutorRunningCount,
		sparkAppExecutorSuccessCount:  sparkAppExecutorSuccessCount,
		sparkAppExecutorFailureCount:  sparkAppExecutorFailureCount,
	}
}

func (m *Metrics) registerMetrics() {
	util.RegisterMetric(m.sparkAppCount)
	util.RegisterMetric(m.sparkAppSubmitCount)
	util.RegisterMetric(m.sparkAppSuccessCount)
	util.RegisterMetric(m.sparkAppFailureCount)
	util.RegisterMetric(m.sparkAppSuccessExecutionTime)
	util.RegisterMetric(m.sparkAppFailureExecutionTime)
	util.RegisterMetric(m.sparkAppStartLatency)
	util.RegisterMetric(m.sparkAppStartLatencyHistogram)
	util.RegisterMetric(m.sparkAppExecutorSuccessCount)
	util.RegisterMetric(m.sparkAppExecutorFailureCount)
	m.sparkAppRunningCount.Register()
	m.sparkAppExecutorRunningCount.Register()
}

func (m *Metrics) exportMetricsOnDeletion(app *v1beta2.SparkApplication) {
	metricLabels := m.fetchMetricLabels(app)
	state := app.Status.AppState.State
	if state == v1beta2.ApplicationStateRunning {
		logger.V(1).Info("Decreasing spark application running count", "name", app.Name, "namespace", app.Namespace, "state")
		m.sparkAppRunningCount.Dec(metricLabels)
	}
	for executorName, executorState := range app.Status.ExecutorState {
		if executorState == v1beta2.ExecutorStateRunning {
			logger.V(1).Info("Decreasing executor pod running count", "name", executorName, "namespace", app.Namespace)
			m.sparkAppExecutorRunningCount.Dec(metricLabels)
		}
	}
}

func (m *Metrics) exportMetrics(oldApp, newApp *v1beta2.SparkApplication) {
	metricLabels := m.fetchMetricLabels(newApp)

	oldState := oldApp.Status.AppState.State
	newState := newApp.Status.AppState.State
	if newState != oldState {
		if oldState == v1beta2.ApplicationStateNew {
			if m, err := m.sparkAppCount.GetMetricWith(metricLabels); err != nil {
				glog.Errorf("Error while exporting metrics: %v", err)
			} else {
				m.Inc()
			}
		}

		switch newState {
		case v1beta2.ApplicationStateSubmitted:
			if m, err := m.sparkAppSubmitCount.GetMetricWith(metricLabels); err != nil {
				glog.Errorf("Error while exporting metrics: %v", err)
			} else {
				m.Inc()
			}
		case v1beta2.ApplicationStateRunning:
			m.sparkAppRunningCount.Inc(metricLabels)
			m.exportJobStartLatencyMetrics(newApp, metricLabels)
		case v1beta2.ApplicationStateSucceeding:
			if !newApp.Status.LastSubmissionAttemptTime.Time.IsZero() && !newApp.Status.TerminationTime.Time.IsZero() {
				d := newApp.Status.TerminationTime.Time.Sub(newApp.Status.LastSubmissionAttemptTime.Time)
				if m, err := m.sparkAppSuccessExecutionTime.GetMetricWith(metricLabels); err != nil {
					glog.Errorf("Error while exporting metrics: %v", err)
				} else {
					m.Observe(float64(d / time.Microsecond))
				}
			}
			m.sparkAppRunningCount.Dec(metricLabels)
			if m, err := m.sparkAppSuccessCount.GetMetricWith(metricLabels); err != nil {
				glog.Errorf("Error while exporting metrics: %v", err)
			} else {
				m.Inc()
			}
		case v1beta2.ApplicationStateFailing:
			if !newApp.Status.LastSubmissionAttemptTime.Time.IsZero() && !newApp.Status.TerminationTime.Time.IsZero() {
				d := newApp.Status.TerminationTime.Time.Sub(newApp.Status.LastSubmissionAttemptTime.Time)
				if m, err := m.sparkAppFailureExecutionTime.GetMetricWith(metricLabels); err != nil {
					glog.Errorf("Error while exporting metrics: %v", err)
				} else {
					m.Observe(float64(d / time.Microsecond))
				}
			}
			m.sparkAppRunningCount.Dec(metricLabels)
			if m, err := m.sparkAppFailureCount.GetMetricWith(metricLabels); err != nil {
				glog.Errorf("Error while exporting metrics: %v", err)
			} else {
				m.Inc()
			}
		case v1beta2.ApplicationStateFailedSubmission:
			if m, err := m.sparkAppFailedSubmissionCount.GetMetricWith(metricLabels); err != nil {
				glog.Errorf("Error while exporting metrics: %v", err)
			} else {
				m.Inc()
			}
		}
	}

	// In the event that state transitions happened too quickly and the spark app skipped the RUNNING state, the job
	// start latency should still be captured.
	// Note: There is an edge case that a Submitted state can go directly to a Failing state if the driver pod is
	//       deleted. This is very unlikely if not being done intentionally, so we choose not to handle it.
	if newState != oldState {
		if (newState == v1beta2.ApplicationStateFailing || newState == v1beta2.ApplicationStateSucceeding) && oldState == v1beta2.ApplicationStateSubmitted {
			// TODO: remove this log once we've gathered some data in prod fleets.
			glog.V(2).Infof("Calculating job start latency metrics for edge case transition from %v to %v in app %v in namespace %v.", oldState, newState, newApp.Name, newApp.Namespace)
			m.exportJobStartLatencyMetrics(newApp, metricLabels)
		}
	}

	oldExecutorStates := oldApp.Status.ExecutorState
	// Potential Executor status updates
	for executor, newExecState := range newApp.Status.ExecutorState {
		switch newExecState {
		case v1beta2.ExecutorStateRunning:
			if oldExecutorStates[executor] != newExecState {
				glog.V(2).Infof("Exporting Metrics for Executor %s. OldState: %v NewState: %v", executor,
					oldExecutorStates[executor], newExecState)
				m.sparkAppExecutorRunningCount.Inc(metricLabels)
			}
		case v1beta2.ExecutorStateCompleted:
			if oldExecutorStates[executor] != newExecState {
				glog.V(2).Infof("Exporting Metrics for Executor %s. OldState: %v NewState: %v", executor,
					oldExecutorStates[executor], newExecState)
				m.sparkAppExecutorRunningCount.Dec(metricLabels)
				if m, err := m.sparkAppExecutorSuccessCount.GetMetricWith(metricLabels); err != nil {
					glog.Errorf("Error while exporting metrics: %v", err)
				} else {
					m.Inc()
				}
			}
		case v1beta2.ExecutorStateFailed:
			if oldExecutorStates[executor] != newExecState {
				glog.V(2).Infof("Exporting Metrics for Executor %s. OldState: %v NewState: %v", executor,
					oldExecutorStates[executor], newExecState)
				m.sparkAppExecutorRunningCount.Dec(metricLabels)
				if m, err := m.sparkAppExecutorFailureCount.GetMetricWith(metricLabels); err != nil {
					glog.Errorf("Error while exporting metrics: %v", err)
				} else {
					m.Inc()
				}
			}
		}
	}
}

func (m *Metrics) exportJobStartLatencyMetrics(app *v1beta2.SparkApplication, labels map[string]string) {
	// Expose the job start latency related metrics of an SparkApp only once when it runs for the first time
	if app.Status.ExecutionAttempts == 1 {
		latency := time.Since(app.CreationTimestamp.Time)
		if m, err := m.sparkAppStartLatency.GetMetricWith(labels); err != nil {
			glog.Errorf("Error while exporting metrics: %v", err)
		} else {
			m.Observe(float64(latency / time.Microsecond))
		}
		if m, err := m.sparkAppStartLatencyHistogram.GetMetricWith(labels); err != nil {
			glog.Errorf("Error while exporting metrics: %v", err)
		} else {
			m.Observe(float64(latency / time.Second))
		}
	}
}

func (m *Metrics) fetchMetricLabels(app *v1beta2.SparkApplication) map[string]string {
	// Convert app labels into ones that can be used as metric labels.
	validLabels := make(map[string]string)
	for labelKey, v := range app.Labels {
		newKey := util.CreateValidMetricNameLabel("", labelKey)
		validLabels[newKey] = v
	}

	metricLabels := make(map[string]string)
	for _, label := range m.labels {
		if value, ok := validLabels[label]; ok {
			metricLabels[label] = value
		} else if label == "namespace" { // If the "namespace" label is in the metrics config, use it.
			metricLabels[label] = app.Namespace
		} else {
			metricLabels[label] = "Unknown"
		}
	}
	return metricLabels
}
