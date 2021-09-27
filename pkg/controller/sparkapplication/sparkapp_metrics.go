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

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/util"
)

type sparkAppMetrics struct {
	labels []string
	prefix string

	sparkAppCount                 *prometheus.CounterVec
	sparkAppSubmitCount           *prometheus.CounterVec
	sparkAppSuccessCount          *prometheus.CounterVec
	sparkAppFailureCount          *prometheus.CounterVec
	sparkAppFailedSubmissionCount *prometheus.CounterVec
	sparkAppRunningCount          *util.PositiveGauge

	sparkAppSuccessExecutionTime  *prometheus.SummaryVec
	sparkAppFailureExecutionTime  *prometheus.SummaryVec
	sparkAppStartLatency          *prometheus.SummaryVec
	sparkAppStartLatencyHistogram *prometheus.HistogramVec

	sparkAppExecutorRunningCount *util.PositiveGauge
	sparkAppExecutorFailureCount *prometheus.CounterVec
	sparkAppExecutorSuccessCount *prometheus.CounterVec
}

func newSparkAppMetrics(metricsConfig *util.MetricConfig) *sparkAppMetrics {
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

	return &sparkAppMetrics{
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

func (sm *sparkAppMetrics) registerMetrics() {
	util.RegisterMetric(sm.sparkAppCount)
	util.RegisterMetric(sm.sparkAppSubmitCount)
	util.RegisterMetric(sm.sparkAppSuccessCount)
	util.RegisterMetric(sm.sparkAppFailureCount)
	util.RegisterMetric(sm.sparkAppSuccessExecutionTime)
	util.RegisterMetric(sm.sparkAppFailureExecutionTime)
	util.RegisterMetric(sm.sparkAppStartLatency)
	util.RegisterMetric(sm.sparkAppStartLatencyHistogram)
	util.RegisterMetric(sm.sparkAppExecutorSuccessCount)
	util.RegisterMetric(sm.sparkAppExecutorFailureCount)
	sm.sparkAppRunningCount.Register()
	sm.sparkAppExecutorRunningCount.Register()
}

func (sm *sparkAppMetrics) exportMetricsOnDelete(oldApp *v1beta2.SparkApplication) {
	metricLabels := fetchMetricLabels(oldApp, sm.labels)
	oldState := oldApp.Status.AppState.State
	if oldState == v1beta2.RunningState {
		sm.sparkAppRunningCount.Dec(metricLabels)
	}
	for executor, oldExecState := range oldApp.Status.ExecutorState {
		if oldExecState == v1beta2.ExecutorRunningState {
			glog.V(2).Infof("Application is deleted. Decreasing Running Count for Executor %s.", executor)
			sm.sparkAppExecutorRunningCount.Dec(metricLabels)
		}
	}
}

func (sm *sparkAppMetrics) exportMetrics(oldApp, newApp *v1beta2.SparkApplication) {
	metricLabels := fetchMetricLabels(newApp, sm.labels)

	oldState := oldApp.Status.AppState.State
	newState := newApp.Status.AppState.State
	if newState != oldState {
		if oldState == v1beta2.NewState {
			if m, err := sm.sparkAppCount.GetMetricWith(metricLabels); err != nil {
				glog.Errorf("Error while exporting metrics: %v", err)
			} else {
				m.Inc()
			}
		}

		switch newState {
		case v1beta2.SubmittedState:
			if m, err := sm.sparkAppSubmitCount.GetMetricWith(metricLabels); err != nil {
				glog.Errorf("Error while exporting metrics: %v", err)
			} else {
				m.Inc()
			}
		case v1beta2.RunningState:
			sm.sparkAppRunningCount.Inc(metricLabels)
			sm.exportJobStartLatencyMetrics(newApp, metricLabels)
		case v1beta2.SucceedingState:
			if !newApp.Status.LastSubmissionAttemptTime.Time.IsZero() && !newApp.Status.TerminationTime.Time.IsZero() {
				d := newApp.Status.TerminationTime.Time.Sub(newApp.Status.LastSubmissionAttemptTime.Time)
				if m, err := sm.sparkAppSuccessExecutionTime.GetMetricWith(metricLabels); err != nil {
					glog.Errorf("Error while exporting metrics: %v", err)
				} else {
					m.Observe(float64(d / time.Microsecond))
				}
			}
			sm.sparkAppRunningCount.Dec(metricLabels)
			if m, err := sm.sparkAppSuccessCount.GetMetricWith(metricLabels); err != nil {
				glog.Errorf("Error while exporting metrics: %v", err)
			} else {
				m.Inc()
			}
		case v1beta2.FailingState:
			if !newApp.Status.LastSubmissionAttemptTime.Time.IsZero() && !newApp.Status.TerminationTime.Time.IsZero() {
				d := newApp.Status.TerminationTime.Time.Sub(newApp.Status.LastSubmissionAttemptTime.Time)
				if m, err := sm.sparkAppFailureExecutionTime.GetMetricWith(metricLabels); err != nil {
					glog.Errorf("Error while exporting metrics: %v", err)
				} else {
					m.Observe(float64(d / time.Microsecond))
				}
			}
			sm.sparkAppRunningCount.Dec(metricLabels)
			if m, err := sm.sparkAppFailureCount.GetMetricWith(metricLabels); err != nil {
				glog.Errorf("Error while exporting metrics: %v", err)
			} else {
				m.Inc()
			}
		case v1beta2.FailedSubmissionState:
			if m, err := sm.sparkAppFailedSubmissionCount.GetMetricWith(metricLabels); err != nil {
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
		if (newState == v1beta2.FailingState || newState == v1beta2.SucceedingState) && oldState == v1beta2.SubmittedState {
			// TODO: remove this log once we've gathered some data in prod fleets.
			glog.V(2).Infof("Calculating job start latency metrics for edge case transition from %v to %v in app %v in namespace %v.", oldState, newState, newApp.Name, newApp.Namespace)
			sm.exportJobStartLatencyMetrics(newApp, metricLabels)
		}
	}

	oldExecutorStates := oldApp.Status.ExecutorState
	// Potential Executor status updates
	for executor, newExecState := range newApp.Status.ExecutorState {
		switch newExecState {
		case v1beta2.ExecutorRunningState:
			if oldExecutorStates[executor] != newExecState {
				glog.V(2).Infof("Exporting Metrics for Executor %s. OldState: %v NewState: %v", executor,
					oldExecutorStates[executor], newExecState)
				sm.sparkAppExecutorRunningCount.Inc(metricLabels)
			}
		case v1beta2.ExecutorCompletedState:
			if oldExecutorStates[executor] != newExecState {
				glog.V(2).Infof("Exporting Metrics for Executor %s. OldState: %v NewState: %v", executor,
					oldExecutorStates[executor], newExecState)
				sm.sparkAppExecutorRunningCount.Dec(metricLabels)
				if m, err := sm.sparkAppExecutorSuccessCount.GetMetricWith(metricLabels); err != nil {
					glog.Errorf("Error while exporting metrics: %v", err)
				} else {
					m.Inc()
				}
			}
		case v1beta2.ExecutorFailedState:
			if oldExecutorStates[executor] != newExecState {
				glog.V(2).Infof("Exporting Metrics for Executor %s. OldState: %v NewState: %v", executor,
					oldExecutorStates[executor], newExecState)
				sm.sparkAppExecutorRunningCount.Dec(metricLabels)
				if m, err := sm.sparkAppExecutorFailureCount.GetMetricWith(metricLabels); err != nil {
					glog.Errorf("Error while exporting metrics: %v", err)
				} else {
					m.Inc()
				}
			}
		}
	}
}

func (sm *sparkAppMetrics) exportJobStartLatencyMetrics(app *v1beta2.SparkApplication, labels map[string]string) {
	// Expose the job start latency related metrics of an SparkApp only once when it runs for the first time
	if app.Status.ExecutionAttempts == 1 {
		latency := time.Now().Sub(app.CreationTimestamp.Time)
		if m, err := sm.sparkAppStartLatency.GetMetricWith(labels); err != nil {
			glog.Errorf("Error while exporting metrics: %v", err)
		} else {
			m.Observe(float64(latency / time.Microsecond))
		}
		if m, err := sm.sparkAppStartLatencyHistogram.GetMetricWith(labels); err != nil {
			glog.Errorf("Error while exporting metrics: %v", err)
		} else {
			m.Observe(float64(latency / time.Second))
		}
	}
}

func fetchMetricLabels(app *v1beta2.SparkApplication, labels []string) map[string]string {
	// Convert app labels into ones that can be used as metric labels.
	validLabels := make(map[string]string)
	for labelKey, v := range app.Labels {
		newKey := util.CreateValidMetricNameLabel("", labelKey)
		validLabels[newKey] = v
	}

	metricLabels := make(map[string]string)
	for _, label := range labels {
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
