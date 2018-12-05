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

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/util"
)

type sparkAppMetrics struct {
	labels []string
	prefix string

	sparkAppSubmitCount  *prometheus.CounterVec
	sparkAppSuccessCount *prometheus.CounterVec
	sparkAppFailureCount *prometheus.CounterVec
	sparkAppRunningCount *util.PositiveGauge

	sparkAppSuccessExecutionTime *prometheus.SummaryVec
	sparkAppFailureExecutionTime *prometheus.SummaryVec

	sparkAppExecutorRunningCount *util.PositiveGauge
	sparkAppExecutorFailureCount *prometheus.CounterVec
	sparkAppExecutorSuccessCount *prometheus.CounterVec
}

func newSparkAppMetrics(prefix string, labels []string) *sparkAppMetrics {
	validLabels := make([]string, len(labels))
	for i, label := range labels {
		validLabels[i] = util.CreateValidMetricNameLabel("", label)
	}

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
	sparkAppExecutorFailureCount := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: util.CreateValidMetricNameLabel(prefix, "spark_app_executor_failure_count"),
			Help: "Spark App Failed Executor Count via the Operator",
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
	sparkAppRunningCount := util.NewPositiveGauge(util.CreateValidMetricNameLabel(prefix, "spark_app_running_count"),
		"Spark App Running Count via the Operator", validLabels)
	sparkAppExecutorRunningCount := util.NewPositiveGauge(util.CreateValidMetricNameLabel(prefix,
		"spark_app_executor_running_count"), "Spark App Running Executor Count via the Operator", validLabels)

	return &sparkAppMetrics{
		labels:                       validLabels,
		prefix:                       prefix,
		sparkAppSubmitCount:          sparkAppSubmitCount,
		sparkAppSuccessCount:         sparkAppSuccessCount,
		sparkAppFailureCount:         sparkAppFailureCount,
		sparkAppRunningCount:         sparkAppRunningCount,
		sparkAppSuccessExecutionTime: sparkAppSuccessExecutionTime,
		sparkAppFailureExecutionTime: sparkAppFailureExecutionTime,
		sparkAppExecutorRunningCount: sparkAppExecutorRunningCount,
		sparkAppExecutorFailureCount: sparkAppExecutorFailureCount,
		sparkAppExecutorSuccessCount: sparkAppExecutorSuccessCount,
	}
}

func (sm *sparkAppMetrics) registerMetrics() {
	util.RegisterMetric(sm.sparkAppSubmitCount)
	util.RegisterMetric(sm.sparkAppSuccessCount)
	util.RegisterMetric(sm.sparkAppFailureCount)
	util.RegisterMetric(sm.sparkAppSuccessExecutionTime)
	util.RegisterMetric(sm.sparkAppFailureExecutionTime)
	util.RegisterMetric(sm.sparkAppExecutorFailureCount)
	util.RegisterMetric(sm.sparkAppExecutorSuccessCount)
	sm.sparkAppRunningCount.Register()
	sm.sparkAppExecutorRunningCount.Register()
}

func (sm *sparkAppMetrics) exportMetrics(oldApp, newApp *v1alpha1.SparkApplication) {
	metricLabels := fetchMetricLabels(newApp.Labels, sm.labels)
	glog.V(2).Infof("Exporting metrics for %s; old status: %v new status: %v", newApp.Name,
		oldApp.Status, newApp.Status)

	oldState := oldApp.Status.AppState.State
	newState := newApp.Status.AppState.State
	switch newState {
	case v1alpha1.SubmittedState:
		if oldState != newState {
			if m, err := sm.sparkAppSubmitCount.GetMetricWith(metricLabels); err != nil {
				glog.Errorf("Error while exporting metrics: %v", err)
			} else {
				m.Inc()
			}
		}
	case v1alpha1.RunningState:
		if oldState != newState {
			sm.sparkAppRunningCount.Inc(metricLabels)
		}
	case v1alpha1.SucceedingState:
		if oldState != newState {
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
		}
	case v1alpha1.FailingState, v1alpha1.FailedSubmissionState:
		if oldState != newState {
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
		}
	}

	// Potential Executor status updates
	for executor, newExecState := range newApp.Status.ExecutorState {
		switch newExecState {
		case v1alpha1.ExecutorRunningState:
			if oldApp.Status.ExecutorState[executor] != newExecState {
				glog.V(2).Infof("Exporting Metrics for Executor %s. OldState: %v NewState: %v", executor,
					oldApp.Status.ExecutorState[executor], newExecState)
				sm.sparkAppExecutorRunningCount.Inc(metricLabels)
			}
		case v1alpha1.ExecutorCompletedState:
			if oldApp.Status.ExecutorState[executor] != newExecState {
				glog.V(2).Infof("Exporting Metrics for Executor %s. OldState: %v NewState: %v", executor,
					oldApp.Status.ExecutorState[executor], newExecState)
				sm.sparkAppExecutorRunningCount.Dec(metricLabels)
				if m, err := sm.sparkAppExecutorSuccessCount.GetMetricWith(metricLabels); err != nil {
					glog.Errorf("Error while exporting metrics: %v", err)
				} else {
					m.Inc()
				}
			}
		case v1alpha1.ExecutorFailedState:
			if oldApp.Status.ExecutorState[executor] != newExecState {
				glog.V(2).Infof("Exporting Metrics for Executor %s. OldState: %v NewState: %v", executor,
					oldApp.Status.ExecutorState[executor], newExecState)
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

func fetchMetricLabels(specLabels map[string]string, labels []string) map[string]string {
	// Transform spec labels since our labels names might be not same as specLabels if we removed invalid characters.
	validSpecLabels := make(map[string]string)
	for labelKey, v := range specLabels {
		newKey := util.CreateValidMetricNameLabel("", labelKey)
		validSpecLabels[newKey] = v
	}

	metricLabels := make(map[string]string)
	for _, label := range labels {
		if value, ok := validSpecLabels[label]; ok {
			metricLabels[label] = value
		} else {
			metricLabels[label] = "Unknown"
		}
	}
	return metricLabels
}
