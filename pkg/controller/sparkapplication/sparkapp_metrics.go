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
	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	"k8s.io/spark-on-k8s-operator/pkg/util"
)

type sparkAppMetrics struct {
	Labels               []string
	prefix               string
	SparkAppSubmitCount  *prometheus.CounterVec
	SparkAppSuccessCount *prometheus.CounterVec
	SparkAppFailureCount *prometheus.CounterVec
	SparkAppRunningCount *util.PositiveGauge

	SparkAppSuccessExecutionTime *prometheus.SummaryVec
	SparkAppFailureExecutionTime *prometheus.SummaryVec

	SparkAppExecutorRunningCount *util.PositiveGauge
	SparkAppExecutorFailureCount *prometheus.CounterVec
	SparkAppExecutorSuccessCount *prometheus.CounterVec
}

func newSparkAppMetrics(prefix string, labels []string) *sparkAppMetrics {
	for i, label := range labels {
		labels[i] = util.CreateValidMetricNameLabel("", label)
	}

	sparkAppSubmitCount := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: util.CreateValidMetricNameLabel(prefix, "spark_app_submit_count"),
			Help: "Spark App Submits via the Operator",
		},
		labels,
	)
	sparkAppSuccessCount := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: util.CreateValidMetricNameLabel(prefix, "spark_app_success_count"),
			Help: "Spark App Success Count via the Operator",
		},
		labels,
	)
	sparkAppFailureCount := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: util.CreateValidMetricNameLabel(prefix, "spark_app_failure_count"),
			Help: "Spark App Failure Count via the Operator",
		},
		labels,
	)
	sparkAppSuccessExecutionTime := prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: util.CreateValidMetricNameLabel(prefix, "spark_app_success_execution_time_microseconds"),
			Help: "Spark App Runtime via the Operator",
		},
		labels,
	)
	sparkAppFailureExecutionTime := prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: util.CreateValidMetricNameLabel(prefix, "spark_app_failure_execution_time_microseconds"),
			Help: "Spark App Runtime via the Operator",
		},
		labels,
	)
	sparkAppExecutorFailureCount := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: util.CreateValidMetricNameLabel(prefix, "spark_app_executor_failure_count"),
			Help: "Spark App Failed Executor Count via the Operator",
		},
		labels,
	)
	sparkAppExecutorSuccessCount := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: util.CreateValidMetricNameLabel(prefix, "spark_app_executor_success_count"),
			Help: "Spark App Successful Executor Count via the Operator",
		},
		labels,
	)
	sparkAppRunningCount := util.NewPositiveGauge(util.CreateValidMetricNameLabel(prefix, "spark_app_running_count"),
		"Spark App Running Count via the Operator", labels)
	sparkAppExecutorRunningCount := util.NewPositiveGauge(util.CreateValidMetricNameLabel(prefix,
		"spark_app_executor_running_count"), "Spark App Executor Running Count via the Operator", labels)

	util.RegisterMetric(sparkAppSubmitCount)
	util.RegisterMetric(sparkAppSuccessCount)
	util.RegisterMetric(sparkAppFailureCount)
	util.RegisterMetric(sparkAppSuccessExecutionTime)
	util.RegisterMetric(sparkAppFailureExecutionTime)
	util.RegisterMetric(sparkAppExecutorFailureCount)
	util.RegisterMetric(sparkAppExecutorSuccessCount)

	return &sparkAppMetrics{
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
		metricLabels[label] = validSpecLabels[label]
		if metricLabels[label] == "" {
			metricLabels[label] = "Unknown"
		}
	}
	return metricLabels
}

func (sm *sparkAppMetrics) exportMetrics(oldApp, newApp *v1alpha1.SparkApplication) {
	metricLabels := fetchMetricLabels(newApp.Labels, sm.Labels)
	glog.V(2).Infof("Posting Metrics for %s. OldStatus: %v NewStatus: %v", newApp.GetName(), oldApp.Status, newApp.Status)

	oldState := oldApp.Status.AppState.State
	newState := newApp.Status.AppState.State
	switch newState {
	case v1alpha1.SubmittedState:
		if isValidDriverStateTransition(oldState, newState) {
			if m, err := sm.SparkAppSubmitCount.GetMetricWith(metricLabels); err != nil {
				glog.Errorf("Error while posting metrics: %v", err)
			} else {
				m.Inc()
			}
		}
	case v1alpha1.RunningState:
		if isValidDriverStateTransition(oldState, newState) {
			sm.SparkAppRunningCount.Inc(metricLabels)
		}
	case v1alpha1.CompletedState:
		if isValidDriverStateTransition(oldState, newState) {
			if !newApp.Status.SubmissionTime.Time.IsZero() && !newApp.Status.CompletionTime.Time.IsZero() {
				d := newApp.Status.CompletionTime.Time.Sub(newApp.Status.SubmissionTime.Time)

				if m, err := sm.SparkAppSuccessExecutionTime.GetMetricWith(metricLabels); err != nil {
					glog.Errorf("Error while posting metrics: %v", err)
				} else {
					m.Observe(float64(d / time.Microsecond))
				}
			}
			sm.SparkAppRunningCount.Dec(metricLabels)
			if m, err := sm.SparkAppSuccessCount.GetMetricWith(metricLabels); err != nil {
				glog.Errorf("Error while posting metrics: %v", err)
			} else {
				m.Inc()
			}
		}
	case v1alpha1.FailedSubmissionState:
		fallthrough
	case v1alpha1.FailedState:
		if isValidDriverStateTransition(oldState, newState) {
			if !newApp.Status.SubmissionTime.Time.IsZero() && !newApp.Status.CompletionTime.Time.IsZero() {
				d := newApp.Status.CompletionTime.Time.Sub(newApp.Status.SubmissionTime.Time)
				if m, err := sm.SparkAppFailureExecutionTime.GetMetricWith(metricLabels); err != nil {
					glog.Errorf("Error while posting metrics: %v", err)
				} else {
					m.Observe(float64(d / time.Microsecond))
				}
			}
			sm.SparkAppRunningCount.Dec(metricLabels)
			if m, err := sm.SparkAppFailureCount.GetMetricWith(metricLabels); err != nil {
				glog.Errorf("Error while posting metrics: %v", err)
			} else {
				m.Inc()
			}
		}
	}

	// Potential Executor status updates
	for executor, newExecState := range newApp.Status.ExecutorState {
		if newExecState == v1alpha1.ExecutorRunningState && (isValidExecutorStateTransition(oldApp.Status.ExecutorState[executor], newExecState)) {
			glog.V(2).Infof("Posting Metrics for Executor %s. OldState: %v NewState: %v", executor, oldApp.Status.ExecutorState[executor], newExecState)
			sm.SparkAppExecutorRunningCount.Inc(metricLabels)
		}
		if newExecState == v1alpha1.ExecutorCompletedState &&
			isValidExecutorStateTransition(oldApp.Status.ExecutorState[executor], newExecState) {
			glog.V(2).Infof("Posting Metrics for Executor %s. OldState: %v NewState: %v", executor, oldApp.Status.ExecutorState[executor], newExecState)
			sm.SparkAppExecutorRunningCount.Dec(metricLabels)
			if m, err := sm.SparkAppExecutorSuccessCount.GetMetricWith(metricLabels); err != nil {
				glog.Errorf("Error while posting metrics: %v", err)
			} else {
				m.Inc()
			}
		}
		if newExecState == v1alpha1.ExecutorFailedState &&
			isValidExecutorStateTransition(oldApp.Status.ExecutorState[executor], newExecState) {
			glog.V(2).Infof("Posting Metrics for Executor %s. OldState: %v NewState: %v", executor, oldApp.Status.ExecutorState[executor], newExecState)
			sm.SparkAppExecutorRunningCount.Dec(metricLabels)
			if m, err := sm.SparkAppExecutorFailureCount.GetMetricWith(metricLabels); err != nil {
				glog.Errorf("Error while posting metrics: %v", err)
			} else {
				m.Inc()
			}
		}
	}
}
