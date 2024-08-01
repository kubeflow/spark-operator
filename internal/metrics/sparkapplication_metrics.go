/*
Copyright 2024 The Kubeflow authors.

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

package metrics

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
)

type SparkApplicationMetrics struct {
	prefix                 string
	labels                 []string
	jobStartLatencyBuckets []float64

	count                 *prometheus.CounterVec
	submitCount           *prometheus.CounterVec
	failedSubmissionCount *prometheus.CounterVec
	runningCount          *prometheus.GaugeVec
	successCount          *prometheus.CounterVec
	failureCount          *prometheus.CounterVec

	successExecutionTimeSeconds *prometheus.SummaryVec
	failureExecutionTimeSeconds *prometheus.SummaryVec

	startLatencySeconds          *prometheus.SummaryVec
	startLatencySecondsHistogram *prometheus.HistogramVec
}

func NewSparkApplicationMetrics(prefix string, labels []string, jobStartLatencyBuckets []float64) *SparkApplicationMetrics {
	validLabels := make([]string, 0, len(labels))
	for _, label := range labels {
		validLabel := util.CreateValidMetricNameLabel("", label)
		validLabels = append(validLabels, validLabel)
	}

	return &SparkApplicationMetrics{
		prefix:                 prefix,
		labels:                 validLabels,
		jobStartLatencyBuckets: jobStartLatencyBuckets,

		count: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: util.CreateValidMetricNameLabel(prefix, common.MetricSparkApplicationCount),
				Help: "Total number of SparkApplication",
			},
			validLabels,
		),
		submitCount: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: util.CreateValidMetricNameLabel(prefix, common.MetricSparkApplicationSubmitCount),
				Help: "Total number of submitted SparkApplication",
			},
			validLabels,
		),
		failedSubmissionCount: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: util.CreateValidMetricNameLabel(prefix, common.MetricSparkApplicationFailedSubmissionCount),
				Help: "Total number of failed SparkApplication submission",
			},
			validLabels,
		),
		runningCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: util.CreateValidMetricNameLabel(prefix, common.MetricSparkApplicationRunningCount),
				Help: "Total number of running SparkApplication",
			},
			validLabels,
		),
		successCount: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: util.CreateValidMetricNameLabel(prefix, common.MetricSparkApplicationSuccessCount),
				Help: "Total number of successful SparkApplication",
			},
			validLabels,
		),
		failureCount: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: util.CreateValidMetricNameLabel(prefix, common.MetricSparkApplicationFailureCount),
				Help: "Total number of failed SparkApplication",
			},
			validLabels,
		),
		successExecutionTimeSeconds: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Name: util.CreateValidMetricNameLabel(prefix, common.MetricSparkApplicationSuccessExecutionTimeSeconds),
			},
			validLabels,
		),
		failureExecutionTimeSeconds: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Name: util.CreateValidMetricNameLabel(prefix, common.MetricSparkApplicationFailureExecutionTimeSeconds),
			},
			validLabels,
		),
		startLatencySeconds: prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Name: util.CreateValidMetricNameLabel(prefix, common.MetricSparkApplicationStartLatencySeconds),
				Help: "Spark App Start Latency via the Operator",
			},
			validLabels,
		),
		startLatencySecondsHistogram: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    util.CreateValidMetricNameLabel(prefix, common.MetricSparkApplicationStartLatencySecondsHistogram),
				Help:    "Spark App Start Latency counts in buckets via the Operator",
				Buckets: jobStartLatencyBuckets,
			},
			validLabels,
		),
	}
}

func (m *SparkApplicationMetrics) Register() {
	if err := metrics.Registry.Register(m.count); err != nil {
		logger.Error(err, "Failed to register spark application metric", "name", common.MetricSparkApplicationCount)
	}
	if err := metrics.Registry.Register(m.submitCount); err != nil {
		logger.Error(err, "Failed to register spark application metric", "name", common.MetricSparkApplicationSubmitCount)
	}
	if err := metrics.Registry.Register(m.failedSubmissionCount); err != nil {
		logger.Error(err, "Failed to register spark application metric", "name", common.MetricSparkApplicationFailedSubmissionCount)
	}
	if err := metrics.Registry.Register(m.runningCount); err != nil {
		logger.Error(err, "Failed to register spark application metric", "name", common.MetricSparkApplicationRunningCount)
	}
	if err := metrics.Registry.Register(m.successCount); err != nil {
		logger.Error(err, "Failed to register spark application metric", "name", common.MetricSparkApplicationSuccessCount)
	}
	if err := metrics.Registry.Register(m.failureCount); err != nil {
		logger.Error(err, "Failed to register spark application metric", "name", common.MetricSparkApplicationFailureCount)
	}
	if err := metrics.Registry.Register(m.successExecutionTimeSeconds); err != nil {
		logger.Error(err, "Failed to register spark application metric", "name", common.MetricSparkApplicationSuccessExecutionTimeSeconds)
	}
	if err := metrics.Registry.Register(m.failureExecutionTimeSeconds); err != nil {
		logger.Error(err, "Failed to register spark application metric", "name", common.MetricSparkApplicationFailureExecutionTimeSeconds)
	}
	if err := metrics.Registry.Register(m.startLatencySeconds); err != nil {
		logger.Error(err, "Failed to register spark application metric", "name", common.MetricSparkApplicationStartLatencySeconds)
	}
	if err := metrics.Registry.Register(m.startLatencySecondsHistogram); err != nil {
		logger.Error(err, "Failed to register spark application metric", "name", common.MetricSparkApplicationStartLatencySecondsHistogram)
	}
}

func (m *SparkApplicationMetrics) HandleSparkApplicationCreate(app *v1beta2.SparkApplication) {
	state := util.GetApplicationState(app)

	switch state {
	case v1beta2.ApplicationStateNew:
		m.incCount(app)
	case v1beta2.ApplicationStateSubmitted:
		m.incSubmitCount(app)
	case v1beta2.ApplicationStateFailedSubmission:
		m.incFailedSubmissionCount(app)
	case v1beta2.ApplicationStateRunning:
		m.incRunningCount(app)
	case v1beta2.ApplicationStateFailed:
		m.incFailureCount(app)
	case v1beta2.ApplicationStateCompleted:
		m.incSuccessCount(app)
	}
}

func (m *SparkApplicationMetrics) HandleSparkApplicationUpdate(oldApp *v1beta2.SparkApplication, newApp *v1beta2.SparkApplication) {
	oldState := util.GetApplicationState(oldApp)
	newState := util.GetApplicationState(newApp)
	if newState == oldState {
		return
	}

	switch oldState {
	case v1beta2.ApplicationStateRunning:
		m.decRunningCount(oldApp)
	}

	switch newState {
	case v1beta2.ApplicationStateNew:
		m.incCount(newApp)
	case v1beta2.ApplicationStateSubmitted:
		m.incSubmitCount(newApp)
	case v1beta2.ApplicationStateFailedSubmission:
		m.incFailedSubmissionCount(newApp)
	case v1beta2.ApplicationStateRunning:
		m.incRunningCount(newApp)
		m.observeStartLatencySeconds(newApp)
	case v1beta2.ApplicationStateCompleted:
		m.incSuccessCount(newApp)
		m.observeSuccessExecutionTimeSeconds(newApp)
	case v1beta2.ApplicationStateFailed:
		m.incFailureCount(newApp)
		m.observeFailureExecutionTimeSeconds(newApp)
	}
}

func (m *SparkApplicationMetrics) HandleSparkApplicationDelete(app *v1beta2.SparkApplication) {
	state := util.GetApplicationState(app)

	switch state {
	case v1beta2.ApplicationStateRunning:
		m.decRunningCount(app)
	}
}

func (m *SparkApplicationMetrics) incCount(app *v1beta2.SparkApplication) {
	labels := m.getMetricLabels(app)
	counter, err := m.count.GetMetricWith(labels)
	if err != nil {
		logger.Error(err, "Failed to collect metric for SparkApplication", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationCount, "labels", labels)
		return
	}

	counter.Inc()
	logger.V(1).Info("Increased spark application count", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationCount, "labels", labels)
}

func (m *SparkApplicationMetrics) incSubmitCount(app *v1beta2.SparkApplication) {
	labels := m.getMetricLabels(app)
	counter, err := m.submitCount.GetMetricWith(labels)
	if err != nil {
		logger.Error(err, "Failed to collect metric for SparkApplication", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationSubmitCount, "labels", labels)
		return
	}

	counter.Inc()
	logger.V(1).Info("Increased spark application submit count", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationSubmitCount, "labels", labels)
}

func (m *SparkApplicationMetrics) incFailedSubmissionCount(app *v1beta2.SparkApplication) {
	labels := m.getMetricLabels(app)
	counter, err := m.failedSubmissionCount.GetMetricWith(labels)
	if err != nil {
		logger.Error(err, "Failed to collect metric for SparkApplication", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationFailedSubmissionCount, "labels", labels)
		return
	}

	counter.Inc()
	logger.V(1).Info("Increased spark application failed submission count", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationFailedSubmissionCount, "labels", labels)
}

func (m *SparkApplicationMetrics) incRunningCount(app *v1beta2.SparkApplication) {
	labels := m.getMetricLabels(app)
	gauge, err := m.runningCount.GetMetricWith(labels)
	if err != nil {
		logger.Error(err, "Failed to collect metric for SparkApplication", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationRunningCount, "labels", labels)
		return
	}

	gauge.Inc()
	logger.V(1).Info("Increased spark application running count", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationRunningCount, "labels", labels)
}

func (m *SparkApplicationMetrics) decRunningCount(app *v1beta2.SparkApplication) {
	labels := m.getMetricLabels(app)
	gauge, err := m.runningCount.GetMetricWith(labels)
	if err != nil {
		logger.Error(err, "Failed to collect metric for SparkApplication", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationRunningCount, "labels", labels)
		return
	}

	gauge.Dec()
	logger.V(1).Info("Decreased SparkApplication running count", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationRunningCount, "labels", labels)
}

func (m *SparkApplicationMetrics) incSuccessCount(app *v1beta2.SparkApplication) {
	labels := m.getMetricLabels(app)
	counter, err := m.successCount.GetMetricWith(labels)
	if err != nil {
		logger.Error(err, "Failed to collect metric for SparkApplication", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationSuccessCount, "labels", labels)
		return
	}

	counter.Inc()
	logger.V(1).Info("Increased spark application success count", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationSuccessCount, "labels", labels)
}

func (m *SparkApplicationMetrics) incFailureCount(app *v1beta2.SparkApplication) {
	labels := m.getMetricLabels(app)
	counter, err := m.failureCount.GetMetricWith(labels)
	if err != nil {
		logger.Error(err, "Failed to collect metric for SparkApplication", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationFailureCount, "labels", labels)
		return
	}

	counter.Inc()
	logger.V(1).Info("Increased spark application failure count", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationFailureCount, "labels", labels)
}

func (m *SparkApplicationMetrics) observeSuccessExecutionTimeSeconds(app *v1beta2.SparkApplication) {
	labels := m.getMetricLabels(app)
	observer, err := m.successExecutionTimeSeconds.GetMetricWith(labels)
	if err != nil {
		logger.Error(err, "Failed to collect metric for SparkApplication", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationSuccessExecutionTimeSeconds, "labels", labels)
	}

	if app.Status.LastSubmissionAttemptTime.IsZero() || app.Status.TerminationTime.IsZero() {
		err := fmt.Errorf("last submission attempt time or termination time is zero")
		logger.Error(err, "Failed to collect metric for SparkApplication", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationSuccessExecutionTimeSeconds, "labels", labels)
		return
	}
	duration := app.Status.TerminationTime.Sub(app.Status.LastSubmissionAttemptTime.Time)
	observer.Observe(duration.Seconds())
	logger.V(1).Info("Observed spark application success execution time seconds", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationSuccessExecutionTimeSeconds, "labels", labels, "value", duration.Seconds())
}

func (m *SparkApplicationMetrics) observeFailureExecutionTimeSeconds(app *v1beta2.SparkApplication) {
	labels := m.getMetricLabels(app)
	observer, err := m.failureExecutionTimeSeconds.GetMetricWith(labels)
	if err != nil {
		logger.Error(err, "Failed to collect metric for SparkApplication", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationFailureExecutionTimeSeconds, "labels", labels)
	}

	if app.Status.LastSubmissionAttemptTime.IsZero() || app.Status.TerminationTime.IsZero() {
		err := fmt.Errorf("last submission attempt time or termination time is zero")
		logger.Error(err, "Failed to collect metric for SparkApplication", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationFailureExecutionTimeSeconds, "labels", labels)
		return
	}
	duration := app.Status.TerminationTime.Sub(app.Status.LastSubmissionAttemptTime.Time)
	observer.Observe(duration.Seconds())
	logger.V(1).Info("Observed spark application failure execution time seconds", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationFailureExecutionTimeSeconds, "labels", labels, "value", duration.Seconds())
}

func (m *SparkApplicationMetrics) observeStartLatencySeconds(app *v1beta2.SparkApplication) {
	// Only export the spark application start latency seconds metric for the first time
	if app.Status.ExecutionAttempts != 1 {
		return
	}

	labels := m.getMetricLabels(app)
	latency := time.Since(app.CreationTimestamp.Time)
	if observer, err := m.startLatencySeconds.GetMetricWith(labels); err != nil {
		logger.Error(err, "Failed to collect metric for SparkApplication", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationStartLatencySeconds, "labels", labels)
	} else {
		observer.Observe(latency.Seconds())
		logger.V(1).Info("Observed spark application start latency seconds", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationStartLatencySeconds, "labels", labels, "value", latency.Seconds())
	}

	if histogram, err := m.startLatencySecondsHistogram.GetMetricWith(labels); err != nil {
		logger.Error(err, "Failed to collect metric for SparkApplication", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationStartLatencySecondsHistogram, "labels", labels)
	} else {
		histogram.Observe(latency.Seconds())
		logger.V(1).Info("Observed spark application start latency seconds", "name", app.Name, "namespace", app.Namespace, "metric", common.MetricSparkApplicationStartLatencySecondsHistogram, "labels", labels, "value", latency.Seconds())
	}
}

func (m *SparkApplicationMetrics) getMetricLabels(app *v1beta2.SparkApplication) map[string]string {
	// Convert spark application validLabels to valid metric validLabels.
	validLabels := make(map[string]string)
	for key, val := range app.Labels {
		newKey := util.CreateValidMetricNameLabel(m.prefix, key)
		validLabels[newKey] = val
	}

	metricLabels := make(map[string]string)
	for _, label := range m.labels {
		if _, ok := validLabels[label]; ok {
			metricLabels[label] = validLabels[label]
		} else if label == "namespace" {
			metricLabels[label] = app.Namespace
		} else {
			metricLabels[label] = "Unknown"
		}
	}
	return metricLabels
}
