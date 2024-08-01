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
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
)

type SparkExecutorMetrics struct {
	prefix string
	labels []string

	runningCount *prometheus.GaugeVec
	successCount *prometheus.CounterVec
	failureCount *prometheus.CounterVec
}

func NewSparkExecutorMetrics(prefix string, labels []string) *SparkExecutorMetrics {
	validLabels := make([]string, 0, len(labels))
	for _, label := range labels {
		validLabel := util.CreateValidMetricNameLabel("", label)
		validLabels = append(validLabels, validLabel)
	}

	return &SparkExecutorMetrics{
		prefix: prefix,
		labels: labels,

		runningCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: util.CreateValidMetricNameLabel(prefix, common.MetricSparkExecutorRunningCount),
				Help: "Total number of running Spark executors",
			},
			validLabels,
		),
		successCount: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: util.CreateValidMetricNameLabel(prefix, common.MetricSparkExecutorSuccessCount),
				Help: "Total number of successful Spark executors",
			},
			validLabels,
		),
		failureCount: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: util.CreateValidMetricNameLabel(prefix, common.MetricSparkExecutorFailureCount),
				Help: "Total number of failed Spark executors",
			},
			validLabels,
		),
	}
}

func (m *SparkExecutorMetrics) Register() {
	if err := metrics.Registry.Register(m.runningCount); err != nil {
		logger.Error(err, "Failed to register spark executor metric", "name", common.MetricSparkExecutorRunningCount)
	}
	if err := metrics.Registry.Register(m.successCount); err != nil {
		logger.Error(err, "Failed to register spark executor metric", "name", common.MetricSparkExecutorSuccessCount)
	}
	if err := metrics.Registry.Register(m.failureCount); err != nil {
		logger.Error(err, "Failed to register spark executor metric", "name", common.MetricSparkExecutorFailureCount)
	}
}

func (m *SparkExecutorMetrics) HandleSparkExecutorCreate(pod *corev1.Pod) {
	state := util.GetExecutorState(pod)
	switch state {
	case v1beta2.ExecutorStateRunning:
		m.incRunningCount(pod)
	}
}

func (m *SparkExecutorMetrics) HandleSparkExecutorUpdate(oldPod, newPod *corev1.Pod) {
	oldState := util.GetExecutorState(oldPod)
	newState := util.GetExecutorState(newPod)
	if newState == oldState {
		return
	}

	switch oldState {
	case v1beta2.ExecutorStateRunning:
		m.decRunningCount(oldPod)
	}

	switch newState {
	case v1beta2.ExecutorStateRunning:
		m.incRunningCount(newPod)
	case v1beta2.ExecutorStateCompleted:
		m.incSuccessCount(newPod)
	case v1beta2.ExecutorStateFailed:
		m.incFailureCount(newPod)
	}
}

func (m *SparkExecutorMetrics) HandleSparkExecutorDelete(pod *corev1.Pod) {
	state := util.GetExecutorState(pod)

	switch state {
	case v1beta2.ExecutorStateRunning:
		m.decRunningCount(pod)
	}
}

func (m *SparkExecutorMetrics) incRunningCount(pod *corev1.Pod) {
	labels := m.getMetricLabels(pod)
	runningCount, err := m.runningCount.GetMetricWith(labels)
	if err != nil {
		logger.Error(err, "Failed to collect metric for Spark executor", "name", pod.Name, "namespace", pod.Namespace, "metric", common.MetricSparkExecutorRunningCount, "labels", labels)
		return
	}

	runningCount.Inc()
	logger.V(1).Info("Increased Spark executor running count", "name", pod.Name, "namespace", pod.Namespace, "metric", common.MetricSparkExecutorRunningCount, "labels", labels)
}

func (m *SparkExecutorMetrics) decRunningCount(pod *corev1.Pod) {
	labels := m.getMetricLabels(pod)
	runningCount, err := m.runningCount.GetMetricWith(labels)
	if err != nil {
		logger.Error(err, "Failed to collect metric for Spark executor", "name", pod.Name, "namespace", pod.Namespace, "metric", common.MetricSparkExecutorRunningCount, "labels", labels)
		return
	}

	runningCount.Dec()
	logger.V(1).Info("Decreased Spark executor running count", "name", pod.Name, "namespace", pod.Namespace, "metric", common.MetricSparkExecutorRunningCount, "labels", labels)
}

func (m *SparkExecutorMetrics) incSuccessCount(pod *corev1.Pod) {
	labels := m.getMetricLabels(pod)
	successCount, err := m.successCount.GetMetricWith(labels)
	if err != nil {
		logger.Error(err, "Failed to collect metric for Spark executor", "name", pod.Name, "namespace", pod.Namespace, "metric", common.MetricSparkExecutorSuccessCount, "labels", labels)
		return
	}

	successCount.Inc()
	logger.V(1).Info("Increased Spark executor success count", "name", pod.Name, "namespace", pod.Namespace, "metric", common.MetricSparkExecutorSuccessCount, "labels", labels)
}

func (m *SparkExecutorMetrics) incFailureCount(pod *corev1.Pod) {
	labels := m.getMetricLabels(pod)
	failureCount, err := m.failureCount.GetMetricWith(labels)
	if err != nil {
		logger.Error(err, "Failed to collect metric for Spark executor", "name", pod.Name, "namespace", pod.Namespace, "metric", common.MetricSparkExecutorFailureCount, "labels", labels)
		return
	}

	failureCount.Inc()
	logger.V(1).Info("Increased Spark executor running count", "name", pod.Name, "namespace", pod.Namespace, "metric", common.MetricSparkExecutorFailureCount, "labels", labels)
}

func (m *SparkExecutorMetrics) getMetricLabels(pod *corev1.Pod) map[string]string {
	// Convert pod metricLabels to valid metric metricLabels.
	validLabels := make(map[string]string)
	for key, val := range pod.Labels {
		newKey := util.CreateValidMetricNameLabel("", key)
		validLabels[newKey] = val
	}

	metricLabels := make(map[string]string)
	for _, label := range m.labels {
		if _, ok := validLabels[label]; ok {
			metricLabels[label] = validLabels[label]
		} else if label == "namespace" {
			metricLabels[label] = pod.Namespace
		} else {
			metricLabels[label] = "Unknown"
		}
	}
	return metricLabels
}
