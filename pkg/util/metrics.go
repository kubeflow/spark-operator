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

package util

import (
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	prometheusmodel "github.com/prometheus/client_model/go"

	"k8s.io/client-go/util/workqueue"
)

func CreateValidMetricNameLabel(prefix, name string) string {
	// "-" is not a valid character for prometheus metric names or labels.
	return strings.Replace(prefix+name, "-", "_", -1)
}

// Best effort metric registration with Prometheus.
func RegisterMetric(metric prometheus.Collector) {
	if err := prometheus.Register(metric); err != nil {
		// Ignore AlreadyRegisteredError.
		if _, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return
		}
		glog.Errorf("failed to register metric: %v", err)
	}
}

// MetricConfig is a container of configuration properties for the collection and exporting of
// application metrics to Prometheus.
type MetricConfig struct {
	MetricsEndpoint               string
	MetricsPort                   string
	MetricsPrefix                 string
	MetricsLabels                 []string
	MetricsJobStartLatencyBuckets []float64
}

// A variant of Prometheus Gauge that only holds non-negative values.
type PositiveGauge struct {
	mux         sync.RWMutex
	name        string
	gaugeMetric *prometheus.GaugeVec
}

func NewPositiveGauge(name string, description string, labels []string) *PositiveGauge {
	validLabels := make([]string, len(labels))
	for i, label := range labels {
		validLabels[i] = CreateValidMetricNameLabel("", label)
	}

	gauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: name,
			Help: description,
		},
		validLabels,
	)

	return &PositiveGauge{
		gaugeMetric: gauge,
		name:        name,
	}
}

func fetchGaugeValue(m *prometheus.GaugeVec, labels map[string]string) float64 {
	// Hack to get the current value of the metric to support PositiveGauge
	pb := &prometheusmodel.Metric{}

	m.With(labels).Write(pb)
	return pb.GetGauge().GetValue()
}

func (p *PositiveGauge) Register() {
	RegisterMetric(p.gaugeMetric)
}

func (p *PositiveGauge) Value(labelMap map[string]string) float64 {
	p.mux.RLock()
	defer p.mux.RUnlock()
	return fetchGaugeValue(p.gaugeMetric, labelMap)
}

// Increment the Metric for the labels specified
func (p *PositiveGauge) Inc(labelMap map[string]string) {
	p.mux.Lock()
	defer p.mux.Unlock()

	if m, err := p.gaugeMetric.GetMetricWith(labelMap); err != nil {
		glog.Errorf("Error while exporting metrics: %v", err)
	} else {
		glog.V(2).Infof("Incrementing %s with labels %s", p.name, labelMap)
		m.Inc()
	}
}

// Decrement the metric only if its positive for the labels specified
func (p *PositiveGauge) Dec(labelMap map[string]string) {
	p.mux.Lock()
	defer p.mux.Unlock()

	// Decrement only if positive
	val := fetchGaugeValue(p.gaugeMetric, labelMap)
	if val > 0 {
		glog.V(2).Infof("Decrementing %s with labels %s metricVal to %v", p.name, labelMap, val-1)
		if m, err := p.gaugeMetric.GetMetricWith(labelMap); err != nil {
			glog.Errorf("Error while exporting metrics: %v", err)
		} else {
			m.Dec()
		}
	}
}

type WorkQueueMetrics struct {
	prefix string
}

func InitializeMetrics(metricsConfig *MetricConfig) {
	// Start the metrics endpoint for Prometheus to scrape
	http.Handle(metricsConfig.MetricsEndpoint, promhttp.Handler())
	go http.ListenAndServe(fmt.Sprintf(":%s", metricsConfig.MetricsPort), nil)
	glog.Infof("Started Metrics server at localhost:%s%s", metricsConfig.MetricsPort, metricsConfig.MetricsEndpoint)

	workQueueMetrics := WorkQueueMetrics{prefix: metricsConfig.MetricsPrefix}
	workqueue.SetProvider(&workQueueMetrics)
}

// Depth Metric for the kubernetes workqueue.
func (p *WorkQueueMetrics) NewDepthMetric(name string) workqueue.GaugeMetric {
	depthMetric := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: CreateValidMetricNameLabel(p.prefix, name+"_depth"),
		Help: fmt.Sprintf("Current depth of workqueue: %s", name),
	},
	)
	RegisterMetric(depthMetric)
	return depthMetric
}

// Adds Count Metrics for the kubernetes workqueue.
func (p *WorkQueueMetrics) NewAddsMetric(name string) workqueue.CounterMetric {
	addsMetric := prometheus.NewCounter(prometheus.CounterOpts{
		Name: CreateValidMetricNameLabel(p.prefix, name+"_adds"),
		Help: fmt.Sprintf("Total number of adds handled by workqueue: %s", name),
	})
	RegisterMetric(addsMetric)
	return addsMetric
}

// Latency Metric for the kubernetes workqueue.
func (p *WorkQueueMetrics) NewLatencyMetric(name string) workqueue.HistogramMetric {
	latencyMetric := prometheus.NewSummary(prometheus.SummaryOpts{
		Name: CreateValidMetricNameLabel(p.prefix, name+"_latency"),
		Help: fmt.Sprintf("Latency for workqueue: %s", name),
	})
	RegisterMetric(latencyMetric)
	return latencyMetric
}

// WorkDuration Metric for the kubernetes workqueue.
func (p *WorkQueueMetrics) NewWorkDurationMetric(name string) workqueue.HistogramMetric {
	workDurationMetric := prometheus.NewSummary(prometheus.SummaryOpts{
		Name: CreateValidMetricNameLabel(p.prefix, name+"_work_duration"),
		Help: fmt.Sprintf("How long processing an item from workqueue %s takes.", name),
	})
	RegisterMetric(workDurationMetric)
	return workDurationMetric
}

// Retry Metric for the kubernetes workqueue.
func (p *WorkQueueMetrics) NewRetriesMetric(name string) workqueue.CounterMetric {
	retriesMetrics := prometheus.NewCounter(prometheus.CounterOpts{
		Name: CreateValidMetricNameLabel(p.prefix, name+"_retries"),
		Help: fmt.Sprintf("Total number of retries handled by workqueue: %s", name),
	})
	RegisterMetric(retriesMetrics)
	return retriesMetrics
}

func (p *WorkQueueMetrics) NewUnfinishedWorkSecondsMetric(name string) workqueue.SettableGaugeMetric {
	unfinishedWorkSecondsMetric := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: CreateValidMetricNameLabel(p.prefix, name+"_unfinished_work_seconds"),
		Help: fmt.Sprintf("Unfinished work seconds: %s", name),
	},
	)
	RegisterMetric(unfinishedWorkSecondsMetric)
	return unfinishedWorkSecondsMetric
}

func (p *WorkQueueMetrics) NewLongestRunningProcessorSecondsMetric(name string) workqueue.SettableGaugeMetric {
	longestRunningProcessorMicrosecondsMetric := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: CreateValidMetricNameLabel(p.prefix, name+"_longest_running_processor_microseconds"),
		Help: fmt.Sprintf("Longest running processor microseconds: %s", name),
	},
	)
	RegisterMetric(longestRunningProcessorMicrosecondsMetric)
	return longestRunningProcessorMicrosecondsMetric
}
