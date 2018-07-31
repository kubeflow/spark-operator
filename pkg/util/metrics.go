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
	"net/http"
	"strings"
	"sync"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	prometheus_model "github.com/prometheus/client_model/go"
	"k8s.io/client-go/util/workqueue"
)

func CreateValidMetricNameLabel(prefix, name string) string {
	// "-" aren't valid characters for prometheus metric names or labels
	return strings.Replace(prefix+name, "-", "_", -1)
}

// Best effort metric registration with Prometheus.
func RegisterMetric(metric prometheus.Collector) {
	if err := prometheus.Register(metric); err != nil {
		glog.Warningf("Error while registering Prometheus metric: [%v]", err)
	}
}

type PositiveGauge struct {
	mux         sync.RWMutex
	name        string
	gaugeMetric *prometheus.GaugeVec
}

// Gauge with conditional decrement ensuring its value is never negative.
func NewPositiveGauge(name string, description string, labels []string) *PositiveGauge {
	for i, label := range labels {
		labels[i] = CreateValidMetricNameLabel("", label)
	}

	gauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: name,
			Help: description,
		},
		labels,
	)

	RegisterMetric(gauge)

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

type WorkQueueMetrics struct {
	prefix string
}

func InitializeMetrics(endpoint string, port string, prefix string) {

	// Start the metrics endpoint for Prometheus to scrape
	http.Handle(endpoint, promhttp.Handler())
	go http.ListenAndServe(port, nil)
	glog.Infof("Started Metrics server at localhost%s%s", port, endpoint)

	workQueueMetrics := WorkQueueMetrics{prefix: prefix}
	workqueue.SetProvider(&workQueueMetrics)
}

// Depth Metric for kubernetes workqueue
func (p *WorkQueueMetrics) NewDepthMetric(name string) workqueue.GaugeMetric {
	depthMetric := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: CreateValidMetricNameLabel(p.prefix, name+"_depth"),
		Help: "Current depth of workqueue: " + name,
	},
	)
	RegisterMetric(depthMetric)
	return depthMetric
}

// Adds Count Metrics for kubernetes workqueue
func (p *WorkQueueMetrics) NewAddsMetric(name string) workqueue.CounterMetric {
	addsMetric := prometheus.NewCounter(prometheus.CounterOpts{
		Name: CreateValidMetricNameLabel(p.prefix, name+"_adds"),
		Help: "Total number of adds handled by workqueue: " + name,
	})
	RegisterMetric(addsMetric)
	return addsMetric
}

// Latency Metric for kubernetes workqueue
func (p *WorkQueueMetrics) NewLatencyMetric(name string) workqueue.SummaryMetric {
	latencyMetric := prometheus.NewSummary(prometheus.SummaryOpts{
		Name: CreateValidMetricNameLabel(p.prefix, name+"_latency"),
		Help: "Latency for workqueue: " + name,
	})
	RegisterMetric(latencyMetric)
	return latencyMetric
}

// WorkDuration Metric for kubernetes workqueue
func (p *WorkQueueMetrics) NewWorkDurationMetric(name string) workqueue.SummaryMetric {
	workDurationMetric := prometheus.NewSummary(prometheus.SummaryOpts{
		Name: CreateValidMetricNameLabel(p.prefix, name+"_work_duration"),
		Help: "How long processing an item from workqueue" + name + " takes.",
	})
	RegisterMetric(workDurationMetric)
	return workDurationMetric
}

// Retry Metric for kubernetes workqueue
func (p *WorkQueueMetrics) NewRetriesMetric(name string) workqueue.CounterMetric {
	retriesMetrics := prometheus.NewCounter(prometheus.CounterOpts{
		Name: CreateValidMetricNameLabel(p.prefix, name+"_retries"),
		Help: "Total number of retries handled by workqueue: " + name,
	})
	RegisterMetric(retriesMetrics)
	return retriesMetrics
}
