package util

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPositiveGauge_EmptyLabels(t *testing.T) {
	gauge := NewPositiveGauge("testGauge", "test-description", []string{})
	emptyMap := map[string]string{}
	gauge.Dec(emptyMap)
	assert.Equal(t, fetchGaugeValue(gauge.gaugeMetric, emptyMap), float64(0))

	gauge.Inc(emptyMap)
	assert.Equal(t, fetchGaugeValue(gauge.gaugeMetric, emptyMap), float64(1))
	gauge.Dec(map[string]string{})
	assert.Equal(t, fetchGaugeValue(gauge.gaugeMetric, emptyMap), float64(0))
}

func TestPositiveGauge_WithLabels(t *testing.T) {
	gauge := NewPositiveGauge("testGauge1", "test-description-1", []string{"app_id"})
	app1 := map[string]string{"app_id": "test1"}
	app2 := map[string]string{"app_id": "test2"}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		for i := 0; i < 10; i++ {
			gauge.Inc(app1)
		}
		for i := 0; i < 5; i++ {
			gauge.Dec(app1)
		}
		wg.Done()
	}()
	go func() {
		for i := 0; i < 5; i++ {
			gauge.Inc(app2)
		}
		for i := 0; i < 10; i++ {
			gauge.Dec(app2)
		}
		wg.Done()
	}()

	wg.Wait()
	assert.Equal(t, float64(5), fetchGaugeValue(gauge.gaugeMetric, app1))
	// Always Positive Gauge.
	assert.Equal(t, float64(0), fetchGaugeValue(gauge.gaugeMetric, app2))
}
