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
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSparkAppMetrics(t *testing.T) {
	http.DefaultServeMux = new(http.ServeMux)
	// Invalid label.
	metrics := newSparkAppMetrics("", []string{"app-name"})
	app1 := map[string]string{"app_name": "test1"}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for i := 0; i < 10; i++ {
			metrics.SparkAppSubmitCount.With(app1).Inc()
			metrics.SparkAppRunningCount.Inc(app1)
			metrics.SparkAppFailureCount.With(app1).Inc()
			metrics.SparkAppSuccessCount.With(app1).Inc()
			metrics.SparkAppExecutorFailureCount.With(app1).Inc()
			metrics.SparkAppSuccessExecutionTime.With(app1).Observe(float64(100 * i))
			metrics.SparkAppFailureExecutionTime.With(app1).Observe(float64(500 * i))
			metrics.SparkAppExecutorRunningCount.Inc(app1)
			metrics.SparkAppExecutorSuccessCount.With(app1).Inc()

		}
		for i := 0; i < 5; i++ {
			metrics.SparkAppRunningCount.Dec(app1)
			metrics.SparkAppExecutorRunningCount.Dec(app1)
		}
		wg.Done()
	}()

	wg.Wait()
	assert.Equal(t, float64(10), fetchCounterValue(metrics.SparkAppSubmitCount, app1))
	assert.Equal(t, float64(10), fetchCounterValue(metrics.SparkAppFailureCount, app1))
	assert.Equal(t, float64(10), fetchCounterValue(metrics.SparkAppSuccessCount, app1))
	assert.Equal(t, float64(10), fetchCounterValue(metrics.SparkAppExecutorFailureCount, app1))
	assert.Equal(t, float64(10), fetchCounterValue(metrics.SparkAppExecutorSuccessCount, app1))
	assert.Equal(t, float64(5), metrics.SparkAppExecutorRunningCount.Value(app1))
	assert.Equal(t, float64(5), metrics.SparkAppRunningCount.Value(app1))

}
