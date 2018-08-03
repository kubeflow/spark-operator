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
	// Test with label containing "-". Expect them to be converted to "_".
	metrics := newSparkAppMetrics("", []string{"app-name"})
	app1 := map[string]string{"app_name": "test1"}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for i := 0; i < 10; i++ {
			metrics.sparkAppSubmitCount.With(app1).Inc()
			metrics.sparkAppRunningCount.Inc(app1)
			metrics.sparkAppFailureCount.With(app1).Inc()
			metrics.sparkAppSuccessCount.With(app1).Inc()
			metrics.sparkAppExecutorFailureCount.With(app1).Inc()
			metrics.sparkAppSuccessExecutionTime.With(app1).Observe(float64(100 * i))
			metrics.sparkAppFailureExecutionTime.With(app1).Observe(float64(500 * i))
			metrics.sparkAppExecutorRunningCount.Inc(app1)
			metrics.sparkAppExecutorSuccessCount.With(app1).Inc()
		}
		for i := 0; i < 5; i++ {
			metrics.sparkAppRunningCount.Dec(app1)
			metrics.sparkAppExecutorRunningCount.Dec(app1)
		}
		wg.Done()
	}()

	wg.Wait()
	assert.Equal(t, float64(10), fetchCounterValue(metrics.sparkAppSubmitCount, app1))
	assert.Equal(t, float64(10), fetchCounterValue(metrics.sparkAppFailureCount, app1))
	assert.Equal(t, float64(10), fetchCounterValue(metrics.sparkAppSuccessCount, app1))
	assert.Equal(t, float64(10), fetchCounterValue(metrics.sparkAppExecutorFailureCount, app1))
	assert.Equal(t, float64(10), fetchCounterValue(metrics.sparkAppExecutorSuccessCount, app1))
	assert.Equal(t, float64(5), metrics.sparkAppExecutorRunningCount.Value(app1))
	assert.Equal(t, float64(5), metrics.sparkAppRunningCount.Value(app1))
}
