/*
Copyright 2026 The Kubeflow Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package metrics_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/kubeflow/spark-operator/v2/internal/metrics"
)

func TestExtraHandlersDefaultEndpoint(t *testing.T) {
	for _, endpoint := range []string{"", "/metrics", "metrics"} {
		assert.Nil(t, metrics.ExtraHandlers(endpoint), "endpoint %q", endpoint)
	}
}

func TestExtraHandlersCustomEndpoint(t *testing.T) {
	counter := prometheus.NewCounter(prometheus.CounterOpts{Name: "spark_operator_extra_handlers_test_total"})
	require.NoError(t, ctrlmetrics.Registry.Register(counter))
	t.Cleanup(func() { ctrlmetrics.Registry.Unregister(counter) })

	for _, endpoint := range []string{"/custom-metrics", "custom-metrics"} {
		handlers := metrics.ExtraHandlers(endpoint)
		require.Len(t, handlers, 1, "endpoint %q", endpoint)
		handler, ok := handlers["/custom-metrics"]
		require.True(t, ok, "endpoint %q", endpoint)

		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/custom-metrics", nil))
		assert.Equal(t, http.StatusOK, recorder.Code)
		assert.Contains(t, recorder.Body.String(), "spark_operator_extra_handlers_test_total")
	}
}
