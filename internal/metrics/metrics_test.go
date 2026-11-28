/*
Copyright The Kubeflow Authors.

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
	for _, endpoint := range []string{"", " ", "/metrics", "metrics", " /metrics ", "/%6detrics"} {
		handlers, err := metrics.ExtraHandlers(endpoint)
		require.NoError(t, err, "endpoint %q", endpoint)
		assert.Nil(t, handlers, "endpoint %q", endpoint)
	}
}

func TestExtraHandlersEncodedDefaultEndpoint(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", http.NotFoundHandler())

	// The mux unescapes its patterns, so this spelling would conflict with /metrics.
	handlers, err := metrics.ExtraHandlers("/%6detrics")
	require.NoError(t, err)
	require.NotPanics(t, func() {
		for path, handler := range handlers {
			mux.Handle(path, handler)
		}
	})
}

func TestExtraHandlersInvalidEndpoint(t *testing.T) {
	for _, endpoint := range []string{"/custom metrics", "/custom\tmetrics", "/custom{metrics", "/{name}/{name}"} {
		handlers, err := metrics.ExtraHandlers(endpoint)
		require.ErrorContains(t, err, "invalid metrics endpoint", "endpoint %q", endpoint)
		assert.Nil(t, handlers, "endpoint %q", endpoint)
	}
}

func TestExtraHandlersCustomEndpoint(t *testing.T) {
	counter := prometheus.NewCounter(prometheus.CounterOpts{Name: "spark_operator_extra_handlers_test_total"})
	require.NoError(t, ctrlmetrics.Registry.Register(counter))
	t.Cleanup(func() { ctrlmetrics.Registry.Unregister(counter) })

	for _, endpoint := range []string{"/custom-metrics", "custom-metrics", "/custom-metrics ", " custom-metrics\n"} {
		handlers, err := metrics.ExtraHandlers(endpoint)
		require.NoError(t, err, "endpoint %q", endpoint)
		require.Len(t, handlers, 1, "endpoint %q", endpoint)
		handler, ok := handlers["/custom-metrics"]
		require.True(t, ok, "endpoint %q", endpoint)

		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/custom-metrics", nil))
		assert.Equal(t, http.StatusOK, recorder.Code)
		assert.Contains(t, recorder.Body.String(), "spark_operator_extra_handlers_test_total")
	}
}
