/*
Copyright 2024 The Kubeflow Authors.

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

package metrics

import (
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

// defaultEndpoint is always served by the controller-runtime metrics server and cannot be overridden.
const defaultEndpoint = "/metrics"

var (
	logger = log.Log.WithName("")
)

// ExtraHandlers returns the handlers that serve the metrics at a non-default endpoint.
func ExtraHandlers(endpoint string) map[string]http.Handler {
	if endpoint == "" {
		return nil
	}
	if !strings.HasPrefix(endpoint, "/") {
		endpoint = "/" + endpoint
	}
	if endpoint == defaultEndpoint {
		return nil
	}
	return map[string]http.Handler{
		endpoint: promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{
			ErrorHandling: promhttp.HTTPErrorOnError,
		}),
	}
}
