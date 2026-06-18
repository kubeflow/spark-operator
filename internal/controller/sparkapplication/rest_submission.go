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

// REST Submitter Client
//
// Submits SparkApplications to the k8s-spark-submitter service (POST /api/v1/spark-submit).
//
// # Contract
//
//  1. Submission-only, stateless. The submitter creates the driver pod and returns its identity.
//     It does not track, monitor, restart, or delete applications. All lifecycle and status
//     ownership remains with the operator via Kubernetes owner references.
//  2. The request carries the same spark-submit argument vector the operator already produces
//     via buildSparkSubmitArgs. There is no parallel spec schema.
//  3. Every submission carries a client-supplied submission_id for correlation and operator-side
//     idempotency. The client is responsible for providing a unique id; conflicts lead to failures.
//  4. Pod templates are sent inline as JSON. The operator omits podTemplateFile --conf entries
//     from spark_submit_args; the submitter merges the inline template with the args exactly as
//     Spark does when loading a podTemplateFile (template is base, --conf values layered on top).
//
// # Idempotency
//
// The submitter is stateless — it creates the driver pod via Spark libraries and returns
// DRIVER_POD_ALREADY_EXISTS if the pod already exists. It does not track prior submissions.
//
// The operator achieves idempotency by comparing the returned submission_id with its own:
//   - Match → retry of a completed submission, treat as success.
//   - Mismatch → genuine conflict, treat as failure.
//
// # Request Payload
//
//	POST /api/v1/spark-submit
//	Content-Type: application/json
//
//	{
//	  "submission_id": "550e8400-e29b-41d4-a716-446655440000",
//	  "spark_submit_args": ["--master", "k8s://...", "--deploy-mode", "cluster", "..."],
//	  "driver_pod_template": { "metadata": {}, "spec": {} },
//	  "executor_pod_template": { "metadata": {}, "spec": {} }
//	}
//
//	| Field                  | Type                   | Required | Notes                                                  |
//	| ---------------------- | ---------------------- | -------- | ------------------------------------------------------ |
//	| submission_id          | string                 | yes      | Correlation + idempotency key. From Status.SubmissionID |
//	| spark_submit_args      | []string               | yes      | Verbatim spark-submit args                             |
//	| driver_pod_template    | object (PodTemplateSpec)| no      | Inline driver template (omits podTemplateFile conf)    |
//	| executor_pod_template  | object (PodTemplateSpec)| no      | Inline executor template                               |
//
// # Success Response (201 Created)
//
//	{
//	  "submission_id": "550e8400-e29b-41d4-a716-446655440000",
//	  "app_name": "spark-pi",
//	  "spark_app_id": "spark-b992db7da52c42298736dcbb3c9142be",
//	  "driver_pod_name": "spark-pi-abc123-driver",
//	  "driver_pod_uid": "0c1f...",
//	  "namespace": "spark-jobs",
//	  "submitted_at": "2026-06-16T08:30:00Z"
//	}
//
//	| Field              | Type   | Notes                                                                |
//	| ------------------ | ------ | -------------------------------------------------------------------- |
//	| submission_id      | string | Echoes the request value                                             |
//	| spark_app_id       | string | Spark application id assigned to the submission                      |
//	| driver_pod_name    | string | Name of the created driver pod                                       |
//	| driver_pod_uid     | string | UID of the driver pod; lets the operator bind to the exact object    |
//
//	Semantics of 201: the driver pod object was persisted to the API server. It does not assert
//	the driver was scheduled, started, or succeeded.
//
// # Error Response
//
//	{
//	  "submission_id": "550e8400-e29b-41d4-a716-446655440000",
//	  "status": 422,
//	  "error_code": "POD_TEMPLATE_INVALID",
//	  "message": "Driver template missing 'spark-kubernetes-driver' container",
//	  "timestamp": "2026-06-16T08:30:00Z"
//	}
//
//	| error_code                 | HTTP | Retry? | Meaning                                            |
//	| -------------------------- | ---- | ------ | -------------------------------------------------- |
//	| BAD_REQUEST                | 400  | No     | Malformed JSON or missing required fields           |
//	| INVALID_SPARK_SUBMIT_ARGS  | 400  | No     | spark_submit_args failed Spark argument parsing     |
//	| METHOD_NOT_ALLOWED         | 405  | No     | Non-POST request                                   |
//	| UNSUPPORTED_MEDIA_TYPE     | 415  | No     | Content-Type is not application/json                |
//	| DRIVER_POD_ALREADY_EXISTS  | 422  | No     | Pod name conflict (K8s 409)                        |
//	| INVALID_POD_TEMPLATE       | 422  | No     | Template structurally invalid (K8s 422)            |
//	| INTERNAL_SERVER_ERROR      | 500  | Yes    | Unexpected server failure                          |
//	| SUBMITTER_OVERLOADED       | 503  | Yes    | Submitter at capacity; back off and retry          |

package sparkapplication

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

const (
	contentTypeJSON          = "application/json"
	maxResponseBodySizeBytes = 1048576 // 1MB
	maxErrorBodyLength       = 200
	startupPollInterval      = 1 * time.Second
	maxIdleConns             = 100
	maxIdleConnsPerHost      = 100
	idleConnTimeout          = 90 * time.Second
)

// --- Error codes and types ---

// SubmitErrorCode represents the structured error codes returned by the submitter service.
// See the error_code table in the file-level doc for the full mapping.
type SubmitErrorCode string

const (
	ErrorCodeBadRequest             SubmitErrorCode = "BAD_REQUEST"
	ErrorCodeInvalidSparkSubmitArgs SubmitErrorCode = "INVALID_SPARK_SUBMIT_ARGS"
	ErrorCodeMethodNotAllowed       SubmitErrorCode = "METHOD_NOT_ALLOWED"
	ErrorCodeUnsupportedMediaType   SubmitErrorCode = "UNSUPPORTED_MEDIA_TYPE"
	ErrorCodeDriverPodAlreadyExists SubmitErrorCode = "DRIVER_POD_ALREADY_EXISTS"
	ErrorCodeInvalidPodTemplate     SubmitErrorCode = "INVALID_POD_TEMPLATE"
	ErrorCodeInternalServerError    SubmitErrorCode = "INTERNAL_SERVER_ERROR"
	ErrorCodeSubmitterOverloaded    SubmitErrorCode = "SUBMITTER_OVERLOADED"
)

// SubmitError wraps a submitter error response with the structured error code.
type SubmitError struct {
	SubmissionID string
	Code         SubmitErrorCode
	StatusCode   int
	Message      string
}

func (e *SubmitError) Error() string {
	return fmt.Sprintf("submitter service returned error: %s (HTTP %d, error code: %s)", e.Message, e.StatusCode, e.Code)
}

func (e *SubmitError) IsRetryable() bool {
	return e.Code == ErrorCodeSubmitterOverloaded || e.Code == ErrorCodeInternalServerError
}

func (e *SubmitError) IsRetryOfCompletedSubmission(submissionID string) bool {
	return e.Code == ErrorCodeDriverPodAlreadyExists && e.SubmissionID == submissionID
}

// --- Config and types ---

var restLogger = ctrl.Log.WithName("rest-submitter")

// TLSConfig holds TLS settings for the submitter REST client.
type TLSConfig struct {
	Enabled    bool
	CertFile   string
	KeyFile    string
	CACertFile string
}

// RestSparkSubmitterConfig holds submitter connection settings.
type RestSparkSubmitterConfig struct {
	URL             string
	RetryMaxRetries int
	RequestTimeout  time.Duration
	InitialBackoff  time.Duration
	TLS             *TLSConfig
}

// RestSparkSubmitter submits a SparkApplication via the REST submitter service.
type RestSparkSubmitter struct {
	httpClient     *http.Client
	submitURL      string
	hostAddr       string
	maxRetries     int
	initialBackoff time.Duration
}

var _ SparkApplicationSubmitter = &RestSparkSubmitter{}

// --- Request/Response payloads ---

type submitRequest struct {
	SubmissionID        string                  `json:"submission_id"`
	SparkSubmitArgs     []string                `json:"spark_submit_args"`
	DriverPodTemplate   *corev1.PodTemplateSpec `json:"driver_pod_template,omitempty"`
	ExecutorPodTemplate *corev1.PodTemplateSpec `json:"executor_pod_template,omitempty"`
}

type submitResponse struct {
	SubmissionID  string `json:"submission_id"`
	AppName       string `json:"app_name"`
	Message       string `json:"message"`
	SubmittedAt   string `json:"submitted_at"`
	SparkAppID    string `json:"spark_app_id"`
	DriverPodName string `json:"driver_pod_name"`
	DriverPodUID  string `json:"driver_pod_uid"`
	Namespace     string `json:"namespace"`
}

type submitErrorResponse struct {
	SubmissionID string          `json:"submission_id"`
	Status       int             `json:"status"`
	ErrorCode    SubmitErrorCode `json:"error_code"`
	Message      string          `json:"message"`
	Timestamp    string          `json:"timestamp"`
}

// --- Constructor ---

// NewRestSparkSubmitter creates a REST client for the submitter service.
func NewRestSparkSubmitter(cfg RestSparkSubmitterConfig) (*RestSparkSubmitter, error) {
	submitURL := strings.TrimSpace(cfg.URL)
	if submitURL == "" {
		return nil, fmt.Errorf("submitter URL is required")
	}

	u, err := url.Parse(submitURL)
	if err != nil {
		return nil, fmt.Errorf("invalid submitter URL %q: %w", cfg.URL, err)
	}
	if u.Port() == "" {
		return nil, fmt.Errorf("submitter URL %q must include an explicit port", cfg.URL)
	}

	transport, err := buildTransport(cfg.TLS)
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	return &RestSparkSubmitter{
		httpClient:     &http.Client{Timeout: cfg.RequestTimeout, Transport: transport},
		submitURL:      submitURL,
		hostAddr:       u.Host,
		maxRetries:     cfg.RetryMaxRetries,
		initialBackoff: cfg.InitialBackoff,
	}, nil
}

// --- Public methods ---

// WaitForConnection blocks until the submitter service responds to an HTTP request (verifying
// both network connectivity and TLS handshake) or the context expires.
func (c *RestSparkSubmitter) WaitForConnection(ctx context.Context) error {
	restLogger.Info("Waiting for submitter service to be ready", "addr", c.hostAddr)
	for {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, c.submitURL, nil)
		resp, err := c.httpClient.Do(req)
		if err == nil {
			resp.Body.Close()
			restLogger.Info("Submitter service is ready", "addr", c.hostAddr)
			return nil
		}
		restLogger.V(1).Info("Submitter service not yet ready, retrying", "addr", c.hostAddr, "error", err)

		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for submitter service at %s: %w", c.hostAddr, ctx.Err())
		case <-time.After(startupPollInterval):
		}
	}
}

// Submit implements SparkApplicationSubmitter interface.
func (c *RestSparkSubmitter) Submit(ctx context.Context, app *v1beta2.SparkApplication) error {
	args, err := buildSparkSubmitArgs(app, true)
	if err != nil {
		return fmt.Errorf("failed to build spark-submit arguments: %v", err)
	}

	request := newSubmitRequest(args, app)
	restLogger.Info("Submitting spark application via RestSubmitter", "name", app.Name, "namespace", app.Namespace, "submissionId", request.SubmissionID)

	response, err := c.doSubmitWithRetry(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to submit Spark application %s/%s: %w", app.Namespace, app.Name, err)
	}

	restLogger.Info("Submitted successfully",
		"name", app.Name,
		"submissionId", response.SubmissionID,
		"driverPod", response.DriverPodName,
		"sparkAppId", response.SparkAppID,
	)
	return nil
}

// --- Submission internals ---

func newSubmitRequest(args []string, app *v1beta2.SparkApplication) *submitRequest {
	return &submitRequest{
		SubmissionID:        app.Status.SubmissionID,
		SparkSubmitArgs:     args,
		DriverPodTemplate:   app.Spec.Driver.Template,
		ExecutorPodTemplate: app.Spec.Executor.Template,
	}
}

// doSubmitWithRetry submits the request with exponential backoff.
// If the submitter returns DRIVER_POD_ALREADY_EXISTS for the same submission_id,
// it is treated as idempotent success (the prior attempt created the pod).
func (c *RestSparkSubmitter) doSubmitWithRetry(ctx context.Context, request *submitRequest) (*submitResponse, error) {
	jsonData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal submission request to JSON: %w", err)
	}

	var lastErr error
	backoff := c.initialBackoff

	for attempt := 0; attempt < c.maxRetries; attempt++ {
		result, postErr := c.tryPost(ctx, jsonData)
		if postErr == nil {
			return result, nil
		}
		lastErr = postErr

		var submitErr *SubmitError
		if errors.As(lastErr, &submitErr) && submitErr.IsRetryOfCompletedSubmission(request.SubmissionID) {
			restLogger.Info("Driver pod already exists for this submission, treating as success",
				"submissionId", request.SubmissionID)
			return &submitResponse{SubmissionID: request.SubmissionID}, nil
		}

		if !c.canRetry(attempt, lastErr) {
			return nil, lastErr
		}

		backoff, err = c.waitForRetry(ctx, attempt, backoff, lastErr)
		if err != nil {
			return nil, err
		}
	}

	return nil, lastErr
}

func (c *RestSparkSubmitter) tryPost(ctx context.Context, jsonData []byte) (*submitResponse, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.submitURL, bytes.NewReader(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request to %s: %w", c.submitURL, err)
	}
	httpReq.Header.Set("Content-Type", contentTypeJSON)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send HTTP request to %s: %w", c.submitURL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	return parseSubmitResponse(resp)
}

func (c *RestSparkSubmitter) canRetry(attempt int, err error) bool {
	if attempt >= c.maxRetries-1 {
		return false
	}
	var submitErr *SubmitError
	if errors.As(err, &submitErr) {
		return submitErr.IsRetryable()
	}
	// Network and TLS errors (connection refused, timeout, cert reload) are transient and retryable.
	return true
}

func (c *RestSparkSubmitter) waitForRetry(ctx context.Context, attempt int, backoff time.Duration, lastErr error) (time.Duration, error) {
	restLogger.V(1).Info("Retrying submission", "attempt", attempt+1, "maxRetries", c.maxRetries, "backoff", backoff, "error", lastErr)
	select {
	case <-ctx.Done():
		return backoff, fmt.Errorf("retry cancelled: %w", ctx.Err())
	case <-time.After(backoff):
		return backoff * 2, nil
	}
}

// --- Response parsing ---

func parseSubmitResponse(resp *http.Response) (*submitResponse, error) {
	statusCode := resp.StatusCode
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodySizeBytes))

	if readErr != nil {
		return nil, fmt.Errorf("failed to read response body from submitter service (status %d): %w", statusCode, readErr)
	}

	if statusCode == http.StatusCreated {
		var r submitResponse
		if err := json.Unmarshal(body, &r); err != nil {
			return nil, fmt.Errorf("failed to parse success response from submitter service (status %d): %w", statusCode, err)
		}
		return &r, nil
	}

	var errResp submitErrorResponse
	if err := json.Unmarshal(body, &errResp); err == nil && errResp.Message != "" {
		return nil, &SubmitError{
			SubmissionID: errResp.SubmissionID,
			Code:         errResp.ErrorCode,
			StatusCode:   statusCode,
			Message:      errResp.Message,
		}
	}

	bodyStr := string(body)
	if len(bodyStr) > maxErrorBodyLength {
		bodyStr = bodyStr[:maxErrorBodyLength] + "..."
	}
	return nil, fmt.Errorf("submitter service returned unexpected response (HTTP %d): %s", statusCode, bodyStr)
}

// --- TLS transport ---

// buildTransport creates an HTTP transport, optionally with TLS configured.
func buildTransport(tlsCfg *TLSConfig) (http.RoundTripper, error) {
	transport := &http.Transport{
		MaxIdleConns:        maxIdleConns,
		MaxIdleConnsPerHost: maxIdleConnsPerHost,
		IdleConnTimeout:     idleConnTimeout,
	}

	if tlsCfg == nil || !tlsCfg.Enabled {
		return transport, nil
	}

	tlsConfig := &tls.Config{}

	caCertFile := strings.TrimSpace(tlsCfg.CACertFile)
	certFile := strings.TrimSpace(tlsCfg.CertFile)
	keyFile := strings.TrimSpace(tlsCfg.KeyFile)

	if caCertFile != "" {
		caCert, err := os.ReadFile(caCertFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA cert file %q: %w", caCertFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA cert from %q", caCertFile)
		}
		tlsConfig.RootCAs = pool
	}

	if (certFile != "") != (keyFile != "") {
		return nil, fmt.Errorf("both tls.crt and tls.key must be present in the TLS cert directory, got cert=%q key=%q", certFile, keyFile)
	}

	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate %q / %q: %w", certFile, keyFile, err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
		restLogger.Info("TLS enabled with client certificate (mTLS)", "certFile", certFile, "keyFile", keyFile)
	} else {
		restLogger.Info("TLS enabled (server verification only)", "caCertFile", caCertFile)
	}

	transport.TLSClientConfig = tlsConfig
	return transport, nil
}
