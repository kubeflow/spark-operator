package sparkapplication

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

func TestNewRestSparkSubmitter(t *testing.T) {
	t.Run("rejects empty URL", func(t *testing.T) {
		_, err := NewRestSparkSubmitter(RestSparkSubmitterConfig{})
		assert.Error(t, err)
	})

	t.Run("rejects URL without port", func(t *testing.T) {
		_, err := NewRestSparkSubmitter(RestSparkSubmitterConfig{URL: "http://host"})
		assert.Error(t, err)
	})

	t.Run("succeeds with valid URL", func(t *testing.T) {
		s, err := NewRestSparkSubmitter(RestSparkSubmitterConfig{
			URL: "http://host:8080", RetryMaxRetries: 3, RequestTimeout: 10 * time.Second, InitialBackoff: 1 * time.Second,
		})
		assert.NoError(t, err)
		assert.NotNil(t, s)
	})
}

func TestWaitForConnection(t *testing.T) {
	t.Run("succeeds when service is reachable", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusMethodNotAllowed)
		}))
		defer server.Close()

		s := newTestSubmitter(t, server.URL)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		assert.NoError(t, s.WaitForConnection(ctx))
	})

	t.Run("fails when context expires", func(t *testing.T) {
		s := newTestSubmitter(t, "http://127.0.0.1:19999")
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		assert.Error(t, s.WaitForConnection(ctx))
	})
}

func TestSubmitRetry(t *testing.T) {
	setK8sEnv(t)
	t.Run("retries on SUBMITTER_OVERLOADED and succeeds", func(t *testing.T) {
		attempt := 0
		server := retryThenSuccessServer(3, http.StatusServiceUnavailable, ErrorCodeSubmitterOverloaded, &attempt)
		defer server.Close()

		s := newTestSubmitter(t, server.URL)
		assert.NoError(t, s.Submit(context.Background(), newTestApp()))
		assert.Equal(t, 3, attempt)
	})

	t.Run("does not retry on BAD_REQUEST", func(t *testing.T) {
		attempt := 0
		server := retryAlwaysFailServer(http.StatusBadRequest, ErrorCodeBadRequest, &attempt)
		defer server.Close()

		s := newTestSubmitter(t, server.URL)
		assert.Error(t, s.Submit(context.Background(), newTestApp()))
		assert.Equal(t, 1, attempt)
	})

	t.Run("gives up after max retries on INTERNAL_SERVER_ERROR", func(t *testing.T) {
		attempt := 0
		server := retryAlwaysFailServer(http.StatusInternalServerError, ErrorCodeInternalServerError, &attempt)
		defer server.Close()

		s := newTestSubmitter(t, server.URL)
		assert.Error(t, s.Submit(context.Background(), newTestApp()))
		assert.Equal(t, 3, attempt)
	})
}

func TestSubmitIdempotency(t *testing.T) {
	setK8sEnv(t)

	t.Run("duplicate submission returns success with existing pod details", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req submitRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(submitResponse{
				SubmissionID:        req.SubmissionID,
				AppName:             "test-app",
				SparkAppID:          "spark-existing-id",
				DriverPodName:       "test-app-driver",
				DriverPodUID:        "existing-uid",
				Namespace:           "default",
				DuplicateSubmission: true,
			})
		}))
		defer server.Close()

		s := newTestSubmitter(t, server.URL)
		app := newTestApp()
		app.Status.SubmissionID = "test-sub-123"
		assert.NoError(t, s.Submit(context.Background(), app))
	})

	t.Run("DRIVER_POD_ALREADY_EXISTS is a genuine conflict error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(submitErrorResponse{
				SubmissionID: "different-sub-id",
				Status:       409,
				ErrorCode:    ErrorCodeDriverPodAlreadyExists,
				Message:      "Driver pod already exists for a different submission",
			})
		}))
		defer server.Close()

		s := newTestSubmitter(t, server.URL)
		app := newTestApp()
		app.Status.SubmissionID = "test-sub-123"
		assert.Error(t, s.Submit(context.Background(), app))
	})

	t.Run("does not treat other 422 errors as success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnprocessableEntity)
			_ = json.NewEncoder(w).Encode(submitErrorResponse{
				SubmissionID: "test-sub-123",
				Status:       422,
				ErrorCode:    ErrorCodeInvalidPodTemplate,
				Message:      "invalid template",
			})
		}))
		defer server.Close()

		s := newTestSubmitter(t, server.URL)
		app := newTestApp()
		app.Status.SubmissionID = "test-sub-123"
		assert.Error(t, s.Submit(context.Background(), app))
	})
}

func TestSubmitPodTemplates(t *testing.T) {
	setK8sEnv(t)
	t.Run("sends templates inline when present", func(t *testing.T) {
		var capturedReq submitRequest
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewDecoder(r.Body).Decode(&capturedReq)
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(submitResponse{DriverPodName: "drv", SparkAppID: "id"})
		}))
		defer server.Close()

		app := newTestApp()
		app.Spec.Driver.Template = &corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "driver"}}}}
		app.Spec.Executor.Template = &corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "executor"}}}}

		s := newTestSubmitter(t, server.URL)
		require.NoError(t, s.Submit(context.Background(), app))
		assert.NotNil(t, capturedReq.DriverPodTemplate)
		assert.NotNil(t, capturedReq.ExecutorPodTemplate)
	})

	t.Run("omits templates when not specified", func(t *testing.T) {
		var capturedReq submitRequest
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewDecoder(r.Body).Decode(&capturedReq)
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(submitResponse{DriverPodName: "drv", SparkAppID: "id"})
		}))
		defer server.Close()

		s := newTestSubmitter(t, server.URL)
		require.NoError(t, s.Submit(context.Background(), newTestApp()))
		assert.Nil(t, capturedReq.DriverPodTemplate)
		assert.Nil(t, capturedReq.ExecutorPodTemplate)
	})
}

func TestSubmitContextCancellation(t *testing.T) {
	setK8sEnv(t)

	t.Run("returns error when context cancelled during retry backoff", func(t *testing.T) {
		attempt := 0
		server := retryAlwaysFailServer(http.StatusInternalServerError, ErrorCodeInternalServerError, &attempt)
		defer server.Close()

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		s, _ := NewRestSparkSubmitter(RestSparkSubmitterConfig{
			URL:             server.URL,
			RetryMaxRetries: 3,
			RequestTimeout:  5 * time.Second,
			InitialBackoff:  2 * time.Second,
		})
		err := s.Submit(ctx, newTestApp())
		assert.Error(t, err)
		assert.Equal(t, 1, attempt)
	})

	t.Run("returns error on network failure", func(t *testing.T) {
		s := newTestSubmitter(t, "http://192.0.2.1:9999")
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		assert.Error(t, s.Submit(ctx, newTestApp()))
	})
}

func TestParseSubmitResponse(t *testing.T) {
	t.Run("parses success response", func(t *testing.T) {
		body, _ := json.Marshal(submitResponse{
			SubmissionID: "sub-123", AppName: "app", SparkAppID: "id",
			DriverPodName: "app-driver", DriverPodUID: "uid", Namespace: "ns",
		})
		resp := &http.Response{StatusCode: http.StatusCreated, Body: io.NopCloser(bytes.NewReader(body))}
		result, err := parseSubmitResponse(resp)
		assert.NoError(t, err)
		assert.Equal(t, "sub-123", result.SubmissionID)
		assert.Equal(t, "app-driver", result.DriverPodName)
		assert.False(t, result.DuplicateSubmission)
	})

	t.Run("parses duplicate submission response (200)", func(t *testing.T) {
		body, _ := json.Marshal(submitResponse{
			SubmissionID: "sub-123", AppName: "app", SparkAppID: "spark-existing",
			DriverPodName: "app-driver", DriverPodUID: "uid", Namespace: "ns",
			DuplicateSubmission: true,
		})
		resp := &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(body))}
		result, err := parseSubmitResponse(resp)
		assert.NoError(t, err)
		assert.Equal(t, "sub-123", result.SubmissionID)
		assert.Equal(t, "spark-existing", result.SparkAppID)
		assert.True(t, result.DuplicateSubmission)
	})

	t.Run("structured error", func(t *testing.T) {
		body, _ := json.Marshal(submitErrorResponse{
			SubmissionID: "sub-123",
			Status:       409,
			ErrorCode:    ErrorCodeDriverPodAlreadyExists,
			Message:      "Failed to submit: pod already exists",
			Timestamp:    "2026-06-17T23:30:00.123Z",
		})
		resp := &http.Response{StatusCode: http.StatusConflict, Body: io.NopCloser(bytes.NewReader(body))}
		result, err := parseSubmitResponse(resp)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "Failed to submit")
		assert.Contains(t, err.Error(), "DRIVER_POD_ALREADY_EXISTS")
		var submitErr *SubmitError
		assert.True(t, errors.As(err, &submitErr))
		assert.Equal(t, ErrorCodeDriverPodAlreadyExists, submitErr.Code)
		assert.Equal(t, "sub-123", submitErr.SubmissionID)
	})

	t.Run("structured error with different code", func(t *testing.T) {
		body, _ := json.Marshal(submitErrorResponse{
			SubmissionID: "sub-456",
			Status:       400,
			ErrorCode:    ErrorCodeBadRequest,
			Message:      "Invalid job configuration",
			Timestamp:    "2026-06-17T23:30:00.123Z",
		})
		resp := &http.Response{StatusCode: 400, Body: io.NopCloser(bytes.NewReader(body))}
		result, err := parseSubmitResponse(resp)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "Invalid job configuration")
		assert.Contains(t, err.Error(), "BAD_REQUEST")
	})

	t.Run("non-JSON response body", func(t *testing.T) {
		resp := &http.Response{StatusCode: 502, Body: io.NopCloser(bytes.NewReader([]byte("Bad Gateway")))}
		result, err := parseSubmitResponse(resp)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "unexpected response")
		assert.Contains(t, err.Error(), "Bad Gateway")
	})

	t.Run("truncates long response body", func(t *testing.T) {
		longBody := make([]byte, maxErrorBodyLength+100)
		for i := range longBody {
			longBody[i] = 'x'
		}
		resp := &http.Response{StatusCode: 500, Body: io.NopCloser(bytes.NewReader(longBody))}
		_, err := parseSubmitResponse(resp)
		assert.Contains(t, err.Error(), "...")
		assert.LessOrEqual(t, len(err.Error()), maxErrorBodyLength+100)
	})
}

func TestCanRetry(t *testing.T) {
	s := &RestSparkSubmitter{maxRetries: 3}

	networkErr := fmt.Errorf("connection refused")
	overloaded := &SubmitError{Code: ErrorCodeSubmitterOverloaded, StatusCode: 503, Message: "overloaded"}
	internalErr := &SubmitError{Code: ErrorCodeInternalServerError, StatusCode: 500, Message: "internal"}
	badRequest := &SubmitError{Code: ErrorCodeBadRequest, StatusCode: 400, Message: "bad request"}
	podExists := &SubmitError{Code: ErrorCodeDriverPodAlreadyExists, StatusCode: 409, Message: "exists"}
	invalidTemplate := &SubmitError{Code: ErrorCodeInvalidPodTemplate, StatusCode: 422, Message: "invalid"}

	assert.True(t, s.canRetry(0, networkErr), "network error should retry")
	assert.True(t, s.canRetry(0, overloaded), "SUBMITTER_OVERLOADED should retry")
	assert.True(t, s.canRetry(0, internalErr), "INTERNAL_SERVER_ERROR should retry")
	assert.False(t, s.canRetry(0, badRequest), "BAD_REQUEST should not retry")
	assert.False(t, s.canRetry(0, podExists), "DRIVER_POD_ALREADY_EXISTS should not retry")
	assert.False(t, s.canRetry(0, invalidTemplate), "INVALID_POD_TEMPLATE should not retry")
	assert.False(t, s.canRetry(2, overloaded), "last attempt should not retry")
}

// --- Test helpers ---

func newTestSubmitter(t *testing.T, url string) *RestSparkSubmitter {
	t.Helper()
	s, err := NewRestSparkSubmitter(RestSparkSubmitterConfig{
		URL:             url,
		RetryMaxRetries: 3,
		RequestTimeout:  5 * time.Second,
		InitialBackoff:  10 * time.Millisecond,
	})
	require.NoError(t, err)
	return s
}

func setK8sEnv(t *testing.T) {
	t.Helper()
	t.Setenv("KUBERNETES_SERVICE_HOST", "127.0.0.1")
	t.Setenv("KUBERNETES_SERVICE_PORT", "6443")
}

func newTestApp() *v1beta2.SparkApplication {
	return &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{Name: "test-app", Namespace: "default"},
		Spec:       v1beta2.SparkApplicationSpec{},
	}
}

func retryThenSuccessServer(succeedOnAttempt int, failStatus int, errorCode SubmitErrorCode, attempt *int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*attempt++
		if *attempt < succeedOnAttempt {
			w.WriteHeader(failStatus)
			_ = json.NewEncoder(w).Encode(submitErrorResponse{ErrorCode: errorCode, Message: "transient"})
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(submitResponse{DriverPodName: "drv", SparkAppID: "id"})
	}))
}

func retryAlwaysFailServer(failStatus int, errorCode SubmitErrorCode, attempt *int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*attempt++
		w.WriteHeader(failStatus)
		_ = json.NewEncoder(w).Encode(submitErrorResponse{ErrorCode: errorCode, Message: "error"})
	}))
}

// --- TLS tests ---

func TestBuildTransport_NilConfig(t *testing.T) {
	transport, err := buildTransport(nil)
	assert.NoError(t, err)
	assert.NotNil(t, transport)
}

func TestBuildTransport_Disabled(t *testing.T) {
	transport, err := buildTransport(&TLSConfig{Enabled: false})
	assert.NoError(t, err)
	assert.NotNil(t, transport)
}

func TestBuildTransport_InvalidCACert(t *testing.T) {
	badCA := writeTempFile(t, "bad-ca.pem", "not a cert")
	_, err := buildTransport(&TLSConfig{
		Enabled:    true,
		CACertFile: badCA,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse CA cert")
}

func TestBuildTransport_MissingCACertFile(t *testing.T) {
	_, err := buildTransport(&TLSConfig{
		Enabled:    true,
		CACertFile: "/nonexistent/ca.crt",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read CA cert file")
}

func TestBuildTransport_PartialCertKeyErrors(t *testing.T) {
	certFile := writeTempFile(t, "tls.crt", "cert")

	t.Run("cert without key", func(t *testing.T) {
		_, err := buildTransport(&TLSConfig{
			Enabled:  true,
			CertFile: certFile,
			KeyFile:  "",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "both tls.crt and tls.key must be present in the TLS cert directory")
	})

	t.Run("key without cert", func(t *testing.T) {
		_, err := buildTransport(&TLSConfig{
			Enabled:  true,
			CertFile: "",
			KeyFile:  "/some/key.pem",
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "both tls.crt and tls.key must be present in the TLS cert directory")
	})
}

func TestBuildTransport_InvalidCertKeyPair(t *testing.T) {
	certFile := writeTempFile(t, "tls.crt", "not a cert")
	keyFile := writeTempFile(t, "tls.key", "not a key")

	_, err := buildTransport(&TLSConfig{
		Enabled:  true,
		CertFile: certFile,
		KeyFile:  keyFile,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load client certificate")
}

func TestBuildTransport_ValidTLS(t *testing.T) {
	certFile, keyFile, caFile := generateTestCertFiles(t)

	transport, err := buildTransport(&TLSConfig{
		Enabled:    true,
		CertFile:   certFile,
		KeyFile:    keyFile,
		CACertFile: caFile,
	})
	assert.NoError(t, err)
	assert.NotNil(t, transport)
}

func TestBuildTransport_WhitespacePathsIgnored(t *testing.T) {
	transport, err := buildTransport(&TLSConfig{
		Enabled:    true,
		CertFile:   "  ",
		KeyFile:    "  ",
		CACertFile: "  ",
	})
	assert.NoError(t, err)
	assert.NotNil(t, transport)
}

// --- TLS test helpers ---

func writeTempFile(t *testing.T, name, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/" + name
	require.NoError(t, os.WriteFile(path, []byte(content), 0600))
	return path
}

func generateTestCertFiles(t *testing.T) (certFile, keyFile, caFile string) {
	t.Helper()
	dir := t.TempDir()
	certFile = dir + "/tls.crt"
	keyFile = dir + "/tls.key"
	caFile = dir + "/ca.crt"

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(1 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	require.NoError(t, os.WriteFile(caFile, caPEM, 0600))

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test-leaf"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(1 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caTemplate, &leafKey.PublicKey, caKey)
	require.NoError(t, err)

	leafPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	require.NoError(t, os.WriteFile(certFile, leafPEM, 0600))

	keyBytes, err := x509.MarshalECPrivateKey(leafKey)
	require.NoError(t, err)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0600))

	return certFile, keyFile, caFile
}
