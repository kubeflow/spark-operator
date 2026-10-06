/*
Copyright 2026 The Kubeflow authors.

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

package webhook

import (
	"context"
	"crypto/tls"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
)

func TestNewStartCommandCertificateFlags(t *testing.T) {
	command := NewStartCommand()

	provider, err := command.Flags().GetString("webhook-cert-provider")
	require.NoError(t, err)
	assert.Equal(t, "self-signed", provider)
	waitTimeout, err := command.Flags().GetDuration("webhook-cert-wait-timeout")
	require.NoError(t, err)
	assert.Equal(t, 2*time.Minute, waitTimeout)
}

func TestRunCertificateStartup(t *testing.T) {
	tests := []struct {
		name       string
		provider   certificateProvider
		wantEvents []string
	}{
		{
			name:       "self signed syncs secret writes files and sets up reconcilers",
			provider:   certificateProviderSelfSigned,
			wantEvents: []string{"sync:self-signed", "write", "reconcile-ca"},
		},
		{
			name:       "cert manager syncs and writes only",
			provider:   certificateProviderCertManager,
			wantEvents: []string{"sync:cert-manager", "write"},
		},
		{
			name:     "filesystem serves from files and does nothing else",
			provider: certificateProviderFilesystem,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var events []string
			actions := certificateStartupActions{
				syncSecret: func(_ context.Context, provider certificateProvider) error {
					events = append(events, "sync:"+string(provider))
					return nil
				},
				writeFiles: func() error {
					events = append(events, "write")
					return nil
				},
				setupCAReconcilers: func() error {
					events = append(events, "reconcile-ca")
					return nil
				},
			}

			err := runCertificateStartup(context.Background(), certificateOptions{provider: tt.provider}, actions)
			require.NoError(t, err)
			assert.Equal(t, tt.wantEvents, events)
		})
	}
}

func TestRunCertificateStartupStopsAfterFailure(t *testing.T) {
	var events []string
	actions := certificateStartupActions{
		syncSecret: func(context.Context, certificateProvider) error {
			events = append(events, "sync")
			return assert.AnError
		},
		writeFiles: func() error {
			events = append(events, "write")
			return nil
		},
		setupCAReconcilers: func() error {
			events = append(events, "reconcile-ca")
			return nil
		},
	}

	err := runCertificateStartup(context.Background(), certificateOptions{provider: certificateProviderSelfSigned}, actions)
	require.ErrorIs(t, err, assert.AnError)
	assert.Equal(t, []string{"sync"}, events)
}

func TestDynamicWebhookServerStartsServingAndReloadTogether(t *testing.T) {
	serverStarted := make(chan struct{})
	dynamicStarted := make(chan struct{})
	wrapped := &dynamicWebhookServer{
		Server: &blockingWebhookServer{started: serverStarted},
		dynamicServing: runnableFunc(func(ctx context.Context) error {
			close(dynamicStarted)
			<-ctx.Done()
			return nil
		}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- wrapped.Start(ctx) }()
	requireChannelClosed(t, serverStarted)
	requireChannelClosed(t, dynamicStarted)
	cancel()
	require.NoError(t, <-done)
	assert.False(t, wrapped.NeedLeaderElection())
}

func TestWebhookTLSOptionsAreIsolatedFromMetrics(t *testing.T) {
	dynamicConfig := &stubDynamicTLSConfig{}
	baseOptions := []func(*tls.Config){func(config *tls.Config) {
		config.MinVersion = tls.VersionTLS13
	}}

	webhookConfig := &tls.Config{}
	for _, option := range dynamicWebhookTLSOptions(baseOptions, dynamicConfig) {
		option(webhookConfig)
	}
	metricsConfig := &tls.Config{}
	for _, option := range baseOptions {
		option(metricsConfig)
	}

	assert.Equal(t, uint16(tls.VersionTLS13), webhookConfig.MinVersion)
	assert.NotNil(t, webhookConfig.GetConfigForClient)
	assert.NotNil(t, webhookConfig.GetCertificate)
	assert.Equal(t, uint16(tls.VersionTLS13), metricsConfig.MinVersion)
	assert.Nil(t, metricsConfig.GetConfigForClient)
	assert.Nil(t, metricsConfig.GetCertificate)
}

type stubDynamicTLSConfig struct{}

func (*stubDynamicTLSConfig) GetConfigForClient(*tls.ClientHelloInfo) (*tls.Config, error) {
	return &tls.Config{}, nil
}

func (*stubDynamicTLSConfig) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return &tls.Certificate{}, nil
}

type runnableFunc func(context.Context) error

func (f runnableFunc) Start(ctx context.Context) error {
	return f(ctx)
}

type blockingWebhookServer struct {
	started chan struct{}
}

func (*blockingWebhookServer) NeedLeaderElection() bool      { return false }
func (*blockingWebhookServer) Register(string, http.Handler) {}
func (s *blockingWebhookServer) Start(ctx context.Context) error {
	close(s.started)
	<-ctx.Done()
	return nil
}
func (*blockingWebhookServer) StartedChecker() healthz.Checker {
	return func(*http.Request) error { return nil }
}
func (*blockingWebhookServer) WebhookMux() *http.ServeMux { return http.NewServeMux() }

func requireChannelClosed(t *testing.T, channel <-chan struct{}) {
	t.Helper()
	select {
	case <-channel:
	case <-time.After(time.Second):
		t.Fatal("component did not start")
	}
}
