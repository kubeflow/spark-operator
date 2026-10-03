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
	"errors"

	"github.com/kubeflow/spark-operator/v2/pkg/certificate"

	ctrlwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"
)

type dynamicTLSConfig interface {
	GetConfigForClient(*tls.ClientHelloInfo) (*tls.Config, error)
	GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error)
}

type certificateStartupActions struct {
	syncSecret         func(context.Context, certificateProvider) error
	writeFiles         func() error
	setupCAReconcilers func() error
}

func runCertificateStartup(
	ctx context.Context,
	options certificateOptions,
	actions certificateStartupActions,
) error {
	// The filesystem provider gets its serving cert/key from mounted files
	// (dynamicServing), so it never syncs a secret or writes files. Publishing
	// the caBundle for this provider is a later iteration; here it only serves.
	if options.provider == certificateProviderFilesystem {
		return nil
	}

	if err := actions.syncSecret(ctx, options.provider); err != nil {
		return err
	}
	if err := actions.writeFiles(); err != nil {
		return err
	}
	// The self-signed provider mints and publishes its own CA, so it runs the
	// reconcilers; cert-manager's ca-injector owns the caBundle, so it does not.
	if options.provider == certificateProviderSelfSigned {
		return actions.setupCAReconcilers()
	}
	return nil
}

type dynamicWebhookServer struct {
	ctrlwebhook.Server
	dynamicServing interface {
		Start(context.Context) error
	}
}

func newWebhookServer(options ctrlwebhook.Options, dynamicServing *certificate.DynamicTLSConfig) ctrlwebhook.Server {
	server := ctrlwebhook.NewServer(options)
	if dynamicServing == nil {
		return server
	}
	return &dynamicWebhookServer{
		Server:         server,
		dynamicServing: dynamicServing,
	}
}

func (s *dynamicWebhookServer) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	serverDone := make(chan error, 1)
	dynamicDone := make(chan error, 1)
	go func() { serverDone <- s.Server.Start(runCtx) }()
	go func() { dynamicDone <- s.dynamicServing.Start(runCtx) }()

	select {
	case serverErr := <-serverDone:
		cancel()
		return errors.Join(serverErr, <-dynamicDone)
	case dynamicErr := <-dynamicDone:
		cancel()
		return errors.Join(dynamicErr, <-serverDone)
	}
}

func dynamicWebhookTLSOptions(base []func(*tls.Config), dynamic dynamicTLSConfig) []func(*tls.Config) {
	options := append([]func(*tls.Config){}, base...)
	return append(options, func(config *tls.Config) {
		config.GetConfigForClient = dynamic.GetConfigForClient
		config.GetCertificate = dynamic.GetCertificate
	})
}
