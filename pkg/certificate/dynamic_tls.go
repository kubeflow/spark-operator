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

package certificate

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	"k8s.io/apiserver/pkg/server/dynamiccertificates"
)

type DynamicTLSConfig struct {
	servingContent *dynamiccertificates.DynamicCertKeyPairContent
	controller     *dynamiccertificates.DynamicServingCertificateController
}

// NewDynamicTLSConfig creates an initialized dynamic serving-certificate lifecycle component.
func NewDynamicTLSConfig(
	ctx context.Context,
	certPath string,
	keyPath string,
	waitTimeout time.Duration,
	retryInterval time.Duration,
	baseTLSConfig *tls.Config,
) (*DynamicTLSConfig, error) {
	servingContent, err := acquireDynamicServingContent(ctx, certPath, keyPath, waitTimeout, retryInterval)
	if err != nil {
		return nil, err
	}

	controller := dynamiccertificates.NewDynamicServingCertificateController(
		baseTLSConfig,
		nil,
		servingContent,
		nil,
		nil,
	)
	servingContent.AddListener(controller)
	if err := controller.RunOnce(); err != nil {
		return nil, fmt.Errorf("failed to initialize dynamic serving certificate: %w", err)
	}

	return &DynamicTLSConfig{
		servingContent: servingContent,
		controller:     controller,
	}, nil
}

// GetConfigForClient returns the latest atomically published TLS configuration.
func (c *DynamicTLSConfig) GetConfigForClient(clientHello *tls.ClientHelloInfo) (*tls.Config, error) {
	if clientHello == nil {
		clientHello = &tls.ClientHelloInfo{ServerName: "dynamic-serving-cert"}
	}
	return c.controller.GetConfigForClient(clientHello)
}

// GetCertificate returns the default certificate from the latest TLS configuration.
func (c *DynamicTLSConfig) GetCertificate(clientHello *tls.ClientHelloInfo) (*tls.Certificate, error) {
	config, err := c.GetConfigForClient(clientHello)
	if err != nil {
		return nil, err
	}
	if len(config.Certificates) == 0 {
		return nil, fmt.Errorf("dynamic serving certificate is not ready")
	}
	return &config.Certificates[0], nil
}

// ServedLeaf returns the currently-served webhook leaf certificate, parsed from
// the active TLS configuration. The filesystem CA source uses it for the
// CA<->leaf interlock so it never commits a bundle that would reject our own
// serving certificate. The dynamic tls.Certificate usually leaves Leaf nil, so
// this parses Certificate[0] when needed.
func (c *DynamicTLSConfig) ServedLeaf() (*x509.Certificate, error) {
	cert, err := c.GetCertificate(nil)
	if err != nil {
		return nil, err
	}
	if len(cert.Certificate) == 0 {
		return nil, fmt.Errorf("served certificate has no chain")
	}
	if cert.Leaf != nil {
		return cert.Leaf, nil
	}
	return x509.ParseCertificate(cert.Certificate[0])
}

func (c *DynamicTLSConfig) reload(ctx context.Context) error {
	if err := c.servingContent.RunOnce(ctx); err != nil {
		return fmt.Errorf("failed to reload dynamic serving certificate: %w", err)
	}
	if err := c.controller.RunOnce(); err != nil {
		return fmt.Errorf("failed to publish dynamic serving certificate: %w", err)
	}
	return nil
}

// Start runs the file source and TLS publication controller until cancellation.
func (c *DynamicTLSConfig) Start(ctx context.Context) error {
	contentDone := make(chan struct{}, 1)
	controllerDone := make(chan struct{}, 1)

	go func() {
		defer close(contentDone)
		c.servingContent.Run(ctx, 1)
	}()
	go func() {
		defer close(controllerDone)
		c.controller.Run(1, ctx.Done())
	}()

	<-contentDone
	<-controllerDone
	return nil
}

// NeedLeaderElection indicates that every webhook replica independently reloads its mounted files.
func (*DynamicTLSConfig) NeedLeaderElection() bool {
	return false
}
