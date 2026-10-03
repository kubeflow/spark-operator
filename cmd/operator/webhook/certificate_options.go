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
	"fmt"
	"path/filepath"
	"time"
)

type certificateProvider string

const (
	certificateProviderSelfSigned  certificateProvider = "self-signed"
	certificateProviderCertManager certificateProvider = "cert-manager"
	certificateProviderFilesystem  certificateProvider = "filesystem"
)

type certificateOptionsInput struct {
	provider          string
	providerExplicit  bool
	enableCertManager bool
	certDir           string
	certName          string
	keyName           string
	waitTimeout       time.Duration
	retryInterval     time.Duration
}

type certificateOptions struct {
	provider      certificateProvider
	certPath      string
	keyPath       string
	waitTimeout   time.Duration
	retryInterval time.Duration
}

func resolveCertificateOptions(input certificateOptionsInput) (certificateOptions, error) {
	provider := certificateProviderSelfSigned
	if input.providerExplicit {
		provider = certificateProvider(input.provider)
	} else if input.enableCertManager {
		provider = certificateProviderCertManager
	}

	switch provider {
	case certificateProviderSelfSigned, certificateProviderCertManager, certificateProviderFilesystem:
	default:
		return certificateOptions{}, fmt.Errorf("unsupported certificate provider %q", input.provider)
	}

	if input.providerExplicit && input.enableCertManager && provider != certificateProviderCertManager {
		return certificateOptions{}, fmt.Errorf("certificate provider %q conflicts with --enable-cert-manager", provider)
	}
	if provider == certificateProviderFilesystem {
		if input.waitTimeout <= 0 {
			return certificateOptions{}, fmt.Errorf("filesystem certificate wait timeout must be positive")
		}
		if input.retryInterval <= 0 {
			return certificateOptions{}, fmt.Errorf("filesystem certificate retry interval must be positive")
		}
	}

	return certificateOptions{
		provider:      provider,
		certPath:      filepath.Join(input.certDir, input.certName),
		keyPath:       filepath.Join(input.certDir, input.keyName),
		waitTimeout:   input.waitTimeout,
		retryInterval: input.retryInterval,
	}, nil
}
