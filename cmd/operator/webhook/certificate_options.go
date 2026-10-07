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

// caBundleSyncMode selects who reconciles the admission webhook caBundle.
type caBundleSyncMode string

const (
	// caBundleSyncModeAuto lets the provider decide: filesystem => operator,
	// cert-manager => external (cert-manager's ca-injector), self-signed => operator.
	caBundleSyncModeAuto caBundleSyncMode = "auto"
	// caBundleSyncModeEnabled forces the operator to own the caBundle.
	caBundleSyncModeEnabled caBundleSyncMode = "enabled"
	// caBundleSyncModeDisabled defers the caBundle to an external owner.
	caBundleSyncModeDisabled caBundleSyncMode = "disabled"
)

// caBundleOwner is the resolved owner downstream wiring branches on.
type caBundleOwner string

const (
	caBundleOwnerOperator    caBundleOwner = "operator"
	caBundleOwnerExternal    caBundleOwner = "external"
	caBundleOwnerCertManager caBundleOwner = "cert-manager"
)

type certificateOptionsInput struct {
	provider             string
	providerExplicit     bool
	enableCertManager    bool
	certDir              string
	certName             string
	keyName              string
	waitTimeout          time.Duration
	retryInterval        time.Duration
	caBundleFile         string
	caBundleSync         string
	caBundleSyncInterval time.Duration
}

type certificateOptions struct {
	provider             certificateProvider
	certPath             string
	keyPath              string
	waitTimeout          time.Duration
	retryInterval        time.Duration
	caBundleOwner        caBundleOwner
	caBundleFile         string
	caBundleSyncInterval time.Duration
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

	syncMode := caBundleSyncMode(input.caBundleSync)
	if input.caBundleSync == "" {
		syncMode = caBundleSyncModeAuto
	}
	switch syncMode {
	case caBundleSyncModeAuto, caBundleSyncModeEnabled, caBundleSyncModeDisabled:
	default:
		return certificateOptions{}, fmt.Errorf("unsupported CA bundle sync mode %q", input.caBundleSync)
	}

	owner, err := resolveCABundleOwner(provider, syncMode)
	if err != nil {
		return certificateOptions{}, err
	}

	caBundleFile := input.caBundleFile
	caBundleSyncInterval := input.caBundleSyncInterval
	if provider == certificateProviderFilesystem && owner == caBundleOwnerOperator {
		if caBundleSyncInterval <= 0 {
			return certificateOptions{}, fmt.Errorf("CA bundle sync interval must be positive")
		}
		if caBundleFile == "" {
			caBundleFile = filepath.Join(input.certDir, "ca.crt")
		}
	}

	return certificateOptions{
		provider:             provider,
		certPath:             filepath.Join(input.certDir, input.certName),
		keyPath:              filepath.Join(input.certDir, input.keyName),
		waitTimeout:          input.waitTimeout,
		retryInterval:        input.retryInterval,
		caBundleOwner:        owner,
		caBundleFile:         caBundleFile,
		caBundleSyncInterval: caBundleSyncInterval,
	}, nil
}

// resolveCABundleOwner implements the KEP-3165 ownership matrix.
func resolveCABundleOwner(provider certificateProvider, syncMode caBundleSyncMode) (caBundleOwner, error) {
	switch provider {
	case certificateProviderSelfSigned:
		// The operator mints the self-signed CA, so it always publishes it.
		return caBundleOwnerOperator, nil
	case certificateProviderCertManager:
		if syncMode == caBundleSyncModeEnabled {
			return "", fmt.Errorf("cert-manager owns the caBundle via its ca-injector; --webhook-ca-bundle-sync=enabled is not supported with the cert-manager provider")
		}
		// cert-manager's ca-injector owns the caBundle: external to this
		// operator, but distinguished from an unmanaged external owner.
		return caBundleOwnerCertManager, nil
	case certificateProviderFilesystem:
		switch syncMode {
		case caBundleSyncModeAuto, caBundleSyncModeEnabled:
			return caBundleOwnerOperator, nil
		default: // disabled
			return caBundleOwnerExternal, nil
		}
	default:
		return "", fmt.Errorf("unsupported certificate provider %q", provider)
	}
}
