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
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveCertificateOptions(t *testing.T) {
	const certDir = "/var/run/webhook-tls"

	tests := []struct {
		name                 string
		provider             string
		providerExplicit     bool
		enableCertManager    bool
		waitTimeout          time.Duration
		retryInterval        time.Duration
		caBundleSync         string
		caBundleSyncInterval time.Duration
		caBundleFile         string
		wantProvider         certificateProvider
		wantOwner            caBundleOwner
		wantCABundleFile     string
		wantErr              string
	}{
		{
			name:          "historical default",
			waitTimeout:   2 * time.Minute,
			retryInterval: time.Second,
			wantProvider:  certificateProviderSelfSigned,
			wantOwner:     caBundleOwnerOperator,
		},
		{
			name:              "legacy cert manager alias",
			enableCertManager: true,
			waitTimeout:       2 * time.Minute,
			retryInterval:     time.Second,
			wantProvider:      certificateProviderCertManager,
			wantOwner:         caBundleOwnerCertManager,
		},
		{
			name:             "explicit self signed",
			provider:         "self-signed",
			providerExplicit: true,
			waitTimeout:      2 * time.Minute,
			retryInterval:    time.Second,
			wantProvider:     certificateProviderSelfSigned,
			wantOwner:        caBundleOwnerOperator,
		},
		{
			name:             "explicit cert manager",
			provider:         "cert-manager",
			providerExplicit: true,
			waitTimeout:      2 * time.Minute,
			retryInterval:    time.Second,
			wantProvider:     certificateProviderCertManager,
			wantOwner:        caBundleOwnerCertManager,
		},
		{
			name:                 "explicit filesystem auto is operator owned and defaults the file",
			provider:             "filesystem",
			providerExplicit:     true,
			waitTimeout:          2 * time.Minute,
			retryInterval:        time.Second,
			caBundleSyncInterval: 10 * time.Second,
			wantProvider:         certificateProviderFilesystem,
			wantOwner:            caBundleOwnerOperator,
			wantCABundleFile:     filepath.Join(certDir, "ca.crt"),
		},
		{
			name:                 "explicit filesystem enabled is operator owned and honors an explicit file",
			provider:             "filesystem",
			providerExplicit:     true,
			waitTimeout:          2 * time.Minute,
			retryInterval:        time.Second,
			caBundleSync:         "enabled",
			caBundleSyncInterval: 10 * time.Second,
			caBundleFile:         "/etc/pki_service/ca/cacerts.pem",
			wantProvider:         certificateProviderFilesystem,
			wantOwner:            caBundleOwnerOperator,
			wantCABundleFile:     "/etc/pki_service/ca/cacerts.pem",
		},
		{
			name:             "filesystem disabled defers to an external owner",
			provider:         "filesystem",
			providerExplicit: true,
			waitTimeout:      2 * time.Minute,
			retryInterval:    time.Second,
			caBundleSync:     "disabled",
			wantProvider:     certificateProviderFilesystem,
			wantOwner:        caBundleOwnerExternal,
		},
		{
			name:              "explicit cert manager accepts legacy alias",
			provider:          "cert-manager",
			providerExplicit:  true,
			enableCertManager: true,
			waitTimeout:       2 * time.Minute,
			retryInterval:     time.Second,
			wantProvider:      certificateProviderCertManager,
			wantOwner:         caBundleOwnerCertManager,
		},
		{
			name:              "legacy alias conflicts with self signed",
			provider:          "self-signed",
			providerExplicit:  true,
			enableCertManager: true,
			waitTimeout:       2 * time.Minute,
			retryInterval:     time.Second,
			wantErr:           "conflicts",
		},
		{
			name:              "legacy alias conflicts with filesystem",
			provider:          "filesystem",
			providerExplicit:  true,
			enableCertManager: true,
			waitTimeout:       2 * time.Minute,
			retryInterval:     time.Second,
			wantErr:           "conflicts",
		},
		{
			name:             "unknown provider",
			provider:         "vault",
			providerExplicit: true,
			waitTimeout:      2 * time.Minute,
			retryInterval:    time.Second,
			wantErr:          "unsupported certificate provider",
		},
		{
			name:             "filesystem rejects zero timeout",
			provider:         "filesystem",
			providerExplicit: true,
			retryInterval:    time.Second,
			wantErr:          "wait timeout must be positive",
		},
		{
			name:             "filesystem rejects zero retry interval",
			provider:         "filesystem",
			providerExplicit: true,
			waitTimeout:      2 * time.Minute,
			wantErr:          "retry interval must be positive",
		},
		{
			name:             "cert manager rejects forced sync",
			provider:         "cert-manager",
			providerExplicit: true,
			waitTimeout:      2 * time.Minute,
			retryInterval:    time.Second,
			caBundleSync:     "enabled",
			wantErr:          "not supported with the cert-manager provider",
		},
		{
			name:             "operator owned filesystem rejects non-positive interval",
			provider:         "filesystem",
			providerExplicit: true,
			waitTimeout:      2 * time.Minute,
			retryInterval:    time.Second,
			caBundleSync:     "enabled",
			wantErr:          "CA bundle sync interval must be positive",
		},
		{
			name:         "unknown sync mode",
			waitTimeout:  2 * time.Minute,
			caBundleSync: "sometimes",
			wantErr:      "unsupported CA bundle sync mode",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveCertificateOptions(certificateOptionsInput{
				provider:             tt.provider,
				providerExplicit:     tt.providerExplicit,
				enableCertManager:    tt.enableCertManager,
				certDir:              certDir,
				certName:             "server.crt",
				keyName:              "server.key",
				waitTimeout:          tt.waitTimeout,
				retryInterval:        tt.retryInterval,
				caBundleSync:         tt.caBundleSync,
				caBundleSyncInterval: tt.caBundleSyncInterval,
				caBundleFile:         tt.caBundleFile,
			})
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantProvider, got.provider)
			assert.Equal(t, filepath.Join(certDir, "server.crt"), got.certPath)
			assert.Equal(t, filepath.Join(certDir, "server.key"), got.keyPath)
			assert.Equal(t, tt.wantOwner, got.caBundleOwner)
			if tt.wantCABundleFile != "" {
				assert.Equal(t, tt.wantCABundleFile, got.caBundleFile)
			}
		})
	}
}
