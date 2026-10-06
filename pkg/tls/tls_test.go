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

package tls

import (
	cryptotls "crypto/tls"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ecdheRSAAES128 = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
	ecdheRSAAES256 = "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384"
)

func TestParseTLSVersion(t *testing.T) {
	tests := []struct {
		name    string
		version string
		want    uint16
		wantErr bool
	}{
		{name: "TLS 1.2", version: "VersionTLS12", want: cryptotls.VersionTLS12},
		{name: "TLS 1.3", version: "VersionTLS13", want: cryptotls.VersionTLS13},
		{name: "empty", version: "", wantErr: true},
		{name: "unsupported older version", version: "VersionTLS11", wantErr: true},
		{name: "wrong format", version: "TLS1.2", wantErr: true},
		{name: "wrong case", version: "versiontls12", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseTLSVersion(tt.version)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "unsupported TLS version")
				assert.Zero(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseCipherSuites(t *testing.T) {
	tests := []struct {
		name    string
		names   []string
		want    []uint16
		wantErr string
	}{
		{name: "nil input", names: nil, want: nil},
		{name: "only empty names", names: []string{"", "  "}, want: nil},
		{
			name:  "single suite",
			names: []string{ecdheRSAAES128},
			want:  []uint16{cryptotls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256},
		},
		{
			name:  "multiple suites keep their order",
			names: []string{ecdheRSAAES256, ecdheRSAAES128},
			want: []uint16{
				cryptotls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				cryptotls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			},
		},
		{
			name:  "surrounding whitespace is trimmed and empty names skipped",
			names: []string{" " + ecdheRSAAES128 + " ", ""},
			want:  []uint16{cryptotls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256},
		},
		{
			name:    "unknown suite",
			names:   []string{ecdheRSAAES128, "TLS_DOES_NOT_EXIST"},
			wantErr: "unknown cipher suite: TLS_DOES_NOT_EXIST",
		},
		{
			name:    "insecure suite is rejected",
			names:   []string{"TLS_RSA_WITH_RC4_128_SHA"},
			wantErr: "unknown cipher suite: TLS_RSA_WITH_RC4_128_SHA",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseCipherSuites(tt.names)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSetupTLS(t *testing.T) {
	tests := []struct {
		name             string
		flagPrefix       string
		minVersion       string
		cipherSuites     []string
		wantMinVersion   uint16
		wantCipherSuites []uint16
		wantErr          string
	}{
		{
			name:           "default cipher suites",
			flagPrefix:     "webhook",
			minVersion:     "VersionTLS12",
			wantMinVersion: cryptotls.VersionTLS12,
		},
		{
			name:             "custom cipher suites",
			flagPrefix:       "metric",
			minVersion:       "VersionTLS12",
			cipherSuites:     []string{ecdheRSAAES128},
			wantMinVersion:   cryptotls.VersionTLS12,
			wantCipherSuites: []uint16{cryptotls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256},
		},
		{
			name:           "TLS 1.3",
			flagPrefix:     "webhook",
			minVersion:     "VersionTLS13",
			wantMinVersion: cryptotls.VersionTLS13,
		},
		{
			name:       "invalid webhook min version names the webhook flag",
			flagPrefix: "webhook",
			minVersion: "TLS1.2",
			wantErr:    `invalid --webhook-tls-min-version "TLS1.2"`,
		},
		{
			name:       "invalid metric min version names the metric flag",
			flagPrefix: "metric",
			minVersion: "",
			wantErr:    `invalid --metric-tls-min-version ""`,
		},
		{
			name:         "invalid webhook cipher suite names the webhook flag",
			flagPrefix:   "webhook",
			minVersion:   "VersionTLS12",
			cipherSuites: []string{"TLS_DOES_NOT_EXIST"},
			wantErr:      "invalid --webhook-tls-cipher-suites: unknown cipher suite: TLS_DOES_NOT_EXIST",
		},
		{
			name:         "invalid metric cipher suite names the metric flag",
			flagPrefix:   "metric",
			minVersion:   "VersionTLS12",
			cipherSuites: []string{"TLS_DOES_NOT_EXIST"},
			wantErr:      "invalid --metric-tls-cipher-suites: unknown cipher suite: TLS_DOES_NOT_EXIST",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := SetupTLS(tt.flagPrefix, tt.minVersion, tt.cipherSuites)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Nil(t, opts)
				return
			}
			require.NoError(t, err)

			cfg := &cryptotls.Config{}
			for _, opt := range opts {
				opt(cfg)
			}
			assert.Equal(t, tt.wantMinVersion, cfg.MinVersion)
			assert.Equal(t, tt.wantCipherSuites, cfg.CipherSuites)
			assert.Equal(t, []string{"h2", "http/1.1"}, cfg.NextProtos)
		})
	}
}
