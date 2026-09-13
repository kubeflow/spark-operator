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

package tls_test

import (
	cryptotls "crypto/tls"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubeflow/spark-operator/v2/pkg/tls"
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
		{name: "unsupported version", version: "VersionTLS11", wantErr: true},
		{name: "empty string", version: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tls.ParseTLSVersion(tt.version)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseCipherSuites(t *testing.T) {
	t.Run("nil input returns nil", func(t *testing.T) {
		ids, err := tls.ParseCipherSuites(nil)
		require.NoError(t, err)
		assert.Nil(t, ids)
	})

	t.Run("empty and whitespace entries are filtered out", func(t *testing.T) {
		ids, err := tls.ParseCipherSuites([]string{"", "   "})
		require.NoError(t, err)
		assert.Nil(t, ids)
	})

	t.Run("valid cipher suite name resolves to its id", func(t *testing.T) {
		suites := cryptotls.CipherSuites()
		require.NotEmpty(t, suites)
		name := suites[0].Name

		ids, err := tls.ParseCipherSuites([]string{" " + name + " "})
		require.NoError(t, err)
		require.Len(t, ids, 1)
		assert.Equal(t, suites[0].ID, ids[0])
	})

	t.Run("unknown cipher suite name errors", func(t *testing.T) {
		_, err := tls.ParseCipherSuites([]string{"NOT_A_REAL_CIPHER_SUITE"})
		require.Error(t, err)
	})
}

func TestSetupTLS(t *testing.T) {
	t.Run("invalid min version is rejected", func(t *testing.T) {
		_, err := tls.SetupTLS("bogus", nil)
		require.Error(t, err)
	})

	t.Run("invalid cipher suite is rejected", func(t *testing.T) {
		_, err := tls.SetupTLS("VersionTLS12", []string{"NOT_A_REAL_CIPHER_SUITE"})
		require.Error(t, err)
	})

	t.Run("valid input produces options that configure a tls.Config", func(t *testing.T) {
		suites := cryptotls.CipherSuites()
		require.NotEmpty(t, suites)

		opts, err := tls.SetupTLS("VersionTLS13", []string{suites[0].Name})
		require.NoError(t, err)
		require.NotEmpty(t, opts)

		cfg := &cryptotls.Config{}
		for _, opt := range opts {
			opt(cfg)
		}

		assert.Equal(t, uint16(cryptotls.VersionTLS13), cfg.MinVersion)
		assert.Equal(t, []uint16{suites[0].ID}, cfg.CipherSuites)
		assert.Equal(t, []string{"h2", "http/1.1"}, cfg.NextProtos)
	})

	t.Run("no cipher suites leaves CipherSuites unset", func(t *testing.T) {
		opts, err := tls.SetupTLS("VersionTLS12", nil)
		require.NoError(t, err)

		cfg := &cryptotls.Config{}
		for _, opt := range opts {
			opt(cfg)
		}

		assert.Nil(t, cfg.CipherSuites)
	})
}
