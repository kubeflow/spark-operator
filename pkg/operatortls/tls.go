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

package operatortls

import (
	cryptotls "crypto/tls"
	"fmt"
	"strings"
)

// SetupTLS parses the TLS flags and returns TLS option functions for webhook and metrics servers.
func SetupTLS(minVersion string, cipherSuites []string, enableHTTP2 bool) ([]func(*cryptotls.Config), error) {
	var tlsOpts []func(*cryptotls.Config)

	ver, err := ParseTLSVersion(minVersion)
	if err != nil {
		return nil, fmt.Errorf("invalid --tls-min-version %q: %w", minVersion, err)
	}
	tlsOpts = append(tlsOpts, func(c *cryptotls.Config) {
		c.MinVersion = ver
	})

	if len(cipherSuites) > 0 {
		cipherIDs, err := ParseCipherSuites(cipherSuites)
		if err != nil {
			return nil, fmt.Errorf("invalid --tls-cipher-suites: %w", err)
		}
		tlsOpts = append(tlsOpts, func(c *cryptotls.Config) {
			c.CipherSuites = cipherIDs
		})
	}

	if enableHTTP2 {
		tlsOpts = append(tlsOpts, func(c *cryptotls.Config) {
			c.NextProtos = []string{"h2", "http/1.1"}
		})
	} else {
		tlsOpts = append(tlsOpts, func(c *cryptotls.Config) {
			c.NextProtos = []string{"h2", "http/1.1"}
		})
	}

	return tlsOpts, nil
}

// ParseTLSVersion converts a version string to a tls.Version constant.
func ParseTLSVersion(version string) (uint16, error) {
	switch version {
	case "VersionTLS12":
		return cryptotls.VersionTLS12, nil
	case "VersionTLS13":
		return cryptotls.VersionTLS13, nil
	default:
		return 0, fmt.Errorf("unsupported TLS version: %s (use VersionTLS12 or VersionTLS13)", version)
	}
}

// ParseCipherSuites converts cipher suite names to their IDs.
// Empty strings are silently filtered out.
func ParseCipherSuites(names []string) ([]uint16, error) {
	allCiphers := make(map[string]uint16)
	for _, cs := range cryptotls.CipherSuites() {
		allCiphers[cs.Name] = cs.ID
	}
	var ids []uint16
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		id, ok := allCiphers[name]
		if !ok {
			return nil, fmt.Errorf("unknown cipher suite: %s", name)
		}
		ids = append(ids, id)
	}
	return ids, nil
}
