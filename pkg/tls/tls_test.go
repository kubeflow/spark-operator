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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/kubeflow/spark-operator/v2/pkg/tls"
)

var _ = Describe("ParseTLSVersion", func() {
	It("parses TLS 1.2", func() {
		got, err := tls.ParseTLSVersion("VersionTLS12")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(uint16(cryptotls.VersionTLS12)))
	})

	It("parses TLS 1.3", func() {
		got, err := tls.ParseTLSVersion("VersionTLS13")
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(uint16(cryptotls.VersionTLS13)))
	})

	It("rejects an unsupported version", func() {
		_, err := tls.ParseTLSVersion("VersionTLS11")
		Expect(err).To(HaveOccurred())
	})

	It("rejects an empty string", func() {
		_, err := tls.ParseTLSVersion("")
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("ParseCipherSuites", func() {
	It("returns nil for nil input", func() {
		ids, err := tls.ParseCipherSuites(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(ids).To(BeNil())
	})

	It("filters out empty and whitespace entries", func() {
		ids, err := tls.ParseCipherSuites([]string{"", "   "})
		Expect(err).NotTo(HaveOccurred())
		Expect(ids).To(BeNil())
	})

	It("resolves a valid cipher suite name to its id", func() {
		suites := cryptotls.CipherSuites()
		Expect(suites).NotTo(BeEmpty())
		name := suites[0].Name

		ids, err := tls.ParseCipherSuites([]string{" " + name + " "})
		Expect(err).NotTo(HaveOccurred())
		Expect(ids).To(Equal([]uint16{suites[0].ID}))
	})

	It("errors on an unknown cipher suite name", func() {
		_, err := tls.ParseCipherSuites([]string{"NOT_A_REAL_CIPHER_SUITE"})
		Expect(err).To(HaveOccurred())
	})
})

var _ = Describe("SetupTLS", func() {
	It("rejects an invalid min version", func() {
		_, err := tls.SetupTLS("bogus", nil)
		Expect(err).To(HaveOccurred())
	})

	It("rejects an invalid cipher suite", func() {
		_, err := tls.SetupTLS("VersionTLS12", []string{"NOT_A_REAL_CIPHER_SUITE"})
		Expect(err).To(HaveOccurred())
	})

	It("configures a tls.Config from valid input", func() {
		suites := cryptotls.CipherSuites()
		Expect(suites).NotTo(BeEmpty())

		opts, err := tls.SetupTLS("VersionTLS13", []string{suites[0].Name})
		Expect(err).NotTo(HaveOccurred())
		Expect(opts).NotTo(BeEmpty())

		cfg := &cryptotls.Config{}
		for _, opt := range opts {
			opt(cfg)
		}

		Expect(cfg.MinVersion).To(Equal(uint16(cryptotls.VersionTLS13)))
		Expect(cfg.CipherSuites).To(Equal([]uint16{suites[0].ID}))
		Expect(cfg.NextProtos).To(Equal([]string{"h2", "http/1.1"}))
	})

	It("leaves CipherSuites unset when none are given", func() {
		opts, err := tls.SetupTLS("VersionTLS12", nil)
		Expect(err).NotTo(HaveOccurred())

		cfg := &cryptotls.Config{}
		for _, opt := range opts {
			opt(cfg)
		}

		Expect(cfg.CipherSuites).To(BeNil())
	})
})
