/*
Copyright 2024 The Kubeflow authors.

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

package resourceusage

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("byteStringAsBytes", func() {
	DescribeTable("Should convert a Java-style byte string to bytes",
		func(input string, expected int64) {
			actual, err := byteStringAsBytes(input)
			Expect(err).NotTo(HaveOccurred())
			Expect(actual).To(Equal(expected))
		},
		Entry("kibibytes", "1k", int64(1024)),
		Entry("mebibytes", "1m", int64(1024*1024)),
		Entry("gibibytes", "1g", int64(1024*1024*1024)),
		Entry("tebibytes", "1t", int64(1024*1024*1024*1024)),
		Entry("pebibytes", "1p", int64(1024*1024*1024*1024*1024)),
	)

	DescribeTable("Should return an error for a byte string that cannot be parsed",
		func(input string) {
			_, err := byteStringAsBytes(input)
			Expect(err).To(HaveOccurred())
		},
		Entry("fractional value without a suffix", "0.064"),
		Entry("fractional value with a suffix", "0.064m"),
		Entry("unknown suffix", "500ub"),
		Entry("leading text with a valid value", "This breaks 600b"),
		Entry("leading text without a suffix", "This breaks 600"),
		Entry("trailing text after a valid value", "600gb This breaks"),
		Entry("value embedded in text", "This 123mb breaks"),
	)
})
