/*
Copyright The Kubeflow Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package resourceusage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestByteStringAsMb(t *testing.T) {
	testCases := []struct {
		input       string
		defaultUnit DefaultUnit
		expected    int64
	}{
		// Suffixed inputs — defaultUnit is ignored, should produce the same result regardless.
		{"1k", DefaultUnitMiB, 1024},
		{"1m", DefaultUnitMiB, 1024 * 1024},
		{"1g", DefaultUnitMiB, 1024 * 1024 * 1024},
		{"1t", DefaultUnitMiB, 1024 * 1024 * 1024 * 1024},
		{"1p", DefaultUnitMiB, 1024 * 1024 * 1024 * 1024 * 1024},
		// Two-letter suffixes.
		{"1kb", DefaultUnitMiB, 1024},
		{"1mb", DefaultUnitMiB, 1024 * 1024},
		{"1gb", DefaultUnitMiB, 1024 * 1024 * 1024},
		{"1tb", DefaultUnitMiB, 1024 * 1024 * 1024 * 1024},
		{"1pb", DefaultUnitMiB, 1024 * 1024 * 1024 * 1024 * 1024},
		// Uppercase suffixes: strings.ToLower normalises them before matching.
		{"1G", DefaultUnitMiB, 1024 * 1024 * 1024},
		{"1GB", DefaultUnitMiB, 1024 * 1024 * 1024},
		{"1M", DefaultUnitMiB, 1024 * 1024},
		// Bare number with DefaultUnitMiB: treated as mebibytes (spark.driver.memory,
		// spark.executor.memory, spark.*.memoryOverhead, spark.executor.pyspark.memory).
		{"1024", DefaultUnitMiB, 1024 * 1024 * 1024}, // 1024 MiB = 1 GiB
		{"512", DefaultUnitMiB, 512 * 1024 * 1024},   // 512 MiB
		// Bare number with DefaultUnitBytes: treated as bytes.
		// Used for spark.memory.offHeap.size.
		{"1024", DefaultUnitBytes, 1024}, // 1024 bytes
		{"512", DefaultUnitBytes, 512},   // 512 bytes
		// Suffixed inputs with DefaultUnitBytes: suffix still wins.
		{"1m", DefaultUnitBytes, 1024 * 1024},
	}

	for _, tc := range testCases {
		t.Run(tc.input+"/"+func() string {
			if tc.defaultUnit == DefaultUnitMiB {
				return "MiB"
			}
			return "Bytes"
		}(), func(t *testing.T) {
			actual, err := byteStringAsBytes(tc.input, tc.defaultUnit)
			assert.Nil(t, err)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestByteStringAsMbInvalid(t *testing.T) {
	invalidInputs := []string{
		"0.064",
		"0.064m",
		"500ub",
		"This breaks 600b",
		"This breaks 600",
		"600gb This breaks",
		"This 123mb breaks",
		"",
	}

	for _, input := range invalidInputs {
		t.Run(input, func(t *testing.T) {
			_, err := byteStringAsBytes(input, DefaultUnitMiB)
			assert.NotNil(t, err)
		})
	}
}

func TestByteStringAsBytesOverflow(t *testing.T) {
	// Suffix overflow: "99999999pb" overflows int64.
	_, err := byteStringAsBytes("99999999pb", DefaultUnitMiB)
	assert.NotNil(t, err, "expected overflow error for 99999999pb")
	assert.Contains(t, err.Error(), "overflow")

	// Suffix boundary: MaxInt64/2^50 = 8191, so "8191p" succeeds and "8192p" errors.
	result, err := byteStringAsBytes("8191p", DefaultUnitMiB)
	assert.Nil(t, err, "8191p should not overflow")
	assert.Equal(t, int64(8191)*(1<<50), result)

	_, err = byteStringAsBytes("8192p", DefaultUnitMiB)
	assert.NotNil(t, err, "8192p should overflow")
	assert.Contains(t, err.Error(), "overflow")

	// Bare-number MiB overflow: MaxInt64/(1<<20) = 8796093022207.
	// "8796093022207" succeeds; "8796093022208" errors.
	result, err = byteStringAsBytes("8796093022207", DefaultUnitMiB)
	assert.Nil(t, err, "8796093022207 MiB should not overflow")
	assert.Equal(t, int64(8796093022207)*(1<<20), result)

	_, err = byteStringAsBytes("8796093022208", DefaultUnitMiB)
	assert.NotNil(t, err, "8796093022208 MiB should overflow")
	assert.Contains(t, err.Error(), "overflow")

	// Bare-number Bytes: a large value below MaxInt64 succeeds (no overflow guard needed).
	result, err = byteStringAsBytes("9223372036854775807", DefaultUnitBytes) // MaxInt64
	assert.Nil(t, err, "MaxInt64 bytes should not overflow")
	assert.Equal(t, int64(9223372036854775807), result)
}
