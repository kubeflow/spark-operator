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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestByteStringAsMb(t *testing.T) {
	testCases := []struct {
		input    string
		expected int
	}{
		{"1k", 1024},
		{"1m", 1024 * 1024},
		{"1g", 1024 * 1024 * 1024},
		{"1t", 1024 * 1024 * 1024 * 1024},
		{"1p", 1024 * 1024 * 1024 * 1024 * 1024},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			actual, err := byteStringAsBytes(tc.input)
			assert.Nil(t, err)
			assert.Equal(t, int64(tc.expected), actual)
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
	}

	for _, input := range invalidInputs {
		t.Run(input, func(t *testing.T) {
			_, err := byteStringAsBytes(input)
			assert.NotNil(t, err)
		})
	}
}
