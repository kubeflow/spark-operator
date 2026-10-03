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

func TestBytesToMi(t *testing.T) {
	testCases := []struct {
		input    int64
		expected string
	}{
		{(2 * 1024 * 1024) - 1, "1Mi"},
		{2 * 1024 * 1024, "2Mi"},
		{(1024 * 1024 * 1024) - 1, "1023Mi"},
		{1024 * 1024 * 1024, "1024Mi"},
	}

	for _, tc := range testCases {
		assert.Equal(t, tc.expected, bytesToMi(tc.input))
	}
}
