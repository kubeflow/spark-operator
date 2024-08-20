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

package yunikorn

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMergeMaps(t *testing.T) {
	testCases := []struct {
		m1       map[string]string
		m2       map[string]string
		expected map[string]string
	}{
		{
			m1:       map[string]string{},
			m2:       map[string]string{},
			expected: nil,
		},
		{
			m1:       map[string]string{"key1": "value1"},
			m2:       map[string]string{},
			expected: map[string]string{"key1": "value1"},
		},
		{
			m1:       map[string]string{},
			m2:       map[string]string{"key1": "value1"},
			expected: map[string]string{"key1": "value1"},
		},
		{
			m1:       map[string]string{"key1": "value1"},
			m2:       map[string]string{"key2": "value2"},
			expected: map[string]string{"key1": "value1", "key2": "value2"},
		},
		{
			m1:       map[string]string{"key1": "value1"},
			m2:       map[string]string{"key1": "value2", "key2": "value2"},
			expected: map[string]string{"key1": "value2", "key2": "value2"},
		},
	}

	for _, tc := range testCases {
		assert.Equal(t, tc.expected, mergeMaps(tc.m1, tc.m2))
	}
}
