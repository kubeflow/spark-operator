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
