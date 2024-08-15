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
