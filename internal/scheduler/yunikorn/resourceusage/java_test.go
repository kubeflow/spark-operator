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
