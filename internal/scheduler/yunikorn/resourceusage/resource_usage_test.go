package resourceusage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kubeflow/spark-operator/pkg/util"
)

func TestCpuRequest(t *testing.T) {
	testCases := []struct {
		cores       *int32
		coreRequest *string
		expected    string
	}{
		{nil, nil, "1"},
		{util.Int32Ptr(1), nil, "1"},
		{nil, util.StringPtr("1"), "1"},
		{util.Int32Ptr(1), util.StringPtr("500m"), "500m"},
	}

	for _, tc := range testCases {
		actual, err := cpuRequest(tc.cores, tc.coreRequest)
		assert.Nil(t, err)
		assert.Equal(t, tc.expected, actual)
	}
}

func TestCpuRequestInvalid(t *testing.T) {
	invalidInputs := []string{
		"",
		"asd",
		"Random 500m",
	}

	for _, input := range invalidInputs {
		_, err := cpuRequest(nil, &input)
		assert.NotNil(t, err)
	}
}
