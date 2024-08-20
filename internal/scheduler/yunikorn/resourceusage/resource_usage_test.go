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
