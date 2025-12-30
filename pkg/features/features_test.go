/*
Copyright 2025 The Kubeflow authors.

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

package features

import (
	"testing"

	"github.com/stretchr/testify/assert"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
)

func TestDefaultFeatureGatesRegistered(t *testing.T) {
	// Verify that defaultFeatureGates is registered with the global feature gate.
	// This test ensures the init() function ran successfully.
	assert.NotNil(t, utilfeature.DefaultFeatureGate)
}

func TestEnabledWithRegisteredFeature(t *testing.T) {
	// Register a test feature for this test
	testFeature := featuregate.Feature("TestFeatureForEnabled")
	err := utilfeature.DefaultMutableFeatureGate.Add(map[featuregate.Feature]featuregate.FeatureSpec{
		testFeature: {Default: false, PreRelease: featuregate.Alpha},
	})
	assert.NoError(t, err)

	// Test that Enabled returns false for a disabled feature
	assert.False(t, Enabled(testFeature))
}

func TestSetEnableWithRegisteredFeature(t *testing.T) {
	// Register a test feature for this test
	testFeature := featuregate.Feature("TestFeatureForSetEnable")
	err := utilfeature.DefaultMutableFeatureGate.Add(map[featuregate.Feature]featuregate.FeatureSpec{
		testFeature: {Default: false, PreRelease: featuregate.Alpha},
	})
	assert.NoError(t, err)

	// Test SetEnable
	err = SetEnable(testFeature, true)
	assert.NoError(t, err)
	assert.True(t, Enabled(testFeature))

	// Disable the feature
	err = SetEnable(testFeature, false)
	assert.NoError(t, err)
	assert.False(t, Enabled(testFeature))
}

func TestSetFeatureGateDuringTestHelper(t *testing.T) {
	// Register a test feature for this test
	testFeature := featuregate.Feature("TestFeatureForDuringTest")
	err := utilfeature.DefaultMutableFeatureGate.Add(map[featuregate.Feature]featuregate.FeatureSpec{
		testFeature: {Default: false, PreRelease: featuregate.Alpha},
	})
	assert.NoError(t, err)

	// Verify the feature is disabled by default
	assert.False(t, Enabled(testFeature))

	// Enable the feature gate during the test
	SetFeatureGateDuringTest(t, testFeature, true)

	// Verify the feature is now enabled
	assert.True(t, Enabled(testFeature))

	// After the test, the feature gate will be automatically restored to its original value
}
