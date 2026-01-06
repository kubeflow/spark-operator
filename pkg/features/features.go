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
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/util/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
)

const (
	// PartialRestart enables skipping reconcile for webhook-patched executor fields.
	// When enabled, changes to executor's PriorityClassName, NodeSelector, Tolerations,
	// Affinity, and SchedulerName will not trigger application restart since these fields
	// are applied by the mutating webhook when new pods are created.
	//
	// owner: @Kevinz857
	// alpha: v2.5.0
	PartialRestart featuregate.Feature = "PartialRestart"

	// LoadSparkDefaults enables loading of Spark default properties file (e.g. `${SPARK_CONF_DIR}/spark-defaults.conf` or `${SPARK_HOME}/conf/spark-defaults.conf`).
	// When enabled, operator will add `--load-spark-defaults` flag to spark-submit command (available since Spark 4.0.0).
	// See https://github.com/kubeflow/spark-operator/issues/2795.
	//
	// owner: @ChenYi015
	// alpha: v2.5.0
	LoadSparkDefaults featuregate.Feature = "LoadSparkDefaults"
)

// To add a new feature gate, follow these steps:
//
// 1. Define a new feature gate constant:
//
//	const (
//	    // MyFeature enables some new functionality.
//	    //
//	    // owner: @your-github-username
//	    // alpha: v2.x.0
//	    MyFeature featuregate.Feature = "MyFeature"
//	)
//
// 2. Add the feature gate to defaultFeatureGates map:
//
//	var defaultFeatureGates = map[featuregate.Feature]featuregate.FeatureSpec{
//	    MyFeature: {Default: false, PreRelease: featuregate.Alpha},
//	}
//
// 3. Use the feature gate in your code:
//
//	if features.Enabled(features.MyFeature) {
//	    // feature-specific code
//	}

func init() {
	runtime.Must(utilfeature.DefaultMutableFeatureGate.Add(defaultFeatureGates))
}

// defaultFeatureGates consists of all known spark-operator-specific feature keys.
// To add a new feature, define a key for it above and add it here. The features will be
// available throughout spark-operator binaries.
//
// Entries are separated from each other with blank lines to avoid sweeping gofmt changes
// when adding or removing one entry.
var defaultFeatureGates = map[featuregate.Feature]featuregate.FeatureSpec{
	PartialRestart: {Default: false, PreRelease: featuregate.Alpha},

	LoadSparkDefaults: {Default: false, PreRelease: featuregate.Alpha},
}

// SetFeatureGateDuringTest sets the specified feature gate to the specified value during a test.
func SetFeatureGateDuringTest(tb testing.TB, f featuregate.Feature, value bool) {
	featuregatetesting.SetFeatureGateDuringTest(tb, utilfeature.DefaultFeatureGate, f, value)
}

// Enabled is helper for `utilfeature.DefaultFeatureGate.Enabled()`
func Enabled(f featuregate.Feature) bool {
	return utilfeature.DefaultFeatureGate.Enabled(f)
}

// SetEnable helper function that can be used to set the enabled value of a feature gate,
// it should only be used in integration test pending the merge of
// https://github.com/kubernetes/kubernetes/pull/118346
func SetEnable(f featuregate.Feature, v bool) error {
	return utilfeature.DefaultMutableFeatureGate.Set(fmt.Sprintf("%s=%v", f, v))
}
