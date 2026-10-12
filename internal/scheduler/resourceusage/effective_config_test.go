/*
Copyright The Kubeflow Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package resourceusage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

// TestEffectiveConfigPrecedence verifies the resolver honours the real
// spark-submit arg-builder precedence:
//
//   - CRD memory fields   > sparkConf memory fields    > default 1 GiB
//   - CRD overhead fields > sparkConf overhead fields  > factor calculation
//   - sparkConf factor    > CRD MemoryOverheadFactor   > language default
//
// (See effective_config.go for the full precedence rationale.)
func TestEffectiveConfigPrecedence(t *testing.T) {
	assertMemMi := func(t *testing.T, app *v1beta2.SparkApplication, wantMi int64, role string) {
		t.Helper()
		var (
			rl  corev1.ResourceList
			err error
		)
		if role == "driver" {
			rl, err = DriverPodResourceList(app)
		} else {
			rl, err = ExecutorPodResourceList(app)
		}
		require.NoError(t, err)
		want := resource.MustParse(fmt.Sprintf("%dMi", wantMi))
		got := rl[corev1.ResourceMemory]
		assert.Equal(t, want.Value(), got.Value(),
			"%s memory: got %s, want %dMi", role, got.String(), wantMi)
	}

	// ------------------------------------------------------------------ heap

	t.Run("raw sparkConf memory used when CRD memory is nil", func(t *testing.T) {
		// sparkConf spark.executor.memory=2g: bare suffix g = GiB in Java
		// heap = 2 GiB = 2048 MiB; JVM factor 0.1; overhead floor 384 MiB
		// total = 2048 + 384 = 2432 MiB
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type: v1beta2.SparkApplicationTypeJava,
				SparkConf: map[string]string{
					common.SparkExecutorMemory: "2g",
				},
				Executor: v1beta2.ExecutorSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Cores: ptr.To[int32](1),
						// Memory nil — sparkConf should supply it
					},
				},
			},
		}
		assertMemMi(t, app, 2432, "executor")
	})

	t.Run("raw sparkConf driver memory used when CRD driver memory is nil", func(t *testing.T) {
		// spark.driver.memory=4g; JVM factor 0.1
		// overhead = max(4096*0.1, 384) = max(409.6→409, 384) = 409 MiB
		// total = 4096 + 409 = 4505 MiB
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type: v1beta2.SparkApplicationTypeJava,
				SparkConf: map[string]string{
					common.SparkDriverMemory: "4g",
				},
				Driver: v1beta2.DriverSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Cores: ptr.To[int32](1),
					},
				},
			},
		}
		assertMemMi(t, app, 4505, "driver")
	})

	t.Run("CRD memory wins over sparkConf for same key", func(t *testing.T) {
		// CRD = 1g, sparkConf = 4g → CRD wins → heap = 1024 MiB
		// overhead floor = 384 MiB; total = 1408 MiB
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type: v1beta2.SparkApplicationTypeJava,
				SparkConf: map[string]string{
					common.SparkExecutorMemory: "4g", // should be ignored
				},
				Executor: v1beta2.ExecutorSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Memory: ptr.To("1g"), // CRD wins
						Cores:  ptr.To[int32](1),
					},
				},
			},
		}
		assertMemMi(t, app, 1408, "executor")
	})

	// -------------------------------------------------------------- overhead

	t.Run("sparkConf overhead used when CRD overhead is nil", func(t *testing.T) {
		// heap = 1g = 1024 MiB; explicit overhead via sparkConf = 512m = 512 MiB
		// total = 1024 + 512 = 1536 MiB (factor not applied)
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type: v1beta2.SparkApplicationTypeJava,
				SparkConf: map[string]string{
					common.SparkExecutorMemoryOverhead: "512m",
				},
				Executor: v1beta2.ExecutorSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Memory: ptr.To("1g"),
						Cores:  ptr.To[int32](1),
					},
				},
			},
		}
		assertMemMi(t, app, 1536, "executor")
	})

	t.Run("CRD overhead wins over sparkConf overhead", func(t *testing.T) {
		// CRD overhead = 256m = 256 MiB; sparkConf overhead = 512m (ignored)
		// heap = 1g = 1024 MiB; total = 1024 + 256 = 1280 MiB
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type: v1beta2.SparkApplicationTypeJava,
				SparkConf: map[string]string{
					common.SparkExecutorMemoryOverhead: "512m", // should be ignored
				},
				Executor: v1beta2.ExecutorSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Memory:         ptr.To("1g"),
						MemoryOverhead: ptr.To("256m"), // CRD wins
						Cores:          ptr.To[int32](1),
					},
				},
			},
		}
		assertMemMi(t, app, 1280, "executor")
	})

	// ----------------------------------------------------------- factor

	t.Run("sparkConf factor wins over CRD MemoryOverheadFactor", func(t *testing.T) {
		// sparkConf spark.kubernetes.memoryOverheadFactor=0.5 → factor = 0.5
		// CRD MemoryOverheadFactor=0.1 (should be ignored)
		// heap = 4g = 4096 MiB
		// overhead = max(4096*1024*1024 * 0.5, 384*1024*1024)
		//          = max(2147483648, 402653184) = 2147483648 bytes
		//          = 2048 MiB
		// total = 4096 + 2048 = 6144 MiB
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type:                 v1beta2.SparkApplicationTypeJava,
				MemoryOverheadFactor: ptr.To("0.1"), // should be overridden
				SparkConf: map[string]string{
					common.SparkKubernetesMemoryOverheadFactor: "0.5", // wins
				},
				Executor: v1beta2.ExecutorSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Memory: ptr.To("4g"),
						Cores:  ptr.To[int32](1),
					},
				},
			},
		}
		assertMemMi(t, app, 6144, "executor")
	})

	t.Run("CRD MemoryOverheadFactor used when sparkConf factor absent", func(t *testing.T) {
		// CRD factor = 0.5, no sparkConf factor key
		// heap = 4g = 4096 MiB; overhead = 2048 MiB; total = 6144 MiB
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type:                 v1beta2.SparkApplicationTypeJava,
				MemoryOverheadFactor: ptr.To("0.5"),
				Executor: v1beta2.ExecutorSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Memory: ptr.To("4g"),
						Cores:  ptr.To[int32](1),
					},
				},
			},
		}
		assertMemMi(t, app, 6144, "executor")
	})

	t.Run("Python default factor (0.4) applies when no factor key set", func(t *testing.T) {
		// heap = 1024 MiB; factor 0.4; overhead = 409 MiB; total = 1433 MiB
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type: v1beta2.SparkApplicationTypePython,
				Driver: v1beta2.DriverSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Memory: ptr.To("1g"),
						Cores:  ptr.To[int32](1),
					},
				},
			},
		}
		assertMemMi(t, app, 1433, "driver")
	})

	t.Run("raw sparkConf factor=0.5 with 4g executor — reviewer scenario", func(t *testing.T) {
		// Reviewer's specific scenario: 4GiB executor + sparkConf factor=0.5
		// must NOT use the JVM default 0.1.
		// heap = 4096 MiB; overhead = 2048 MiB; total = 6144 MiB
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type: v1beta2.SparkApplicationTypeJava,
				SparkConf: map[string]string{
					common.SparkKubernetesMemoryOverheadFactor: "0.5",
				},
				Executor: v1beta2.ExecutorSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Memory: ptr.To("4g"),
						Cores:  ptr.To[int32](1),
					},
				},
			},
		}
		assertMemMi(t, app, 6144, "executor")
	})

	// -------------------------------------------------------- invalid values

	t.Run("invalid sparkConf memory returns error", func(t *testing.T) {
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type: v1beta2.SparkApplicationTypeJava,
				SparkConf: map[string]string{
					common.SparkExecutorMemory: "not-a-number",
				},
				Executor: v1beta2.ExecutorSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{Cores: ptr.To[int32](1)},
				},
			},
		}
		_, err := ExecutorPodResourceList(app)
		require.Error(t, err)
		assert.Contains(t, err.Error(), common.SparkExecutorMemory)
	})

	t.Run("invalid sparkConf factor returns error", func(t *testing.T) {
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type: v1beta2.SparkApplicationTypeJava,
				SparkConf: map[string]string{
					common.SparkKubernetesMemoryOverheadFactor: "not-a-float",
				},
				Executor: v1beta2.ExecutorSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Memory: ptr.To("1g"),
						Cores:  ptr.To[int32](1),
					},
				},
			},
		}
		_, err := ExecutorPodResourceList(app)
		require.Error(t, err)
		assert.Contains(t, err.Error(), common.SparkKubernetesMemoryOverheadFactor)
	})
}
