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

// TestPerRoleOverheadFactor covers the per-role sparkConf keys introduced in
// Spark 3.3: spark.driver.memoryOverheadFactor and
// spark.executor.memoryOverheadFactor.  These take precedence over the
// cluster-wide spark.kubernetes.memoryOverheadFactor key.
func TestPerRoleOverheadFactor(t *testing.T) {
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

	t.Run("executor per-role key beats kubernetes-wide key", func(t *testing.T) {
		// spark.executor.memoryOverheadFactor=0.5 wins over
		// spark.kubernetes.memoryOverheadFactor=0.1
		// heap = 4g = 4096 MiB; overhead = 4096*0.5 = 2048 MiB; total = 6144 MiB
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type: v1beta2.SparkApplicationTypeJava,
				SparkConf: map[string]string{
					common.SparkExecutorMemoryOverheadFactor:   "0.5",
					common.SparkKubernetesMemoryOverheadFactor: "0.1", // ignored
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

	t.Run("driver per-role key beats kubernetes-wide key", func(t *testing.T) {
		// spark.driver.memoryOverheadFactor=0.5 wins over
		// spark.kubernetes.memoryOverheadFactor=0.1
		// heap = 4g = 4096 MiB; overhead = 2048 MiB; total = 6144 MiB
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type: v1beta2.SparkApplicationTypeJava,
				SparkConf: map[string]string{
					common.SparkDriverMemoryOverheadFactor:     "0.5",
					common.SparkKubernetesMemoryOverheadFactor: "0.1", // ignored
				},
				Driver: v1beta2.DriverSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Memory: ptr.To("4g"),
						Cores:  ptr.To[int32](1),
					},
				},
			},
		}
		assertMemMi(t, app, 6144, "driver")
	})

	t.Run("executor per-role key applies only to executor, not driver", func(t *testing.T) {
		// spark.executor.memoryOverheadFactor=0.5 → executor uses 0.5
		// driver has no factor key → driver uses JVM default 0.1
		// executor: 4096 + 2048 = 6144 MiB
		// driver:   4096 + max(4096*0.1→409, 384) = 4096 + 409 = 4505 MiB
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type: v1beta2.SparkApplicationTypeJava,
				SparkConf: map[string]string{
					common.SparkExecutorMemoryOverheadFactor: "0.5",
				},
				Driver: v1beta2.DriverSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Memory: ptr.To("4g"),
						Cores:  ptr.To[int32](1),
					},
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
		assertMemMi(t, app, 4505, "driver") // executor key has no effect on driver
	})

	t.Run("driver per-role key applies only to driver, not executor", func(t *testing.T) {
		// spark.driver.memoryOverheadFactor=0.5 → driver uses 0.5
		// executor has no factor key → uses JVM default 0.1
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type: v1beta2.SparkApplicationTypeJava,
				SparkConf: map[string]string{
					common.SparkDriverMemoryOverheadFactor: "0.5",
				},
				Driver: v1beta2.DriverSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Memory: ptr.To("4g"),
						Cores:  ptr.To[int32](1),
					},
				},
				Executor: v1beta2.ExecutorSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Memory: ptr.To("4g"),
						Cores:  ptr.To[int32](1),
					},
				},
			},
		}
		assertMemMi(t, app, 6144, "driver")
		assertMemMi(t, app, 4505, "executor") // driver key has no effect on executor
	})

	t.Run("per-role key beats CRD MemoryOverheadFactor", func(t *testing.T) {
		// CRD MemoryOverheadFactor=0.1; sparkConf executor key=0.5
		// executor should use 0.5, not 0.1
		// heap = 4g; overhead = 2048 MiB; total = 6144 MiB
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type:                 v1beta2.SparkApplicationTypeJava,
				MemoryOverheadFactor: ptr.To("0.1"),
				SparkConf: map[string]string{
					common.SparkExecutorMemoryOverheadFactor: "0.5",
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

	t.Run("invalid executor per-role factor returns error naming the key", func(t *testing.T) {
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type: v1beta2.SparkApplicationTypeJava,
				SparkConf: map[string]string{
					common.SparkExecutorMemoryOverheadFactor: "not-a-float",
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
		assert.Contains(t, err.Error(), common.SparkExecutorMemoryOverheadFactor,
			"error should name the offending key")
	})

	t.Run("invalid driver per-role factor returns error naming the key", func(t *testing.T) {
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type: v1beta2.SparkApplicationTypeJava,
				SparkConf: map[string]string{
					common.SparkDriverMemoryOverheadFactor: "bad",
				},
				Driver: v1beta2.DriverSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Memory: ptr.To("1g"),
						Cores:  ptr.To[int32](1),
					},
				},
			},
		}
		_, err := DriverPodResourceList(app)
		require.Error(t, err)
		assert.Contains(t, err.Error(), common.SparkDriverMemoryOverheadFactor,
			"error should name the offending key")
	})

	// Memory-omitted cases (CRD and sparkConf both unset) — confirming
	// defaults from TestDefaultHeapWhenMemoryOmitted also hold in
	// the effective-config resolver path.
	t.Run("memory omitted JVM driver — default 1g heap = 1408 MiB", func(t *testing.T) {
		// heap = 1024 MiB (default); JVM factor 0.1; overhead floor 384 MiB
		// total = 1024 + 384 = 1408 MiB
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type: v1beta2.SparkApplicationTypeJava,
				Driver: v1beta2.DriverSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{Cores: ptr.To[int32](1)},
				},
			},
		}
		assertMemMi(t, app, 1408, "driver")
	})

	t.Run("memory omitted Python executor — default 1g heap = 1433 MiB", func(t *testing.T) {
		// heap = 1024 MiB (default); Python factor 0.4
		// overhead = int64(1024*1024*1024 * 0.4) bytes = 429496729 bytes → 409 MiB
		// total = 1024 + 409 = 1433 MiB
		app := &v1beta2.SparkApplication{
			Spec: v1beta2.SparkApplicationSpec{
				Type: v1beta2.SparkApplicationTypePython,
				Executor: v1beta2.ExecutorSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{Cores: ptr.To[int32](1)},
				},
			},
		}
		assertMemMi(t, app, 1433, "executor")
	})
}

// TestOverheadFactorValidation verifies that NaN, Inf, and negative overhead
// factors are rejected with a clear error naming the offending key.
func TestOverheadFactorValidation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		key   string
	}{
		{"NaN via per-role key", "NaN", common.SparkExecutorMemoryOverheadFactor},
		{"Inf via per-role key", "Inf", common.SparkExecutorMemoryOverheadFactor},
		{"-Inf via cluster-wide key", "-Inf", common.SparkKubernetesMemoryOverheadFactor},
		{"negative via per-role key", "-0.1", common.SparkExecutorMemoryOverheadFactor},
		{"negative via cluster-wide key", "-1", common.SparkKubernetesMemoryOverheadFactor},
		{"negative via CRD field", "-0.5", "spec.MemoryOverheadFactor"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var app *v1beta2.SparkApplication
			if tc.key == "spec.MemoryOverheadFactor" {
				app = &v1beta2.SparkApplication{
					Spec: v1beta2.SparkApplicationSpec{
						Type:                 v1beta2.SparkApplicationTypeJava,
						MemoryOverheadFactor: ptr.To(tc.value),
						Executor: v1beta2.ExecutorSpec{
							SparkPodSpec: v1beta2.SparkPodSpec{
								Memory: ptr.To("1g"),
								Cores:  ptr.To[int32](1),
							},
						},
					},
				}
			} else {
				app = &v1beta2.SparkApplication{
					Spec: v1beta2.SparkApplicationSpec{
						Type:      v1beta2.SparkApplicationTypeJava,
						SparkConf: map[string]string{tc.key: tc.value},
						Executor: v1beta2.ExecutorSpec{
							SparkPodSpec: v1beta2.SparkPodSpec{
								Memory: ptr.To("1g"),
								Cores:  ptr.To[int32](1),
							},
						},
					},
				}
			}
			_, err := ExecutorPodResourceList(app)
			require.Error(t, err, "expected error for factor %q", tc.value)
			// Error must name the key or field so the user knows what to fix.
			if tc.key != "spec.MemoryOverheadFactor" {
				assert.Contains(t, err.Error(), tc.key,
					"error should name the offending sparkConf key")
			} else {
				assert.Contains(t, err.Error(), "MemoryOverheadFactor",
					"error should mention MemoryOverheadFactor")
			}
		})
	}
}
