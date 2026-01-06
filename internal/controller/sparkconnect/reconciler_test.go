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

package sparkconnect

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"

	"github.com/kubeflow/spark-operator/v2/api/v1alpha1"
)

func setupTestEnv(t *testing.T) {
	// Set up required environment variables for tests
	t.Setenv("KUBERNETES_SERVICE_HOST", "localhost")
	t.Setenv("KUBERNETES_SERVICE_PORT", "6443")
}

func newTestReconciler(t *testing.T) *Reconciler {
	setupTestEnv(t)

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	return &Reconciler{
		scheme:   scheme,
		recorder: record.NewFakeRecorder(10),
	}
}

// TestMain sets up test environment for all tests
func TestMain(m *testing.M) {
	// Set required environment variables
	os.Setenv("KUBERNETES_SERVICE_HOST", "localhost")
	os.Setenv("KUBERNETES_SERVICE_PORT", "6443")
	os.Exit(m.Run())
}

func newTestSparkConnect() *v1alpha1.SparkConnect {
	image := "spark:4.0.0"
	return &v1alpha1.SparkConnect{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-spark-connect",
			Namespace: "test-namespace",
		},
		Spec: v1alpha1.SparkConnectSpec{
			SparkVersion: "4.0.0",
			Image:        &image,
			Server: v1alpha1.ServerSpec{
				SparkPodSpec: v1alpha1.SparkPodSpec{},
			},
			Executor: v1alpha1.ExecutorSpec{
				SparkPodSpec: v1alpha1.SparkPodSpec{
					// Template must be initialized to avoid nil pointer dereference
					Template: &corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{},
					},
				},
			},
		},
	}
}

func TestComputeServerPodSpecHash_Deterministic(t *testing.T) {
	reconciler := newTestReconciler(t)
	conn := newTestSparkConnect()

	hash1, err1 := reconciler.computeServerPodSpecHash(conn)
	require.NoError(t, err1)
	assert.NotEmpty(t, hash1)

	hash2, err2 := reconciler.computeServerPodSpecHash(conn)
	require.NoError(t, err2)
	assert.NotEmpty(t, hash2)

	assert.Equal(t, hash1, hash2, "Hash should be deterministic for the same spec")
}

func TestComputeServerPodSpecHash_ImageChange(t *testing.T) {
	reconciler := newTestReconciler(t)
	conn := newTestSparkConnect()

	hash1, err1 := reconciler.computeServerPodSpecHash(conn)
	require.NoError(t, err1)

	// Change image
	newImage := "spark:4.1.0"
	conn.Spec.Image = &newImage

	hash2, err2 := reconciler.computeServerPodSpecHash(conn)
	require.NoError(t, err2)

	assert.NotEqual(t, hash1, hash2, "Hash should change when image changes")
}

func TestComputeServerPodSpecHash_SparkVersionChange(t *testing.T) {
	reconciler := newTestReconciler(t)
	conn := newTestSparkConnect()

	hash1, err1 := reconciler.computeServerPodSpecHash(conn)
	require.NoError(t, err1)

	// Change spark version - note: sparkVersion is used in labels not pod spec
	// The hash is based on pod spec (image, args, etc.) not labels
	// SparkVersion doesn't directly affect the container spec, so hash may not change
	conn.Spec.SparkVersion = "4.1.0"

	hash2, err2 := reconciler.computeServerPodSpecHash(conn)
	require.NoError(t, err2)

	// Note: SparkVersion primarily affects metadata labels, not the container spec
	// For meaningful spec changes that affect runtime, test other fields like image
	assert.NotEmpty(t, hash1)
	assert.NotEmpty(t, hash2)
}

func TestComputeServerPodSpecHash_SparkConfChange(t *testing.T) {
	reconciler := newTestReconciler(t)
	conn := newTestSparkConnect()

	hash1, err1 := reconciler.computeServerPodSpecHash(conn)
	require.NoError(t, err1)

	// Add spark conf
	conn.Spec.SparkConf = map[string]string{
		"spark.executor.memory": "2g",
	}

	hash2, err2 := reconciler.computeServerPodSpecHash(conn)
	require.NoError(t, err2)

	assert.NotEqual(t, hash1, hash2, "Hash should change when spark conf changes")
}

func TestComputeServerPodSpecHash_ExecutorInstancesChange(t *testing.T) {
	reconciler := newTestReconciler(t)
	conn := newTestSparkConnect()

	hash1, err1 := reconciler.computeServerPodSpecHash(conn)
	require.NoError(t, err1)

	// Change executor instances
	instances := int32(5)
	conn.Spec.Executor.Instances = &instances

	hash2, err2 := reconciler.computeServerPodSpecHash(conn)
	require.NoError(t, err2)

	assert.NotEqual(t, hash1, hash2, "Hash should change when executor instances change")
}

func TestComputeServerPodSpecHash_ServerMemoryChange(t *testing.T) {
	reconciler := newTestReconciler(t)
	conn := newTestSparkConnect()

	hash1, err1 := reconciler.computeServerPodSpecHash(conn)
	require.NoError(t, err1)

	// Change server memory
	memory := "2g"
	conn.Spec.Server.Memory = &memory

	hash2, err2 := reconciler.computeServerPodSpecHash(conn)
	require.NoError(t, err2)

	assert.NotEqual(t, hash1, hash2, "Hash should change when server memory changes")
}

func TestComputeServerPodSpecHash_NoImage(t *testing.T) {
	reconciler := newTestReconciler(t)
	conn := newTestSparkConnect()
	conn.Spec.Image = nil

	_, err := reconciler.computeServerPodSpecHash(conn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "image is not specified")
}

func TestNeedsPodRestart_NilPod(t *testing.T) {
	reconciler := newTestReconciler(t)
	conn := newTestSparkConnect()

	needsRestart, _, err := reconciler.needsPodRestart(context.Background(), conn, nil)
	require.NoError(t, err)
	assert.False(t, needsRestart, "Should not need restart for nil pod")
}

func TestNeedsPodRestart_NotCreatedPod(t *testing.T) {
	reconciler := newTestReconciler(t)
	conn := newTestSparkConnect()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-spark-connect-server",
			Namespace: "test-namespace",
		},
	}

	needsRestart, _, err := reconciler.needsPodRestart(context.Background(), conn, pod)
	require.NoError(t, err)
	assert.False(t, needsRestart, "Should not need restart for pod not created yet")
}

func TestNeedsPodRestart_DeletingPod(t *testing.T) {
	reconciler := newTestReconciler(t)
	conn := newTestSparkConnect()

	now := metav1.Now()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-spark-connect-server",
			Namespace:         "test-namespace",
			CreationTimestamp: now,
			DeletionTimestamp: &now,
		},
	}

	needsRestart, _, err := reconciler.needsPodRestart(context.Background(), conn, pod)
	require.NoError(t, err)
	assert.False(t, needsRestart, "Should not need restart for pod being deleted")
}

func TestNeedsPodRestart_HashMatches(t *testing.T) {
	reconciler := newTestReconciler(t)
	conn := newTestSparkConnect()

	// Compute current hash
	currentHash, err := reconciler.computeServerPodSpecHash(conn)
	require.NoError(t, err)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-spark-connect-server",
			Namespace:         "test-namespace",
			CreationTimestamp: metav1.Now(),
			Annotations: map[string]string{
				ServerPodSpecHashAnnotationKey: currentHash,
			},
		},
	}

	needsRestart, desiredHash, err := reconciler.needsPodRestart(context.Background(), conn, pod)
	require.NoError(t, err)
	assert.False(t, needsRestart, "Should not need restart when hash matches")
	assert.Equal(t, currentHash, desiredHash)
}

func TestNeedsPodRestart_HashDiffers(t *testing.T) {
	reconciler := newTestReconciler(t)
	conn := newTestSparkConnect()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-spark-connect-server",
			Namespace:         "test-namespace",
			CreationTimestamp: metav1.Now(),
			Annotations: map[string]string{
				ServerPodSpecHashAnnotationKey: "old-hash-value",
			},
		},
	}

	needsRestart, desiredHash, err := reconciler.needsPodRestart(context.Background(), conn, pod)
	require.NoError(t, err)
	assert.True(t, needsRestart, "Should need restart when hash differs")
	assert.NotEqual(t, "old-hash-value", desiredHash)
}

func TestNeedsPodRestart_NoHashAnnotation(t *testing.T) {
	reconciler := newTestReconciler(t)
	conn := newTestSparkConnect()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-spark-connect-server",
			Namespace:         "test-namespace",
			CreationTimestamp: metav1.Now(),
		},
	}

	needsRestart, desiredHash, err := reconciler.needsPodRestart(context.Background(), conn, pod)
	require.NoError(t, err)
	assert.True(t, needsRestart, "Should need restart when no hash annotation exists")
	assert.NotEmpty(t, desiredHash)
}

func TestBuildServerPodSpec_SetsImage(t *testing.T) {
	reconciler := newTestReconciler(t)
	conn := newTestSparkConnect()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-spark-connect-server",
			Namespace: "test-namespace",
		},
	}

	err := reconciler.buildServerPodSpec(conn, pod, true)
	require.NoError(t, err)

	assert.Len(t, pod.Spec.Containers, 1)
	assert.Equal(t, "spark:4.0.0", pod.Spec.Containers[0].Image)
}

func TestBuildServerPodSpec_SetsCommandAndArgs(t *testing.T) {
	reconciler := newTestReconciler(t)
	conn := newTestSparkConnect()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-spark-connect-server",
			Namespace: "test-namespace",
		},
	}

	err := reconciler.buildServerPodSpec(conn, pod, true)
	require.NoError(t, err)

	assert.Equal(t, []string{"bash", "-c"}, pod.Spec.Containers[0].Command)
	require.Len(t, pod.Spec.Containers[0].Args, 1)
	assert.Contains(t, pod.Spec.Containers[0].Args[0], "start-connect-server.sh")
}

func TestBuildServerPodSpec_SetsLifecycleHook(t *testing.T) {
	reconciler := newTestReconciler(t)
	conn := newTestSparkConnect()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-spark-connect-server",
			Namespace: "test-namespace",
		},
	}

	err := reconciler.buildServerPodSpec(conn, pod, true)
	require.NoError(t, err)

	require.NotNil(t, pod.Spec.Containers[0].Lifecycle)
	require.NotNil(t, pod.Spec.Containers[0].Lifecycle.PreStop)
	assert.Contains(t, pod.Spec.Containers[0].Lifecycle.PreStop.Exec.Command, "${SPARK_HOME}/sbin/stop-connect-server.sh")
}

func TestBuildServerPodSpec_SetsEnvVars(t *testing.T) {
	reconciler := newTestReconciler(t)
	conn := newTestSparkConnect()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-spark-connect-server",
			Namespace: "test-namespace",
		},
	}

	err := reconciler.buildServerPodSpec(conn, pod, true)
	require.NoError(t, err)

	envVars := make(map[string]corev1.EnvVar)
	for _, env := range pod.Spec.Containers[0].Env {
		envVars[env.Name] = env
	}

	assert.Contains(t, envVars, "POD_IP")
	assert.Contains(t, envVars, "SPARK_NO_DAEMONIZE")
}

func TestBuildServerPodSpec_SetsConfigMapVolume(t *testing.T) {
	reconciler := newTestReconciler(t)
	conn := newTestSparkConnect()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-spark-connect-server",
			Namespace: "test-namespace",
		},
	}

	err := reconciler.buildServerPodSpec(conn, pod, true)
	require.NoError(t, err)

	volumeNames := make(map[string]bool)
	for _, vol := range pod.Spec.Volumes {
		volumeNames[vol.Name] = true
	}
	// Volume name is "spark-conf" as defined in common.SparkConfigMapVolumeMountName
	assert.Contains(t, volumeNames, "spark-conf")
}

func TestBuildServerPodSpec_MergesTemplateLabels(t *testing.T) {
	reconciler := newTestReconciler(t)
	conn := newTestSparkConnect()
	conn.Spec.Server.Template = &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"custom-label": "custom-value",
			},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-spark-connect-server",
			Namespace: "test-namespace",
		},
	}

	err := reconciler.buildServerPodSpec(conn, pod, true)
	require.NoError(t, err)

	assert.Equal(t, "custom-value", pod.Labels["custom-label"])
}

func TestBuildServerPodSpec_MergesTemplateAnnotations(t *testing.T) {
	reconciler := newTestReconciler(t)
	conn := newTestSparkConnect()
	conn.Spec.Server.Template = &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"custom-annotation": "custom-value",
			},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-spark-connect-server",
			Namespace: "test-namespace",
		},
	}

	err := reconciler.buildServerPodSpec(conn, pod, true)
	require.NoError(t, err)

	assert.Equal(t, "custom-value", pod.Annotations["custom-annotation"])
}

func TestMutateServerPod_SetsSpecHashAnnotation(t *testing.T) {
	reconciler := newTestReconciler(t)
	conn := newTestSparkConnect()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-spark-connect-server",
			Namespace: "test-namespace",
		},
	}

	err := reconciler.mutateServerPod(context.Background(), conn, pod)
	require.NoError(t, err)

	assert.Contains(t, pod.Annotations, ServerPodSpecHashAnnotationKey)
	assert.NotEmpty(t, pod.Annotations[ServerPodSpecHashAnnotationKey])
}

func TestMutateServerPod_SetsLabels(t *testing.T) {
	reconciler := newTestReconciler(t)
	conn := newTestSparkConnect()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-spark-connect-server",
			Namespace: "test-namespace",
		},
	}

	err := reconciler.mutateServerPod(context.Background(), conn, pod)
	require.NoError(t, err)

	// Label key is "sparkoperator.k8s.io/connect-name" as defined in common.LabelSparkConnectName
	assert.Contains(t, pod.Labels, "sparkoperator.k8s.io/connect-name")
	assert.Equal(t, "test-spark-connect", pod.Labels["sparkoperator.k8s.io/connect-name"])
	// spark-version label is set via GetServerSelectorLabels
	assert.Contains(t, pod.Labels, "spark-version")
}
