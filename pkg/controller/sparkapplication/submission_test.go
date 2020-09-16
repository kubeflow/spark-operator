/*
Copyright 2017 Google LLC

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

package sparkapplication

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/google/uuid"

	"github.com/stretchr/testify/assert"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
)

const (
	VolumeMountPathTemplate              = "spark.kubernetes.%s.volumes.%s.%s.mount.path=%s"
	VolumeMountOptionPathTemplate        = "spark.kubernetes.%s.volumes.%s.%s.options.%s=%s"
	SparkDriverLabelAnnotationTemplate   = "spark.kubernetes.driver.label.sparkoperator.k8s.io/%s=%s"
	SparkDriverLabelTemplate             = "spark.kubernetes.driver.label.%s=%s"
	SparkExecutorLabelAnnotationTemplate = "spark.kubernetes.executor.label.sparkoperator.k8s.io/%s=%s"
	SparkExecutorLabelTemplate           = "spark.kubernetes.executor.label.%s=%s"
)

func TestAddLocalDir_HostPath(t *testing.T) {
	volumes := []corev1.Volume{
		{
			Name: "spark-local-dir-1",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/tmp/mnt",
				},
			},
		},
	}

	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "spark-local-dir-1",
			MountPath: "/tmp/mnt-1",
		},
	}

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Volumes: volumes,
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					VolumeMounts: volumeMounts,
				},
			},
		},
	}

	localDirOptions, err := addLocalDirConfOptions(app)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 0, len(app.Spec.Volumes))
	assert.Equal(t, 0, len(app.Spec.Driver.VolumeMounts))
	assert.Equal(t, 2, len(localDirOptions))
	assert.Equal(t, fmt.Sprintf(VolumeMountPathTemplate, "driver", "hostPath", volumes[0].Name, volumeMounts[0].MountPath), localDirOptions[0])
	assert.Equal(t, fmt.Sprintf(VolumeMountOptionPathTemplate, "driver", "hostPath", volumes[0].Name, "path", volumes[0].HostPath.Path), localDirOptions[1])
}

func TestAddLocalDir_PVC(t *testing.T) {
	volumes := []corev1.Volume{
		{
			Name: "spark-local-dir-1",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: "/tmp/mnt-1",
				},
			},
		},
	}

	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "spark-local-dir-1",
			MountPath: "/tmp/mnt-1",
		},
	}

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Volumes: volumes,
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					VolumeMounts: volumeMounts,
				},
			},
		},
	}

	localDirOptions, err := addLocalDirConfOptions(app)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 0, len(app.Spec.Volumes))
	assert.Equal(t, 0, len(app.Spec.Driver.VolumeMounts))
	assert.Equal(t, 2, len(localDirOptions))
	assert.Equal(t, fmt.Sprintf(VolumeMountPathTemplate, "driver", "persistentVolumeClaim", volumes[0].Name, volumeMounts[0].MountPath), localDirOptions[0])
	assert.Equal(t, fmt.Sprintf(VolumeMountOptionPathTemplate, "driver", "persistentVolumeClaim", volumes[0].Name, "claimName", volumes[0].PersistentVolumeClaim.ClaimName), localDirOptions[1])
}

func TestAddLocalDir_MixedVolumes(t *testing.T) {
	volumes := []corev1.Volume{
		{
			Name: "spark-local-dir-1",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/tmp/mnt-1",
				},
			},
		},
		{
			Name: "log-dir",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/var/log/spark",
				},
			},
		},
	}

	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "spark-local-dir-1",
			MountPath: "/tmp/mnt-1",
		},
		{
			Name:      "log-dir",
			MountPath: "/var/log/spark",
		},
	}

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Volumes: volumes,
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					VolumeMounts: volumeMounts,
				},
			},
		},
	}

	localDirOptions, err := addLocalDirConfOptions(app)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 1, len(app.Spec.Volumes))
	assert.Equal(t, 1, len(app.Spec.Driver.VolumeMounts))
	assert.Equal(t, 2, len(localDirOptions))
	assert.Equal(t, fmt.Sprintf(VolumeMountPathTemplate, "driver", "hostPath", volumes[0].Name, volumeMounts[0].MountPath), localDirOptions[0])
	assert.Equal(t, fmt.Sprintf(VolumeMountOptionPathTemplate, "driver", "hostPath", volumes[0].Name, "path", volumes[0].HostPath.Path), localDirOptions[1])
}

func TestAddLocalDir_MultipleScratchVolumes(t *testing.T) {
	volumes := []corev1.Volume{
		{
			Name: "spark-local-dir-1",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/tmp/mnt-1",
				},
			},
		},
		{
			Name: "spark-local-dir-2",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/tmp/mnt-2",
				},
			},
		},
	}

	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "spark-local-dir-1",
			MountPath: "/tmp/mnt-1",
		},
		{
			Name:      "spark-local-dir-2",
			MountPath: "/tmp/mnt-2",
		},
	}

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Volumes: volumes,
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					VolumeMounts: volumeMounts,
				},
			},
		},
	}

	localDirOptions, err := addLocalDirConfOptions(app)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 0, len(app.Spec.Volumes))
	assert.Equal(t, 0, len(app.Spec.Driver.VolumeMounts))
	assert.Equal(t, 4, len(localDirOptions))
	assert.Equal(t, fmt.Sprintf(VolumeMountPathTemplate, "driver", "hostPath", volumes[0].Name, volumeMounts[0].MountPath), localDirOptions[0])
	assert.Equal(t, fmt.Sprintf(VolumeMountOptionPathTemplate, "driver", "hostPath", volumes[0].Name, "path", volumes[0].HostPath.Path), localDirOptions[1])
	assert.Equal(t, fmt.Sprintf(VolumeMountPathTemplate, "driver", "hostPath", volumes[1].Name, volumeMounts[1].MountPath), localDirOptions[2])
	assert.Equal(t, fmt.Sprintf(VolumeMountOptionPathTemplate, "driver", "hostPath", volumes[1].Name, "path", volumes[1].HostPath.Path), localDirOptions[3])
}

func TestAddLocalDir_Executor(t *testing.T) {
	volumes := []corev1.Volume{
		{
			Name: "spark-local-dir-1",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/tmp/mnt",
				},
			},
		},
	}

	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "spark-local-dir-1",
			MountPath: "/tmp/mnt-1",
		},
	}

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Volumes: volumes,
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					VolumeMounts: volumeMounts,
				},
			},
		},
	}

	localDirOptions, err := addLocalDirConfOptions(app)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 0, len(app.Spec.Volumes))
	assert.Equal(t, 0, len(app.Spec.Executor.VolumeMounts))
	assert.Equal(t, 2, len(localDirOptions))
	assert.Equal(t, fmt.Sprintf(VolumeMountPathTemplate, "executor", "hostPath", volumes[0].Name, volumeMounts[0].MountPath), localDirOptions[0])
	assert.Equal(t, fmt.Sprintf(VolumeMountOptionPathTemplate, "executor", "hostPath", volumes[0].Name, "path", volumes[0].HostPath.Path), localDirOptions[1])
}

func TestAddLocalDir_Driver_Executor(t *testing.T) {
	volumes := []corev1.Volume{
		{
			Name: "spark-local-dir-1",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/tmp/mnt",
				},
			},
		},
		{
			Name: "test-volume",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/tmp/test",
				},
			},
		},
	}

	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "spark-local-dir-1",
			MountPath: "/tmp/mnt-1",
		},
		{
			Name:      "test-volume",
			MountPath: "/tmp/test",
		},
	}

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Volumes: volumes,
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					VolumeMounts: volumeMounts,
				},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					VolumeMounts: volumeMounts,
				},
			},
		},
	}

	localDirOptions, err := addLocalDirConfOptions(app)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 1, len(app.Spec.Volumes))
	assert.Equal(t, 1, len(app.Spec.Driver.VolumeMounts))
	assert.Equal(t, 1, len(app.Spec.Executor.VolumeMounts))
	assert.Equal(t, 4, len(localDirOptions))
	assert.Equal(t, fmt.Sprintf(VolumeMountPathTemplate, "driver", "hostPath", volumes[0].Name, volumeMounts[0].MountPath), localDirOptions[0])
	assert.Equal(t, fmt.Sprintf(VolumeMountOptionPathTemplate, "driver", "hostPath", volumes[0].Name, "path", volumes[0].HostPath.Path), localDirOptions[1])
	assert.Equal(t, fmt.Sprintf(VolumeMountPathTemplate, "executor", "hostPath", volumes[0].Name, volumeMounts[0].MountPath), localDirOptions[2])
	assert.Equal(t, fmt.Sprintf(VolumeMountOptionPathTemplate, "executor", "hostPath", volumes[0].Name, "path", volumes[0].HostPath.Path), localDirOptions[3])
}

func TestPopulateLabels_Driver_Executor(t *testing.T) {
	const (
		AppLabelKey        = "app-label-key"
		AppLabelValue      = "app-label-value"
		DriverLabelKey     = "driver-label-key"
		DriverLabelValue   = "driver-label-key"
		ExecutorLabelKey   = "executor-label-key"
		ExecutorLabelValue = "executor-label-key"
	)

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "spark-test",
			UID:    "spark-test-1",
			Labels: map[string]string{AppLabelKey: AppLabelValue},
		},
		Spec: v1beta2.SparkApplicationSpec{
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Labels: map[string]string{DriverLabelKey: DriverLabelValue},
				},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Labels: map[string]string{ExecutorLabelKey: ExecutorLabelValue},
				},
			},
		},
	}

	submissionID := uuid.New().String()
	driverOptions, err := addDriverConfOptions(app, submissionID)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 5, len(driverOptions))
	sort.Strings(driverOptions)
	expectedDriverLabels := []string{
		fmt.Sprintf(SparkDriverLabelAnnotationTemplate, "launched-by-spark-operator", strconv.FormatBool(true)),
		fmt.Sprintf(SparkDriverLabelAnnotationTemplate, "app-name", "spark-test"),
		fmt.Sprintf(SparkDriverLabelAnnotationTemplate, "submission-id", submissionID),
		fmt.Sprintf(SparkDriverLabelTemplate, AppLabelKey, AppLabelValue),
		fmt.Sprintf(SparkDriverLabelTemplate, DriverLabelKey, DriverLabelValue),
	}
	sort.Strings(expectedDriverLabels)

	if !reflect.DeepEqual(expectedDriverLabels, driverOptions) {
		t.Errorf("Executor labels: wanted %+q got %+q", expectedDriverLabels, driverOptions)
	}

	executorOptions, err := addExecutorConfOptions(app, submissionID)
	sort.Strings(executorOptions)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 5, len(executorOptions))
	expectedExecutorLabels := []string{
		fmt.Sprintf(SparkExecutorLabelAnnotationTemplate, "app-name", "spark-test"),
		fmt.Sprintf(SparkExecutorLabelAnnotationTemplate, "launched-by-spark-operator", strconv.FormatBool(true)),
		fmt.Sprintf(SparkExecutorLabelAnnotationTemplate, "submission-id", submissionID),
		fmt.Sprintf(SparkExecutorLabelTemplate, AppLabelKey, AppLabelValue),
		fmt.Sprintf(SparkExecutorLabelTemplate, ExecutorLabelKey, ExecutorLabelValue),
	}
	sort.Strings(expectedExecutorLabels)

	if !reflect.DeepEqual(expectedExecutorLabels, executorOptions) {
		t.Errorf("Executor labels: wanted %+q got %+q", expectedExecutorLabels, executorOptions)
	}
}

func TestPopulateLabelsOverride_Driver_Executor(t *testing.T) {
	const (
		AppLabelKey              = "app-label-key"
		AppLabelValue            = "app-label-value"
		DriverLabelKey           = "driver-label-key"
		DriverLabelValue         = "driver-label-key"
		DriverAppLabelOverride   = "driver-app-label-override"
		ExecutorLabelKey         = "executor-label-key"
		ExecutorLabelValue       = "executor-label-key"
		ExecutorAppLabelOverride = "executor-app-label-override"
	)

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "spark-test",
			UID:    "spark-test-1",
			Labels: map[string]string{AppLabelKey: AppLabelValue},
		},
		Spec: v1beta2.SparkApplicationSpec{
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Labels: map[string]string{DriverLabelKey: DriverLabelValue, AppLabelKey: DriverAppLabelOverride},
				},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Labels: map[string]string{ExecutorLabelKey: ExecutorLabelValue, AppLabelKey: ExecutorAppLabelOverride},
				},
			},
		},
	}

	submissionID := uuid.New().String()
	driverOptions, err := addDriverConfOptions(app, submissionID)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(driverOptions)
	assert.Equal(t, 5, len(driverOptions))
	expectedDriverLabels := []string{
		fmt.Sprintf(SparkDriverLabelTemplate, AppLabelKey, DriverAppLabelOverride),
		fmt.Sprintf(SparkDriverLabelTemplate, DriverLabelKey, DriverLabelValue),
		fmt.Sprintf(SparkDriverLabelAnnotationTemplate, "app-name", "spark-test"),
		fmt.Sprintf(SparkDriverLabelAnnotationTemplate, "launched-by-spark-operator", strconv.FormatBool(true)),
		fmt.Sprintf(SparkDriverLabelAnnotationTemplate, "submission-id", submissionID),
	}
	sort.Strings(expectedDriverLabels)

	if !reflect.DeepEqual(expectedDriverLabels, driverOptions) {
		t.Errorf("Executor labels: wanted %+q got %+q", expectedDriverLabels, driverOptions)
	}

	executorOptions, err := addExecutorConfOptions(app, submissionID)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(executorOptions)
	assert.Equal(t, 5, len(executorOptions))
	expectedExecutorLabels := []string{
		fmt.Sprintf(SparkExecutorLabelTemplate, AppLabelKey, ExecutorAppLabelOverride),
		fmt.Sprintf(SparkExecutorLabelTemplate, ExecutorLabelKey, ExecutorLabelValue),
		fmt.Sprintf(SparkExecutorLabelAnnotationTemplate, "launched-by-spark-operator", strconv.FormatBool(true)),
		fmt.Sprintf(SparkExecutorLabelAnnotationTemplate, "app-name", "spark-test"),
		fmt.Sprintf(SparkExecutorLabelAnnotationTemplate, "submission-id", submissionID),
	}
	sort.Strings(expectedExecutorLabels)

	if !reflect.DeepEqual(expectedExecutorLabels, executorOptions) {
		t.Errorf("Executor labels: wanted %+q got %+q", expectedExecutorLabels, executorOptions)
	}
}

func TestDynamicAllocationOptions(t *testing.T) {
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{},
	}
	options := addDynamicAllocationConfOptions(app)
	assert.Equal(t, 0, len(options))

	app = &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			DynamicAllocation: &v1beta2.DynamicAllocation{
				Enabled:                true,
				InitialExecutors:       int32ptr(2),
				MinExecutors:           int32ptr(0),
				MaxExecutors:           int32ptr(10),
				ShuffleTrackingTimeout: int64ptr(6000000),
			},
		},
	}

	options = addDynamicAllocationConfOptions(app)
	assert.Equal(t, 6, len(options))
	assert.Equal(t, fmt.Sprintf("%s=true", config.SparkDynamicAllocationEnabled), options[0])
	assert.Equal(t, fmt.Sprintf("%s=true", config.SparkDynamicAllocationShuffleTrackingEnabled), options[1])
	assert.Equal(t, fmt.Sprintf("%s=2", config.SparkDynamicAllocationInitialExecutors), options[2])
	assert.Equal(t, fmt.Sprintf("%s=0", config.SparkDynamicAllocationMinExecutors), options[3])
	assert.Equal(t, fmt.Sprintf("%s=10", config.SparkDynamicAllocationMaxExecutors), options[4])
	assert.Equal(t, fmt.Sprintf("%s=6000000", config.SparkDynamicAllocationShuffleTrackingTimeout), options[5])
}

func TestProxyUserArg(t *testing.T) {
	const (
		host = "localhost"
		port = "6443"
	)

	if err := os.Setenv(kubernetesServiceHostEnvVar, host); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv(kubernetesServicePortEnvVar, port); err != nil {
		t.Fatal(err)
	}

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Mode:      v1beta2.ClusterMode,
			ProxyUser: stringptr("foo"),
		},
	}

	submissionID := uuid.New().String()
	driverPodName := getDriverPodName(app)
	args, err := buildSubmissionCommandArgs(app, driverPodName, submissionID)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "--master", args[0])
	assert.Equal(t, fmt.Sprintf("k8s://https://%s:%s", host, port), args[1])
	assert.Equal(t, "--deploy-mode", args[2])
	assert.Equal(t, string(v1beta2.ClusterMode), args[3])
	assert.Equal(t, "--proxy-user", args[4])
	assert.Equal(t, "foo", args[5])
}
