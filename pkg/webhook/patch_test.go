/*
Copyright 2018 Google LLC

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

package webhook

import (
	"encoding/json"
	"fmt"
	"testing"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/stretchr/testify/assert"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
)

func TestPatchSparkPod_OwnerReference(t *testing.T) {
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-driver",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkDriverRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	// Test patching a pod without existing OwnerReference and Volume.
	modifiedPod, err := getModifiedPod(pod, app)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(modifiedPod.OwnerReferences))

	// Test patching a pod with existing OwnerReference and Volume.
	pod.OwnerReferences = append(pod.OwnerReferences, metav1.OwnerReference{Name: "owner-reference1"})

	modifiedPod, err = getModifiedPod(pod, app)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 2, len(modifiedPod.OwnerReferences))
}

func TestPatchSparkPod_Local_Volumes(t *testing.T) {
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Volumes: []corev1.Volume{
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
			},
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "spark-local-dir-1",
							MountPath: "/tmp/mnt-1",
						},
						{
							Name:      "spark-local-dir-2",
							MountPath: "/tmp/mnt-2",
						},
					},
				},
			},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-driver",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkDriverRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	modifiedPod, err := getModifiedPod(pod, app)
	if err != nil {
		t.Fatal(err)
	}

	// local volume will not be added by webhook
	assert.Equal(t, 0, len(modifiedPod.Spec.Volumes))
}

func TestPatchSparkPod_Volumes_Subpath(t *testing.T) {
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Volumes: []corev1.Volume{
				{
					Name: "spark-pvc",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: "claim-test",
						},
					},
				},
			},
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "spark-pvc",
							MountPath: "/mnt/spark",
							SubPath:   "/foo/test",
						},
						{
							Name:      "spark-pvc",
							MountPath: "/mnt/foo",
							SubPath:   "/bar/test",
						},
					},
				},
			},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-driver",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkDriverRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	// Test patching a pod without existing OwnerReference and Volume.
	modifiedPod, err := getModifiedPod(pod, app)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 1, len(modifiedPod.Spec.Volumes))
	assert.Equal(t, app.Spec.Volumes[0], modifiedPod.Spec.Volumes[0])
	assert.Equal(t, 2, len(modifiedPod.Spec.Containers[0].VolumeMounts))
	assert.Equal(t, app.Spec.Driver.VolumeMounts[0], modifiedPod.Spec.Containers[0].VolumeMounts[0])
	assert.Equal(t, app.Spec.Driver.VolumeMounts[1], modifiedPod.Spec.Containers[0].VolumeMounts[1])
}

func TestPatchSparkPod_Volumes(t *testing.T) {
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Volumes: []corev1.Volume{
				{
					Name: "spark",
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: "/spark",
						},
					},
				},
				{
					Name: "foo",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			},
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "spark",
							MountPath: "/mnt/spark",
						},
						{
							Name:      "foo",
							MountPath: "/mnt/foo",
						},
					},
				},
			},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-driver",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkDriverRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	// Test patching a pod without existing OwnerReference and Volume.
	modifiedPod, err := getModifiedPod(pod, app)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 2, len(modifiedPod.Spec.Volumes))
	assert.Equal(t, app.Spec.Volumes[0], modifiedPod.Spec.Volumes[0])
	assert.Equal(t, app.Spec.Volumes[1], modifiedPod.Spec.Volumes[1])
	assert.Equal(t, 2, len(modifiedPod.Spec.Containers[0].VolumeMounts))
	assert.Equal(t, app.Spec.Driver.VolumeMounts[0], modifiedPod.Spec.Containers[0].VolumeMounts[0])
	assert.Equal(t, app.Spec.Driver.VolumeMounts[1], modifiedPod.Spec.Containers[0].VolumeMounts[1])

	// Test patching a pod with existing OwnerReference and Volume.
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: "volume1"})
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name: "volume1",
	})

	modifiedPod, err = getModifiedPod(pod, app)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 3, len(modifiedPod.Spec.Volumes))
	assert.Equal(t, app.Spec.Volumes[0], modifiedPod.Spec.Volumes[1])
	assert.Equal(t, app.Spec.Volumes[1], modifiedPod.Spec.Volumes[2])
	assert.Equal(t, 3, len(modifiedPod.Spec.Containers[0].VolumeMounts))
	assert.Equal(t, app.Spec.Driver.VolumeMounts[0], modifiedPod.Spec.Containers[0].VolumeMounts[1])
	assert.Equal(t, app.Spec.Driver.VolumeMounts[1], modifiedPod.Spec.Containers[0].VolumeMounts[2])
}

func TestPatchSparkPod_Affinity(t *testing.T) {
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Affinity: &corev1.Affinity{
						PodAffinity: &corev1.PodAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchLabels: map[string]string{config.SparkRoleLabel: config.SparkDriverRole},
									},
									TopologyKey: "kubernetes.io/hostname",
								},
							},
						},
					},
				},
			},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-driver",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkDriverRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	// Test patching a pod with a pod Affinity.
	modifiedPod, err := getModifiedPod(pod, app)
	if err != nil {
		t.Fatal(err)
	}

	assert.True(t, modifiedPod.Spec.Affinity != nil)
	assert.Equal(t, 1,
		len(modifiedPod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution))
	assert.Equal(t, "kubernetes.io/hostname",
		modifiedPod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].TopologyKey)
}

func TestPatchSparkPod_ConfigMaps(t *testing.T) {
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					ConfigMaps: []v1beta2.NamePath{
						{Name: "foo", Path: "/path/to/foo"},
						{Name: "bar", Path: "/path/to/bar"},
					},
				},
			},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-driver",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkDriverRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	modifiedPod, err := getModifiedPod(pod, app)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 2, len(modifiedPod.Spec.Volumes))
	assert.Equal(t, "foo-vol", modifiedPod.Spec.Volumes[0].Name)
	assert.True(t, modifiedPod.Spec.Volumes[0].ConfigMap != nil)
	assert.Equal(t, "bar-vol", modifiedPod.Spec.Volumes[1].Name)
	assert.True(t, modifiedPod.Spec.Volumes[1].ConfigMap != nil)
	assert.Equal(t, 2, len(modifiedPod.Spec.Containers[0].VolumeMounts))
	assert.Equal(t, "/path/to/foo", modifiedPod.Spec.Containers[0].VolumeMounts[0].MountPath)
	assert.Equal(t, "/path/to/bar", modifiedPod.Spec.Containers[0].VolumeMounts[1].MountPath)
}

func TestPatchSparkPod_SparkConfigMap(t *testing.T) {
	sparkConfMapName := "spark-conf"
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			SparkConfigMap: &sparkConfMapName,
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-driver",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkDriverRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	modifiedPod, err := getModifiedPod(pod, app)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 1, len(modifiedPod.Spec.Volumes))
	assert.Equal(t, config.SparkConfigMapVolumeName, modifiedPod.Spec.Volumes[0].Name)
	assert.True(t, modifiedPod.Spec.Volumes[0].ConfigMap != nil)
	assert.Equal(t, 1, len(modifiedPod.Spec.Containers[0].VolumeMounts))
	assert.Equal(t, config.DefaultSparkConfDir, modifiedPod.Spec.Containers[0].VolumeMounts[0].MountPath)
	assert.Equal(t, 1, len(modifiedPod.Spec.Containers[0].Env))
	assert.Equal(t, config.DefaultSparkConfDir, modifiedPod.Spec.Containers[0].Env[0].Value)
}

func TestPatchSparkPod_HadoopConfigMap(t *testing.T) {
	hadoopConfMapName := "hadoop-conf"
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			HadoopConfigMap: &hadoopConfMapName,
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-driver",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkDriverRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	modifiedPod, err := getModifiedPod(pod, app)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 1, len(modifiedPod.Spec.Volumes))
	assert.Equal(t, config.HadoopConfigMapVolumeName, modifiedPod.Spec.Volumes[0].Name)
	assert.True(t, modifiedPod.Spec.Volumes[0].ConfigMap != nil)
	assert.Equal(t, 1, len(modifiedPod.Spec.Containers[0].VolumeMounts))
	assert.Equal(t, config.DefaultHadoopConfDir, modifiedPod.Spec.Containers[0].VolumeMounts[0].MountPath)
	assert.Equal(t, 1, len(modifiedPod.Spec.Containers[0].Env))
	assert.Equal(t, config.DefaultHadoopConfDir, modifiedPod.Spec.Containers[0].Env[0].Value)
}

func TestPatchSparkPod_PrometheusConfigMaps(t *testing.T) {
	var appPort int32 = 9999
	appPortName := "jmx-exporter"
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Monitoring: &v1beta2.MonitoringSpec{
				Prometheus: &v1beta2.PrometheusSpec{
					JmxExporterJar: "",
					Port:           &appPort,
					PortName:       &appPortName,
					ConfigFile:     nil,
					Configuration:  nil,
				},
				ExposeDriverMetrics: true,
			},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-driver",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkDriverRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	modifiedPod, err := getModifiedPod(pod, app)
	if err != nil {
		t.Fatal(err)
	}

	expectedConfigMapName := config.GetPrometheusConfigMapName(app)
	expectedVolumeName := expectedConfigMapName + "-vol"
	expectedContainerPort := *app.Spec.Monitoring.Prometheus.Port
	expectedContainerPortName := *app.Spec.Monitoring.Prometheus.PortName
	assert.Equal(t, 1, len(modifiedPod.Spec.Volumes))
	assert.Equal(t, expectedVolumeName, modifiedPod.Spec.Volumes[0].Name)
	assert.True(t, modifiedPod.Spec.Volumes[0].ConfigMap != nil)
	assert.Equal(t, expectedConfigMapName, modifiedPod.Spec.Volumes[0].ConfigMap.Name)
	assert.Equal(t, 1, len(modifiedPod.Spec.Containers[0].VolumeMounts))
	assert.Equal(t, expectedVolumeName, modifiedPod.Spec.Containers[0].VolumeMounts[0].Name)
	assert.Equal(t, config.PrometheusConfigMapMountPath, modifiedPod.Spec.Containers[0].VolumeMounts[0].MountPath)
	assert.Equal(t, expectedContainerPort, modifiedPod.Spec.Containers[0].Ports[0].ContainerPort)
	assert.Equal(t, expectedContainerPortName, modifiedPod.Spec.Containers[0].Ports[0].Name)
	assert.Equal(t, corev1.Protocol(config.DefaultPrometheusPortProtocol), modifiedPod.Spec.Containers[0].Ports[0].Protocol)
}

func TestPatchSparkPod_Tolerations(t *testing.T) {
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Tolerations: []corev1.Toleration{
						{
							Key:      "Key1",
							Operator: "Equal",
							Value:    "Value1",
							Effect:   "NoEffect",
						},
						{
							Key:      "Key2",
							Operator: "Equal",
							Value:    "Value2",
							Effect:   "NoEffect",
						},
					},
				},
			},
		},
	}

	// Test patching a pod with a Toleration.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-driver",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkDriverRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	modifiedPod, err := getModifiedPod(pod, app)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 2, len(modifiedPod.Spec.Tolerations))
	assert.Equal(t, app.Spec.Driver.Tolerations[0], modifiedPod.Spec.Tolerations[0])
	assert.Equal(t, app.Spec.Driver.Tolerations[1], modifiedPod.Spec.Tolerations[1])
}

func TestPatchSparkPod_SecurityContext(t *testing.T) {
	var user int64 = 1000
	var user2 int64 = 2000
	var AllowPrivilegeEscalation = false

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					PodSecurityContext: &corev1.PodSecurityContext{
						RunAsUser: &user,
					},
					SecurityContext: &corev1.SecurityContext{
						AllowPrivilegeEscalation: &AllowPrivilegeEscalation,
						RunAsUser:                &user2,
					},
				},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					PodSecurityContext: &corev1.PodSecurityContext{
						RunAsUser: &user,
					},
					SecurityContext: &corev1.SecurityContext{
						AllowPrivilegeEscalation: &AllowPrivilegeEscalation,
						RunAsUser:                &user2,
					},
				},
			},
		},
	}

	driverPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-driver",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkDriverRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	modifiedDriverPod, err := getModifiedPod(driverPod, app)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, app.Spec.Driver.PodSecurityContext, modifiedDriverPod.Spec.SecurityContext)
	assert.Equal(t, app.Spec.Driver.SecurityContext, modifiedDriverPod.Spec.Containers[0].SecurityContext)

	executorPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-executor",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkExecutorRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkExecutorContainerName,
					Image: "spark-executor:latest",
				},
			},
		},
	}

	modifiedExecutorPod, err := getModifiedPod(executorPod, app)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, app.Spec.Executor.PodSecurityContext, modifiedExecutorPod.Spec.SecurityContext)
	assert.Equal(t, app.Spec.Executor.SecurityContext, modifiedExecutorPod.Spec.Containers[0].SecurityContext)
}

func TestPatchSparkPod_SchedulerName(t *testing.T) {
	var schedulerName = "another_scheduler"
	var defaultScheduler = "default-scheduler"

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test-patch-schedulername",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					SchedulerName: &schedulerName,
				},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{},
			},
		},
	}

	driverPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-driver",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkDriverRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			SchedulerName: defaultScheduler,
			Containers: []corev1.Container{
				{
					Name:  config.SparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	modifiedDriverPod, err := getModifiedPod(driverPod, app)
	if err != nil {
		t.Fatal(err)
	}
	//Driver scheduler name should be updated when specified.
	assert.Equal(t, schedulerName, modifiedDriverPod.Spec.SchedulerName)

	executorPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-executor",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkExecutorRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			SchedulerName: defaultScheduler,
			Containers: []corev1.Container{
				{
					Name:  config.SparkExecutorContainerName,
					Image: "spark-executor:latest",
				},
			},
		},
	}

	modifiedExecutorPod, err := getModifiedPod(executorPod, app)
	if err != nil {
		t.Fatal(err)
	}
	//Executor scheduler name should remain the same as before when not specified in SparkApplicationSpec
	assert.Equal(t, defaultScheduler, modifiedExecutorPod.Spec.SchedulerName)
}

func TestPatchSparkPod_PriorityClassName(t *testing.T) {
	var priorityClassName = "critical"

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test-patch-priorityclassname",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			BatchSchedulerOptions: &v1beta2.BatchSchedulerConfiguration{
				PriorityClassName: &priorityClassName,
			},
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{},
			},
		},
	}

	driverPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-driver",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkDriverRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	modifiedDriverPod, err := getModifiedPod(driverPod, app)
	if err != nil {
		t.Fatal(err)
	}
	//Driver priorityClassName should be populated when specified
	assert.Equal(t, priorityClassName, modifiedDriverPod.Spec.PriorityClassName)

	var defaultPriority int32 = 0
	executorPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-executor",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkExecutorRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkExecutorContainerName,
					Image: "spark-executor:latest",
				},
			},
			Priority: &defaultPriority,
		},
	}

	modifiedExecutorPod, err := getModifiedPod(executorPod, app)
	if err != nil {
		t.Fatal(err)
	}
	//Executor priorityClassName should also be populated when specified in SparkApplicationSpec
	assert.Equal(t, priorityClassName, modifiedExecutorPod.Spec.PriorityClassName)
	assert.Nil(t, modifiedExecutorPod.Spec.Priority)
}

func TestPatchSparkPod_Sidecars(t *testing.T) {
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Sidecars: []corev1.Container{
						{
							Name:  "sidecar1",
							Image: "sidecar1:latest",
						},
						{
							Name:  "sidecar2",
							Image: "sidecar2:latest",
						},
					},
				},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Sidecars: []corev1.Container{
						{
							Name:  "sidecar1",
							Image: "sidecar1:latest",
						},
						{
							Name:  "sidecar2",
							Image: "sidecar2:latest",
						},
					},
				},
			},
		},
	}

	driverPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-driver",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkDriverRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	modifiedDriverPod, err := getModifiedPod(driverPod, app)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 3, len(modifiedDriverPod.Spec.Containers))
	assert.Equal(t, "sidecar1", modifiedDriverPod.Spec.Containers[1].Name)
	assert.Equal(t, "sidecar2", modifiedDriverPod.Spec.Containers[2].Name)

	executorPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-executor",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkExecutorRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkExecutorContainerName,
					Image: "spark-executor:latest",
				},
			},
		},
	}

	modifiedExecutorPod, err := getModifiedPod(executorPod, app)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 3, len(modifiedExecutorPod.Spec.Containers))
	assert.Equal(t, "sidecar1", modifiedExecutorPod.Spec.Containers[1].Name)
	assert.Equal(t, "sidecar2", modifiedExecutorPod.Spec.Containers[2].Name)
}

func TestPatchSparkPod_InitContainers(t *testing.T) {
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					InitContainers: []corev1.Container{
						{
							Name:  "init-container1",
							Image: "init-container1:latest",
						},
						{
							Name:  "init-container2",
							Image: "init-container2:latest",
						},
					},
				},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					InitContainers: []corev1.Container{
						{
							Name:  "init-container1",
							Image: "init-container1:latest",
						},
						{
							Name:  "init-container2",
							Image: "init-container2:latest",
						},
					},
				},
			},
		},
	}

	driverPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-driver",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkDriverRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	modifiedDriverPod, err := getModifiedPod(driverPod, app)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 2, len(modifiedDriverPod.Spec.InitContainers))
	assert.Equal(t, "init-container1", modifiedDriverPod.Spec.InitContainers[0].Name)
	assert.Equal(t, "init-container2", modifiedDriverPod.Spec.InitContainers[1].Name)

	executorPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-executor",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkExecutorRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkExecutorContainerName,
					Image: "spark-executor:latest",
				},
			},
		},
	}

	modifiedExecutorPod, err := getModifiedPod(executorPod, app)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 2, len(modifiedExecutorPod.Spec.InitContainers))
	assert.Equal(t, "init-container1", modifiedExecutorPod.Spec.InitContainers[0].Name)
	assert.Equal(t, "init-container2", modifiedExecutorPod.Spec.InitContainers[1].Name)
}

func TestPatchSparkPod_DNSConfig(t *testing.T) {
	aVal := "5"
	sampleDNSConfig := &corev1.PodDNSConfig{
		Nameservers: []string{"8.8.8.8", "4.4.4.4"},
		Searches:    []string{"svc.cluster.local", "cluster.local"},
		Options: []corev1.PodDNSConfigOption{
			{Name: "ndots", Value: &aVal},
		},
	}

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{DNSConfig: sampleDNSConfig},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{DNSConfig: sampleDNSConfig},
			},
		},
	}

	driverPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-driver",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkDriverRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	modifiedDriverPod, err := getModifiedPod(driverPod, app)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotNil(t, modifiedDriverPod.Spec.DNSConfig)
	assert.Equal(t, sampleDNSConfig, modifiedDriverPod.Spec.DNSConfig)

	executorPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-executor",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkExecutorRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkExecutorContainerName,
					Image: "spark-executor:latest",
				},
			},
		},
	}

	modifiedExecutorPod, err := getModifiedPod(executorPod, app)
	if err != nil {
		t.Fatal(err)
	}

	assert.NotNil(t, modifiedExecutorPod.Spec.DNSConfig)
	assert.Equal(t, sampleDNSConfig, modifiedExecutorPod.Spec.DNSConfig)

}

func TestPatchSparkPod_NodeSector(t *testing.T) {
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					NodeSelector: map[string]string{"disk": "ssd", "secondkey": "secondvalue"},
				},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					NodeSelector: map[string]string{"nodeType": "gpu", "secondkey": "secondvalue"},
				},
			},
		},
	}

	driverPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-driver",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkDriverRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	modifiedDriverPod, err := getModifiedPod(driverPod, app)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 2, len(modifiedDriverPod.Spec.NodeSelector))
	assert.Equal(t, "ssd", modifiedDriverPod.Spec.NodeSelector["disk"])
	assert.Equal(t, "secondvalue", modifiedDriverPod.Spec.NodeSelector["secondkey"])

	executorPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-executor",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkExecutorRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkExecutorContainerName,
					Image: "spark-executor:latest",
				},
			},
		},
	}

	modifiedExecutorPod, err := getModifiedPod(executorPod, app)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 2, len(modifiedExecutorPod.Spec.NodeSelector))
	assert.Equal(t, "gpu", modifiedExecutorPod.Spec.NodeSelector["nodeType"])
	assert.Equal(t, "secondvalue", modifiedExecutorPod.Spec.NodeSelector["secondkey"])
}

func TestPatchSparkPod_GPU(t *testing.T) {
	cpuLimit := int64(10)
	cpuRequest := int64(5)

	type testcase struct {
		gpuSpec     *v1beta2.GPUSpec
		cpuLimits   *int64
		cpuRequests *int64
	}

	getResourceRequirements := func(test testcase) *corev1.ResourceRequirements {
		if test.cpuLimits == nil && test.cpuRequests == nil {
			return nil
		}
		ret := corev1.ResourceRequirements{}
		if test.cpuLimits != nil {
			ret.Limits = corev1.ResourceList{}
			ret.Limits[corev1.ResourceCPU] = *resource.NewQuantity(*test.cpuLimits, resource.DecimalSI)
		}
		if test.cpuRequests != nil {
			ret.Requests = corev1.ResourceList{}
			ret.Requests[corev1.ResourceCPU] = *resource.NewQuantity(*test.cpuRequests, resource.DecimalSI)
		}
		return &ret
	}

	assertFn := func(t *testing.T, modifiedPod *corev1.Pod, test testcase) {
		// check GPU Limits
		if test.gpuSpec != nil && test.gpuSpec.Name != "" && test.gpuSpec.Quantity > 0 {
			quantity := modifiedPod.Spec.Containers[0].Resources.Limits[corev1.ResourceName(test.gpuSpec.Name)]
			count, succeed := (&quantity).AsInt64()
			if succeed != true {
				t.Fatal(fmt.Errorf("value cannot be represented in an int64 OR would result in a loss of precision"))
			}
			assert.Equal(t, test.gpuSpec.Quantity, count)
		}

		// check CPU Requests
		if test.cpuRequests != nil {
			quantity := modifiedPod.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU]
			count, succeed := (&quantity).AsInt64()
			if succeed != true {
				t.Fatal(fmt.Errorf("value cannot be represented in an int64 OR would result in a loss of precision"))
			}
			assert.Equal(t, *test.cpuRequests, count)
		}

		// check CPU Limits
		if test.cpuLimits != nil {
			quantity := modifiedPod.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU]
			count, succeed := (&quantity).AsInt64()
			if succeed != true {
				t.Fatal(fmt.Errorf("value cannot be represented in an int64 OR would result in a loss of precision"))
			}
			assert.Equal(t, *test.cpuLimits, count)
		}
	}
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{},
			},
		},
	}
	tests := []testcase{
		{
			nil,
			nil,
			nil,
		},
		{
			nil,
			&cpuLimit,
			nil,
		},
		{
			nil,
			nil,
			&cpuRequest,
		},
		{
			nil,
			&cpuLimit,
			&cpuRequest,
		},
		{
			&v1beta2.GPUSpec{},
			nil,
			nil,
		},
		{
			&v1beta2.GPUSpec{},
			&cpuLimit,
			nil,
		},
		{
			&v1beta2.GPUSpec{},
			nil,
			&cpuRequest,
		},
		{
			&v1beta2.GPUSpec{},
			&cpuLimit,
			&cpuRequest,
		},
		{
			&v1beta2.GPUSpec{Name: "example.com/gpu", Quantity: 1},
			nil,
			nil,
		},
		{
			&v1beta2.GPUSpec{Name: "example.com/gpu", Quantity: 1},
			&cpuLimit,
			nil,
		},
		{
			&v1beta2.GPUSpec{Name: "example.com/gpu", Quantity: 1},
			nil,
			&cpuRequest,
		},
		{
			&v1beta2.GPUSpec{Name: "example.com/gpu", Quantity: 1},
			&cpuLimit,
			&cpuRequest,
		},
	}

	for _, test := range tests {
		app.Spec.Driver.GPU = test.gpuSpec
		app.Spec.Executor.GPU = test.gpuSpec
		driverPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "spark-driver",
				Labels: map[string]string{
					config.SparkRoleLabel:               config.SparkDriverRole,
					config.LaunchedBySparkOperatorLabel: "true",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  config.SparkDriverContainerName,
						Image: "spark-driver:latest",
					},
				},
			},
		}

		if getResourceRequirements(test) != nil {
			driverPod.Spec.Containers[0].Resources = *getResourceRequirements(test)
		}
		modifiedDriverPod, err := getModifiedPod(driverPod, app)
		if err != nil {
			t.Fatal(err)
		}

		assertFn(t, modifiedDriverPod, test)
		executorPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "spark-executor",
				Labels: map[string]string{
					config.SparkRoleLabel:               config.SparkExecutorRole,
					config.LaunchedBySparkOperatorLabel: "true",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  config.SparkExecutorContainerName,
						Image: "spark-executor:latest",
					},
				},
			},
		}
		if getResourceRequirements(test) != nil {
			executorPod.Spec.Containers[0].Resources = *getResourceRequirements(test)
		}
		modifiedExecutorPod, err := getModifiedPod(executorPod, app)
		if err != nil {
			t.Fatal(err)
		}
		assertFn(t, modifiedExecutorPod, test)
	}
}

func TestPatchSparkPod_HostNetwork(t *testing.T) {
	var hostNetwork = true
	var defaultNetwork = false

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test-hostNetwork",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{},
			},
		},
	}

	tests := []*bool{
		nil,
		&defaultNetwork,
		&hostNetwork,
	}

	for _, test := range tests {
		app.Spec.Driver.HostNetwork = test
		app.Spec.Executor.HostNetwork = test
		driverPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "spark-driver",
				Labels: map[string]string{
					config.SparkRoleLabel:               config.SparkDriverRole,
					config.LaunchedBySparkOperatorLabel: "true",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  config.SparkDriverContainerName,
						Image: "spark-driver:latest",
					},
				},
			},
		}

		modifiedDriverPod, err := getModifiedPod(driverPod, app)
		if err != nil {
			t.Fatal(err)
		}
		if test == nil || *test == false {
			assert.Equal(t, false, modifiedDriverPod.Spec.HostNetwork)
		} else {
			assert.Equal(t, true, modifiedDriverPod.Spec.HostNetwork)
			assert.Equal(t, corev1.DNSClusterFirstWithHostNet, modifiedDriverPod.Spec.DNSPolicy)
		}
		executorPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "spark-executor",
				Labels: map[string]string{
					config.SparkRoleLabel:               config.SparkExecutorRole,
					config.LaunchedBySparkOperatorLabel: "true",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  config.SparkExecutorContainerName,
						Image: "spark-executor:latest",
					},
				},
			},
		}

		modifiedExecutorPod, err := getModifiedPod(executorPod, app)
		if err != nil {
			t.Fatal(err)
		}
		if test == nil || *test == false {
			assert.Equal(t, false, modifiedExecutorPod.Spec.HostNetwork)
		} else {
			assert.Equal(t, true, modifiedExecutorPod.Spec.HostNetwork)
			assert.Equal(t, corev1.DNSClusterFirstWithHostNet, modifiedExecutorPod.Spec.DNSPolicy)
		}
	}
}

func TestPatchSparkPod_Env(t *testing.T) {
	drvEnvKey := "TEST_DRV_ENV_VAR_KEY"
	drvEnvVal := "test_drv_env_var_val"
	exeEnvKey := "TEST_EXE_ENV_VAR_KEY"
	exeEnvVal := "test_exe_env_var_val"

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Env: []corev1.EnvVar{
						{
							Name:  exeEnvKey,
							Value: exeEnvVal,
						},
					},
				},
			},
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Env: []corev1.EnvVar{
						{
							Name:  drvEnvKey,
							Value: drvEnvVal,
						},
					},
				},
			},
		},
	}

	executorPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-executor",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkExecutorRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkExecutorContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	driverPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-driver",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkDriverRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	modifiedExecutorPod, err := getModifiedPod(executorPod, app)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 1, len(modifiedExecutorPod.Spec.Containers[0].Env))
	assert.Equal(t, exeEnvKey, modifiedExecutorPod.Spec.Containers[0].Env[0].Name)
	assert.Equal(t, exeEnvVal, modifiedExecutorPod.Spec.Containers[0].Env[0].Value)
	assert.True(t, modifiedExecutorPod.Spec.Containers[0].Env[0].ValueFrom == nil)

	modifiedDriverPod, err := getModifiedPod(driverPod, app)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 1, len(modifiedDriverPod.Spec.Containers[0].Env))
	assert.Equal(t, drvEnvKey, modifiedDriverPod.Spec.Containers[0].Env[0].Name)
	assert.Equal(t, drvEnvVal, modifiedDriverPod.Spec.Containers[0].Env[0].Value)
	assert.True(t, modifiedDriverPod.Spec.Containers[0].Env[0].ValueFrom == nil)
}

func TestPatchSparkPod_EnvFrom(t *testing.T) {
	configMapName := "test-config-map"
	secretName := "test-secret"

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					EnvFrom: []corev1.EnvFromSource{
						{
							ConfigMapRef: &corev1.ConfigMapEnvSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: configMapName,
								},
							},
						},
						{
							SecretRef: &corev1.SecretEnvSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: secretName,
								},
							},
						},
					},
				},
			},
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					EnvFrom: []corev1.EnvFromSource{
						{
							ConfigMapRef: &corev1.ConfigMapEnvSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: configMapName,
								},
							},
						},
						{
							SecretRef: &corev1.SecretEnvSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: secretName,
								},
							},
						},
					},
				},
			},
		},
	}

	driverPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-driver",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkDriverRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	executorPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-executor",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkExecutorRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkExecutorContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	modifiedDriverPod, err := getModifiedPod(driverPod, app)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 2, len(modifiedDriverPod.Spec.Containers[0].EnvFrom))
	assert.Equal(t, configMapName, modifiedDriverPod.Spec.Containers[0].EnvFrom[0].ConfigMapRef.Name)
	assert.Equal(t, secretName, modifiedDriverPod.Spec.Containers[0].EnvFrom[1].SecretRef.Name)

	modifiedExecutorPod, err := getModifiedPod(executorPod, app)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 2, len(modifiedExecutorPod.Spec.Containers[0].EnvFrom))
	assert.Equal(t, configMapName, modifiedExecutorPod.Spec.Containers[0].EnvFrom[0].ConfigMapRef.Name)
	assert.Equal(t, secretName, modifiedExecutorPod.Spec.Containers[0].EnvFrom[1].SecretRef.Name)
}

func TestPatchSparkPod_GracePeriodSeconds(t *testing.T) {

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test-hostNetwork",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{},
			},
		},
	}

	test1 := int64(60)
	tests := []*int64{
		&test1,
		nil,
	}

	for _, test := range tests {
		app.Spec.Driver.TerminationGracePeriodSeconds = test
		app.Spec.Executor.TerminationGracePeriodSeconds = test
		driverPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "spark-driver",
				Labels: map[string]string{
					config.SparkRoleLabel:               config.SparkDriverRole,
					config.LaunchedBySparkOperatorLabel: "true",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  config.SparkDriverContainerName,
						Image: "spark-driver:latest",
					},
				},
			},
		}

		modifiedDriverPod, err := getModifiedPod(driverPod, app)
		if err != nil {
			t.Fatal(err)
		}
		if test == nil {
			assert.True(t, modifiedDriverPod.Spec.TerminationGracePeriodSeconds == nil)
		} else {
			assert.Equal(t, int64(60), *modifiedDriverPod.Spec.TerminationGracePeriodSeconds)
		}

		executorPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "spark-executor",
				Labels: map[string]string{
					config.SparkRoleLabel:               config.SparkExecutorRole,
					config.LaunchedBySparkOperatorLabel: "true",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  config.SparkExecutorContainerName,
						Image: "spark-executor:latest",
					},
				},
			},
		}

		modifiedExecPod, err := getModifiedPod(executorPod, app)
		if err != nil {
			t.Fatal(err)
		}
		if test == nil {
			assert.True(t, modifiedDriverPod.Spec.TerminationGracePeriodSeconds == nil)
		} else {
			assert.Equal(t, int64(60), *modifiedExecPod.Spec.TerminationGracePeriodSeconds)
		}
	}
}

func TestPatchSparkPod_Lifecycle(t *testing.T) {
	preStopTest := &corev1.ExecAction{
		Command: []string{"/bin/sh", "-c", "echo Hello from the pre stop handler > /usr/share/message"},
	}
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Driver: v1beta2.DriverSpec{
				Lifecycle: &corev1.Lifecycle{
					PreStop: &corev1.Handler{Exec: preStopTest},
				},
			},
		},
	}

	driverPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-driver",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkDriverRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	modifiedDriverPod, err := getModifiedPod(driverPod, app)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, preStopTest, modifiedDriverPod.Spec.Containers[0].Lifecycle.PreStop.Exec)
}

func getModifiedPod(pod *corev1.Pod, app *v1beta2.SparkApplication) (*corev1.Pod, error) {
	patchOps := patchSparkPod(pod.DeepCopy(), app)
	patchBytes, err := json.Marshal(patchOps)
	if err != nil {
		return nil, err
	}
	patch, err := jsonpatch.DecodePatch(patchBytes)
	if err != nil {
		return nil, err
	}

	original, err := json.Marshal(pod)
	if err != nil {
		return nil, err
	}
	modified, err := patch.Apply(original)
	if err != nil {
		return nil, err
	}
	modifiedPod := &corev1.Pod{}
	if err := json.Unmarshal(modified, modifiedPod); err != nil {
		return nil, err
	}

	return modifiedPod, nil
}

func TestPatchSparkPod_HostAliases(t *testing.T) {
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					HostAliases: []corev1.HostAlias{
						{
							IP:        "127.0.0.1",
							Hostnames: []string{"localhost"},
						},
						{
							IP:        "192.168.0.1",
							Hostnames: []string{"test.com", "test2.com"},
						},
						{
							IP:        "192.168.0.2",
							Hostnames: []string{"test3.com"},
						},
						{
							IP:        "192.168.0.3",
							Hostnames: []string{"test4.com"},
						},
					},
				},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					HostAliases: []corev1.HostAlias{
						{
							IP:        "127.0.0.1",
							Hostnames: []string{"localhost"},
						},
						{
							IP:        "192.168.0.1",
							Hostnames: []string{"test.com", "test2.com"},
						},
					},
				},
			},
		},
	}

	driverPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-driver",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkDriverRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	modifiedDriverPod, err := getModifiedPod(driverPod, app)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 4, len(modifiedDriverPod.Spec.HostAliases))
	assert.Equal(t, "127.0.0.1", modifiedDriverPod.Spec.HostAliases[0].IP)
	assert.Equal(t, "192.168.0.1", modifiedDriverPod.Spec.HostAliases[1].IP)
	assert.Equal(t, "192.168.0.2", modifiedDriverPod.Spec.HostAliases[2].IP)
	assert.Equal(t, "192.168.0.3", modifiedDriverPod.Spec.HostAliases[3].IP)

	executorPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-executor",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkExecutorRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkExecutorContainerName,
					Image: "spark-executor:latest",
				},
			},
		},
	}

	modifiedExecutorPod, err := getModifiedPod(executorPod, app)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 2, len(modifiedExecutorPod.Spec.HostAliases))
	assert.Equal(t, "127.0.0.1", modifiedExecutorPod.Spec.HostAliases[0].IP)
	assert.Equal(t, "192.168.0.1", modifiedExecutorPod.Spec.HostAliases[1].IP)
}

func TestPatchSparkPod_Ports(t *testing.T) {
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Driver: v1beta2.DriverSpec{
				Ports: []v1beta2.Port{
					{Name: "driverPort1", ContainerPort: 8080, Protocol: "TCP"},
					{Name: "driverPort2", ContainerPort: 8081, Protocol: "TCP"},
				},
			},
			Executor: v1beta2.ExecutorSpec{
				Ports: []v1beta2.Port{
					{Name: "executorPort1", ContainerPort: 8082, Protocol: "TCP"},
					{Name: "executorPort2", ContainerPort: 8083, Protocol: "TCP"},
				},
			},
		},
	}

	driverPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-driver",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkDriverRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	modifiedDriverPod, err := getModifiedPod(driverPod, app)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 2, len(modifiedDriverPod.Spec.Containers[0].Ports))
	assert.Equal(t, "driverPort1", modifiedDriverPod.Spec.Containers[0].Ports[0].Name)
	assert.Equal(t, "driverPort2", modifiedDriverPod.Spec.Containers[0].Ports[1].Name)
	assert.Equal(t, int32(8080), modifiedDriverPod.Spec.Containers[0].Ports[0].ContainerPort)
	assert.Equal(t, int32(8081), modifiedDriverPod.Spec.Containers[0].Ports[1].ContainerPort)

	executorPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-executor",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkExecutorRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  config.SparkExecutorContainerName,
					Image: "spark-executor:latest",
				},
			},
		},
	}

	modifiedExecutorPod, err := getModifiedPod(executorPod, app)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 2, len(modifiedExecutorPod.Spec.Containers[0].Ports))
	assert.Equal(t, "executorPort1", modifiedExecutorPod.Spec.Containers[0].Ports[0].Name)
	assert.Equal(t, "executorPort2", modifiedExecutorPod.Spec.Containers[0].Ports[1].Name)
	assert.Equal(t, int32(8082), modifiedExecutorPod.Spec.Containers[0].Ports[0].ContainerPort)
	assert.Equal(t, int32(8083), modifiedExecutorPod.Spec.Containers[0].Ports[1].ContainerPort)
}
