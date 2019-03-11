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
	"testing"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/stretchr/testify/assert"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta1"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
)

func TestPatchSparkPod_OwnerReference(t *testing.T) {
	app := &v1beta1.SparkApplication{
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
					Name:  sparkDriverContainerName,
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

func TestPatchSparkPod_Volumes(t *testing.T) {
	app := &v1beta1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta1.SparkApplicationSpec{
			Volumes: []corev1.Volume{
				corev1.Volume{
					Name: "spark",
					VolumeSource: corev1.VolumeSource{
						HostPath: &corev1.HostPathVolumeSource{
							Path: "/spark",
						},
					},
				},
			},
			Driver: v1beta1.DriverSpec{
				SparkPodSpec: v1beta1.SparkPodSpec{
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "spark",
							MountPath: "/mnt/spark",
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
					Name:  sparkDriverContainerName,
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
	assert.Equal(t, 1, len(modifiedPod.Spec.Containers[0].VolumeMounts))
	assert.Equal(t, app.Spec.Driver.VolumeMounts[0], modifiedPod.Spec.Containers[0].VolumeMounts[0])

	// Test patching a pod with existing OwnerReference and Volume.
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: "volume1"})
	pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name: "volume1",
	})

	modifiedPod, err = getModifiedPod(pod, app)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 2, len(modifiedPod.Spec.Volumes))
	assert.Equal(t, app.Spec.Volumes[0], modifiedPod.Spec.Volumes[1])
	assert.Equal(t, 2, len(modifiedPod.Spec.Containers[0].VolumeMounts))
	assert.Equal(t, app.Spec.Driver.VolumeMounts[0], modifiedPod.Spec.Containers[0].VolumeMounts[1])
}

func TestPatchSparkPod_Affinity(t *testing.T) {
	app := &v1beta1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta1.SparkApplicationSpec{
			Driver: v1beta1.DriverSpec{
				SparkPodSpec: v1beta1.SparkPodSpec{
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
					Name:  sparkDriverContainerName,
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
	app := &v1beta1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta1.SparkApplicationSpec{
			Driver: v1beta1.DriverSpec{
				SparkPodSpec: v1beta1.SparkPodSpec{
					ConfigMaps: []v1beta1.NamePath{{Name: "foo", Path: "/path/to/foo"}},
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
					Name:  sparkDriverContainerName,
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
	assert.Equal(t, "foo-vol", modifiedPod.Spec.Volumes[0].Name)
	assert.True(t, modifiedPod.Spec.Volumes[0].ConfigMap != nil)
	assert.Equal(t, 1, len(modifiedPod.Spec.Containers[0].VolumeMounts))
	assert.Equal(t, "/path/to/foo", modifiedPod.Spec.Containers[0].VolumeMounts[0].MountPath)
}

func TestPatchSparkPod_SparkConfigMap(t *testing.T) {
	sparkConfMapName := "spark-conf"
	app := &v1beta1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta1.SparkApplicationSpec{
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
					Name:  sparkDriverContainerName,
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
	app := &v1beta1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta1.SparkApplicationSpec{
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
					Name:  sparkDriverContainerName,
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

func TestPatchSparkPod_Tolerations(t *testing.T) {
	app := &v1beta1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta1.SparkApplicationSpec{
			Driver: v1beta1.DriverSpec{
				SparkPodSpec: v1beta1.SparkPodSpec{
					Tolerations: []corev1.Toleration{
						{
							Key:      "Key",
							Operator: "Equal",
							Value:    "Value",
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
					Name:  sparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	modifiedPod, err := getModifiedPod(pod, app)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 1, len(modifiedPod.Spec.Tolerations))
	assert.Equal(t, app.Spec.Driver.Tolerations[0], modifiedPod.Spec.Tolerations[0])
}

func TestPatchSparkPod_SecurityContext(t *testing.T) {
	var user int64 = 1000
	app := &v1beta1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-test",
			UID:  "spark-test-1",
		},
		Spec: v1beta1.SparkApplicationSpec{
			Driver: v1beta1.DriverSpec{
				SparkPodSpec: v1beta1.SparkPodSpec{
					SecurityContenxt: &corev1.PodSecurityContext{
						RunAsUser: &user,
					},
				},
			},
			Executor: v1beta1.ExecutorSpec{
				SparkPodSpec: v1beta1.SparkPodSpec{
					SecurityContenxt: &corev1.PodSecurityContext{
						RunAsUser: &user,
					},
				},
			},
		},
	}

	driverPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "spark-executor",
			Labels: map[string]string{
				config.SparkRoleLabel:               config.SparkDriverRole,
				config.LaunchedBySparkOperatorLabel: "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  sparkDriverContainerName,
					Image: "spark-driver:latest",
				},
			},
		},
	}

	modifiedDriverPod, err := getModifiedPod(driverPod, app)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, app.Spec.Executor.SecurityContenxt, modifiedDriverPod.Spec.SecurityContext)

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
					Name:  sparkExecutorContainerName,
					Image: "spark-executor:latest",
				},
			},
		},
	}

	modifiedExecutorPod, err := getModifiedPod(executorPod, app)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, app.Spec.Executor.SecurityContenxt, modifiedExecutorPod.Spec.SecurityContext)
}

func getModifiedPod(pod *corev1.Pod, app *v1beta1.SparkApplication) (*corev1.Pod, error) {
	patchOps := patchSparkPod(pod, app)
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
