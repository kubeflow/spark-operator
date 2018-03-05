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

package config

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	apiv1 "k8s.io/api/core/v1"

	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	"k8s.io/spark-on-k8s-operator/pkg/util"
)

func TestFindVolumes(t *testing.T) {
	volume1 := &apiv1.Volume{
		Name: "volume1",
		VolumeSource: apiv1.VolumeSource{
			HostPath: &apiv1.HostPathVolumeSource{
				Path: "/etc/spark/data",
			},
		},
	}
	volume2 := &apiv1.Volume{
		Name: "volume2",
		VolumeSource: apiv1.VolumeSource{
			EmptyDir: &apiv1.EmptyDirVolumeSource{},
		},
	}

	annotations := make(map[string]string)
	volume1Str, err := util.MarshalVolume(volume1)
	if err != nil {
		t.Fatal(err)
	}
	annotations[fmt.Sprintf("%s%s", VolumesAnnotationPrefix, volume1.Name)] = volume1Str
	volume2Str, err := util.MarshalVolume(volume2)
	if err != nil {
		t.Fatal(err)
	}
	annotations[fmt.Sprintf("%s%s", VolumesAnnotationPrefix, volume2.Name)] = volume2Str

	volumes, err := FindVolumes(annotations)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 2, len(volumes))
	assert.Equal(t, "/etc/spark/data", volumes["volume1"].HostPath.Path)
	assert.NotNil(t, volumes["volume2"].EmptyDir)
}

func TestFindVolumeMounts(t *testing.T) {
	volumeMount1 := &apiv1.VolumeMount{
		Name:      "volume1",
		MountPath: "/etc/spark/data",
	}
	volumeMount2 := &apiv1.VolumeMount{
		Name:      "volume2",
		MountPath: "/etc/spark/work",
	}

	pod := &apiv1.Pod{}
	pod.Annotations = make(map[string]string)

	mount1Str, err := util.MarshalVolumeMount(volumeMount1)
	if err != nil {
		t.Fatal(err)
	}
	pod.Annotations[fmt.Sprintf("%s%s", VolumeMountsAnnotationPrefix, volumeMount1.Name)] = mount1Str
	mount2Str, err := util.MarshalVolumeMount(volumeMount2)
	if err != nil {
		t.Fatal(err)
	}
	pod.Annotations[fmt.Sprintf("%s%s", VolumeMountsAnnotationPrefix, volumeMount2.Name)] = mount2Str

	mounts, err := FindVolumeMounts(pod.Annotations)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 2, len(mounts))
	assert.Equal(t, "/etc/spark/data", mounts["volume1"].MountPath)
	assert.Equal(t, "/etc/spark/work", mounts["volume2"].MountPath)
}

func TestGetVolumeAnnotationsForMount(t *testing.T) {
	volume1 := apiv1.Volume{
		Name: "volume1",
		VolumeSource: apiv1.VolumeSource{
			HostPath: &apiv1.HostPathVolumeSource{
				Path: "/etc/spark/data",
			},
		},
	}
	volume2 := apiv1.Volume{
		Name: "volume2",
		VolumeSource: apiv1.VolumeSource{
			EmptyDir: &apiv1.EmptyDirVolumeSource{},
		},
	}

	volumeMount1 := apiv1.VolumeMount{
		Name:      volume1.Name,
		MountPath: "/etc/spark/data",
	}
	volumeMount2 := apiv1.VolumeMount{
		Name:      volume2.Name,
		MountPath: "/etc/spark/work",
	}

	annotations, err := getVolumeAnnotationsForMounts([]apiv1.Volume{volume1, volume2}, []apiv1.VolumeMount{volumeMount1})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(annotations))
	value, ok := annotations[fmt.Sprintf("%s%s", VolumesAnnotationPrefix, volume1.Name)]
	assert.True(t, ok)
	volume, err := util.UnmarshalVolume(value)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, volume1.HostPath.Path, volume.HostPath.Path)

	annotations, err = getVolumeAnnotationsForMounts([]apiv1.Volume{volume1, volume2}, []apiv1.VolumeMount{volumeMount2})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(annotations))
	value, ok = annotations[fmt.Sprintf("%s%s", VolumesAnnotationPrefix, volume2.Name)]
	assert.True(t, ok)
	volume, err = util.UnmarshalVolume(value)
	if err != nil {
		t.Fatal(err)
	}
	assert.NotNil(t, volume.EmptyDir)
}

func TestGetDriverVolumeMountConfOptions(t *testing.T) {
	app := &v1alpha1.SparkApplication{
		Spec: v1alpha1.SparkApplicationSpec{
			Volumes: []apiv1.Volume{
				{
					Name: "volume1",
					VolumeSource: apiv1.VolumeSource{
						HostPath: &apiv1.HostPathVolumeSource{
							Path: "/etc/spark/data",
						},
					},
				},
				{
					Name: "volume2",
					VolumeSource: apiv1.VolumeSource{
						EmptyDir: &apiv1.EmptyDirVolumeSource{},
					},
				},
			},
			Driver: v1alpha1.DriverSpec{
				SparkPodSpec: v1alpha1.SparkPodSpec{
					VolumeMounts: []apiv1.VolumeMount{
						{
							Name:      "volume1",
							MountPath: "/etc/spark/data",
						},
					},
				},
			},
		},
	}

	options, err := GetDriverVolumeMountConfOptions(app)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 2, len(options))
	assert.True(t, strings.HasPrefix(options[0], fmt.Sprintf("%s%s%s=", SparkDriverAnnotationKeyPrefix,
		VolumeMountsAnnotationPrefix, "volume1")))
	assert.True(t, strings.HasPrefix(options[1], fmt.Sprintf("%s%s%s=", SparkDriverAnnotationKeyPrefix,
		VolumesAnnotationPrefix, "volume1")))
}

func TestGetExecutorVolumeMountConfOptions(t *testing.T) {
	app := &v1alpha1.SparkApplication{
		Spec: v1alpha1.SparkApplicationSpec{
			Volumes: []apiv1.Volume{
				{
					Name: "volume1",
					VolumeSource: apiv1.VolumeSource{
						HostPath: &apiv1.HostPathVolumeSource{
							Path: "/etc/spark/data",
						},
					},
				},
				{
					Name: "volume2",
					VolumeSource: apiv1.VolumeSource{
						EmptyDir: &apiv1.EmptyDirVolumeSource{},
					},
				},
			},
			Executor: v1alpha1.ExecutorSpec{
				SparkPodSpec: v1alpha1.SparkPodSpec{
					VolumeMounts: []apiv1.VolumeMount{
						{
							Name:      "volume2",
							MountPath: "/etc/spark/work",
						},
					},
				},
			},
		},
	}

	options, err := GetExecutorVolumeMountConfOptions(app)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 2, len(options))
	assert.True(t, strings.HasPrefix(options[0], fmt.Sprintf("%s%s%s=", SparkExecutorAnnotationKeyPrefix,
		VolumeMountsAnnotationPrefix, "volume2")))
	assert.True(t, strings.HasPrefix(options[1], fmt.Sprintf("%s%s%s=", SparkExecutorAnnotationKeyPrefix,
		VolumesAnnotationPrefix, "volume2")))
}

func TestGetVolumeMountAnnotations(t *testing.T) {
	volumeMount1 := apiv1.VolumeMount{
		Name:      "volume1",
		MountPath: "/etc/spark/data",
	}
	volumeMount2 := apiv1.VolumeMount{
		Name:      "volume2",
		MountPath: "/etc/spark/work",
	}

	annotations, err := getVolumeMountAnnotations([]apiv1.VolumeMount{volumeMount1, volumeMount2})
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 2, len(annotations))
	value, ok := annotations[fmt.Sprintf("%s%s", VolumeMountsAnnotationPrefix, volumeMount1.Name)]
	assert.True(t, ok)
	mount, err := util.UnmarshalVolumeMount(value)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, volumeMount1.MountPath, mount.MountPath)
	value, ok = annotations[fmt.Sprintf("%s%s", VolumeMountsAnnotationPrefix, volumeMount2.Name)]
	assert.True(t, ok)
	mount, err = util.UnmarshalVolumeMount(value)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, volumeMount2.MountPath, mount.MountPath)
}
