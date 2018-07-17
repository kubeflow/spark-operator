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

	apiv1 "k8s.io/api/core/v1"

	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	"k8s.io/spark-on-k8s-operator/pkg/util"
)

func getVolumeAnnotationsForMounts(volumes []apiv1.Volume, mounts []apiv1.VolumeMount) (map[string]string, error) {
	volumeMap := make(map[string]apiv1.Volume)
	for _, v := range volumes {
		volumeMap[v.Name] = v
	}

	annotations := make(map[string]string)
	for _, mount := range mounts {
		v, ok := volumeMap[mount.Name]
		if !ok {
			return nil, fmt.Errorf("no volume named %s found", mount.Name)
		}
		volumeStr, err := util.MarshalVolume(&v)
		if err != nil {
			return nil, err
		}
		key := fmt.Sprintf("%s%s", VolumesAnnotationPrefix, v.Name)
		annotations[key] = volumeStr
	}

	return annotations, nil
}

// GetDriverVolumeMountConfOptions returns a list of spark-submit options for driver annotations for volumes to be
// mounted into the driver.
func GetDriverVolumeMountConfOptions(app *v1alpha1.SparkApplication) ([]string, error) {
	var options []string

	annotations, err := getVolumeMountAnnotations(app.Spec.Driver.VolumeMounts)
	if err != nil {
		return nil, err
	}
	for key, value := range annotations {
		options = append(options, GetDriverAnnotationOption(key, value))
	}

	annotations, err = getVolumeAnnotationsForMounts(app.Spec.Volumes, app.Spec.Driver.VolumeMounts)
	if err != nil {
		return nil, err
	}
	for key, value := range annotations {
		options = append(options, GetDriverAnnotationOption(key, value))
	}

	return options, nil
}

// GetExecutorVolumeMountConfOptions returns a list of spark-submit options for executor annotations for volumes to be
// mounted into the executors.
func GetExecutorVolumeMountConfOptions(app *v1alpha1.SparkApplication) ([]string, error) {
	var options []string

	annotations, err := getVolumeMountAnnotations(app.Spec.Executor.VolumeMounts)
	if err != nil {
		return nil, err
	}
	for key, value := range annotations {
		options = append(options, GetExecutorAnnotationOption(key, value))
	}

	annotations, err = getVolumeAnnotationsForMounts(app.Spec.Volumes, app.Spec.Executor.VolumeMounts)
	if err != nil {
		return nil, err
	}
	for key, value := range annotations {
		options = append(options, GetExecutorAnnotationOption(key, value))
	}

	return options, nil
}

func getVolumeMountAnnotations(volumeMounts []apiv1.VolumeMount) (map[string]string, error) {
	annotations := make(map[string]string)
	for _, v := range volumeMounts {
		volumeMountStr, err := util.MarshalVolumeMount(&v)
		if err != nil {
			return nil, err
		}
		key := fmt.Sprintf("%s%s", VolumeMountsAnnotationPrefix, v.Name)
		annotations[key] = volumeMountStr
	}

	return annotations, nil
}

// FindVolumes finds and parses Volumes in the given annotations.
func FindVolumes(annotations map[string]string) (map[string]*apiv1.Volume, error) {
	volumes := make(map[string]*apiv1.Volume)
	for key, value := range annotations {
		if strings.HasPrefix(key, VolumesAnnotationPrefix) {
			volume, err := util.UnmarshalVolume(value)
			if err != nil {
				return nil, err
			}
			key := strings.TrimPrefix(key, VolumesAnnotationPrefix)
			volumes[key] = volume
		}
	}

	return volumes, nil
}

// FindVolumeMounts finds and parses VolumeMounts in the given annotations.
func FindVolumeMounts(annotations map[string]string) (map[string]*apiv1.VolumeMount, error) {
	volumeMounts := make(map[string]*apiv1.VolumeMount)
	for key, value := range annotations {
		if strings.HasPrefix(key, VolumeMountsAnnotationPrefix) {
			volumeMount, err := util.UnmarshalVolumeMount(value)
			if err != nil {
				return nil, err
			}
			key := strings.TrimPrefix(key, VolumeMountsAnnotationPrefix)
			volumeMounts[key] = volumeMount
		}
	}

	return volumeMounts, nil
}
