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
)

// FindGeneralConfigMaps finds the annotations for specifying general secrets and returns
// an map of names of the secrets to their mount paths.
func FindGeneralConfigMaps(annotations map[string]string) map[string]string {
	configMaps := make(map[string]string)
	for annotation := range annotations {
		if strings.HasPrefix(annotation, GeneralConfigMapsAnnotationPrefix) {
			name := strings.TrimPrefix(annotation, GeneralConfigMapsAnnotationPrefix)
			path := annotations[annotation]
			configMaps[name] = path
		}
	}
	return configMaps
}

// GetDriverConfigMapConfOptions returns a list of spark-submit options for driver annotations for ConfigMaps to be
// mounted into the driver.
func GetDriverConfigMapConfOptions(app *v1alpha1.SparkApplication) []string {
	var options []string
	for key, value := range getConfigMapAnnotations(app.Spec.Driver.ConfigMaps) {
		options = append(options, GetDriverAnnotationOption(key, value))
	}
	return options
}

// GetExecutorConfigMapConfOptions returns a list of spark-submit options for executor annotations for ConfigMaps to be
// mounted into the executors.
func GetExecutorConfigMapConfOptions(app *v1alpha1.SparkApplication) []string {
	var options []string
	for key, value := range getConfigMapAnnotations(app.Spec.Executor.ConfigMaps) {
		options = append(options, GetExecutorAnnotationOption(key, value))
	}
	return options
}

func getConfigMapAnnotations(namePaths []v1alpha1.NamePath) map[string]string {
	annotations := make(map[string]string)
	for _, np := range namePaths {
		key := fmt.Sprintf("%s%s", GeneralConfigMapsAnnotationPrefix, np.Name)
		annotations[key] = np.Path
	}

	return annotations
}

// AddSparkConfigMapVolumeToPod add a ConfigMap volume for Spark configuration files into the given pod.
func AddSparkConfigMapVolumeToPod(configMapName string, pod *apiv1.Pod) string {
	return AddConfigMapVolumeToPod(SparkConfigMapVolumeName, configMapName, pod)
}

// AddHadoopConfigMapVolumeToPod add a ConfigMap volume for Hadoop configuration files into the given pod.
func AddHadoopConfigMapVolumeToPod(configMapName string, pod *apiv1.Pod) string {
	return AddConfigMapVolumeToPod(HadoopConfigMapVolumeName, configMapName, pod)
}

// AddConfigMapVolumeToPod adds a ConfigMap volume into the given pod.
func AddConfigMapVolumeToPod(configMapVolumeName string, configMapName string, pod *apiv1.Pod) string {
	volume := apiv1.Volume{
		Name: configMapVolumeName,
		VolumeSource: apiv1.VolumeSource{
			ConfigMap: &apiv1.ConfigMapVolumeSource{
				LocalObjectReference: apiv1.LocalObjectReference{
					Name: configMapName,
				},
			},
		},
	}
	pod.Spec.Volumes = append(pod.Spec.Volumes, volume)
	return volume.Name
}

// MountSparkConfigMapToContainer mounts the ConfigMap for Spark configuration files into the given container.
func MountSparkConfigMapToContainer(container *apiv1.Container) {
	mountConfigMapToContainer(SparkConfigMapVolumeName, DefaultSparkConfDir, SparkConfDirEnvVar, container)
	appendSparkClasspathEnvVar(DefaultSparkConfDir, container)
}

// MountHadoopConfigMapToContainer mounts the ConfigMap for Hadoop configuration files into the given container.
func MountHadoopConfigMapToContainer(container *apiv1.Container) {
	mountConfigMapToContainer(HadoopConfigMapVolumeName, DefaultHadoopConfDir, HadoopConfDirEnvVar, container)
	appendSparkClasspathEnvVar(DefaultHadoopConfDir, container)
}

// MountConfigMapToContainer mounts the ConfigMap volume named volumeName onto mountPath into the given container.
func MountConfigMapToContainer(volumeName string, mountPath string, container *apiv1.Container) {
	mountConfigMapToContainer(volumeName, mountPath, "", container)
}

func appendSparkClasspathEnvVar(sparkClasspath string, container *apiv1.Container) {
	for i, envVar := range container.Env {
		if envVar.Name == SparkClasspathEnvVar {
			container.Env[i].Value = fmt.Sprintf("%s:%s", sparkClasspath, envVar.Value)
			return
		}
	}

	container.Env = append(
		container.Env,
		apiv1.EnvVar{
			Name:  SparkClasspathEnvVar,
			Value: fmt.Sprintf("%s:$%s", sparkClasspath, SparkClasspathEnvVar),
		})
}

func mountConfigMapToContainer(volumeName string, mountPath string, env string, container *apiv1.Container) {
	volumeMount := apiv1.VolumeMount{
		Name:      volumeName,
		ReadOnly:  true,
		MountPath: mountPath,
	}
	container.VolumeMounts = append(container.VolumeMounts, volumeMount)
	if env != "" {
		appCredentialEnvVar := apiv1.EnvVar{Name: env, Value: mountPath}
		container.Env = append(container.Env, appCredentialEnvVar)
	}
}
