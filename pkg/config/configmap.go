package config

import (
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/liyinan926/spark-operator/pkg/apis/v1alpha1"
	"github.com/liyinan926/spark-operator/pkg/util"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
)

// AddConfigMapAnnotation adds an annotation key=value using the --conf option.
func AddConfigMapAnnotation(app *v1alpha1.SparkApplication, annotationConfKey string, key string, value string) {
	annotations, ok := app.Spec.SparkConf[annotationConfKey]
	if ok {
		app.Spec.SparkConf[annotationConfKey] = fmt.Sprintf("%s,%s=%s", annotations, key, value)
	} else {
		app.Spec.SparkConf[annotationConfKey] = fmt.Sprintf("%s=%s", key, value)
	}
}

// CreateSparkConfigMap is to be used by the SparkApplication controller to create a ConfigMap from a directory of Spark configuration files.
func CreateSparkConfigMap(sparkConfDir string, namespace string, app *v1alpha1.SparkApplication, kubeClient clientset.Interface) error {
	name, err := createConfigMap(sparkConfDir, namespace, SparkConfigMapNamePrefix, app, kubeClient)
	if err != nil {
		return err
	}

	// Add an annotation to the driver and executor Pods so the initializer gets informed.
	AddConfigMapAnnotation(app, SparkDriverAnnotationsKey, SparkConfigMapAnnotation, name)
	AddConfigMapAnnotation(app, SparkExecutorAnnotationsKey, SparkConfigMapAnnotation, name)
	// Update the Spec to include the name of the newly created ConfigMap.
	app.Spec.SparkConfigMap = new(string)
	*app.Spec.SparkConfigMap = name

	return nil
}

// CreateHadoopConfigMap is to be used by the SparkApplication controller to create a ConfigMap from a directory of Hadoop configuration files.
func CreateHadoopConfigMap(hadoopConfDir string, namespace string, app *v1alpha1.SparkApplication, kubeClient clientset.Interface) error {
	name, err := createConfigMap(hadoopConfDir, namespace, HadoopConfigMapNamePrefix, app, kubeClient)
	if err != nil {
		return err
	}

	// Add an annotation to the driver and executor Pods so the initializer gets informed.
	AddConfigMapAnnotation(app, SparkDriverAnnotationsKey, HadoopConfigMapAnnotation, name)
	AddConfigMapAnnotation(app, SparkExecutorAnnotationsKey, HadoopConfigMapAnnotation, name)
	// Update the Spec to include the name of the newly created ConfigMap.
	app.Spec.HadoopConfigMap = new(string)
	*app.Spec.HadoopConfigMap = name

	return nil
}

// AddSparkConfigMapVolumeToPod add a ConfigMap volume for Spark configuration files into the given Pod.
func AddSparkConfigMapVolumeToPod(configMapName string, pod *apiv1.Pod) string {
	return addConfigMapVolumeToPod(configMapName, SparkConfigMapVolumeName, pod)
}

// AddHadoopConfigMapVolumeToPod add a ConfigMap volume for Hadoop configuration files into the given Pod.
func AddHadoopConfigMapVolumeToPod(configMapName string, pod *apiv1.Pod) string {
	return addConfigMapVolumeToPod(configMapName, HadoopConfigMapVolumeName, pod)
}

func addConfigMapVolumeToPod(configMapName string, configMapVolumeName string, pod *apiv1.Pod) string {
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
func MountSparkConfigMapToContainer(volumeName string, mountPath string, container *apiv1.Container) {
	mountConfigMapToContainer(volumeName, mountPath, SparkConfDirEnvVar, container)
}

// MountHadoopConfigMapToContainer mounts the ConfigMap for Hadoop configuration files into the given container.
func MountHadoopConfigMapToContainer(volumeName string, mountPath string, container *apiv1.Container) {
	mountConfigMapToContainer(volumeName, mountPath, HadoopConfDirEnvVar, container)
}

// MountConfigMapToContainer mounts the ConfigMap volume named volumeName onto mountPath into the given container.
func MountConfigMapToContainer(volumeName string, mountPath string, container *apiv1.Container) {
	mountConfigMapToContainer(volumeName, mountPath, "", container)
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

func createConfigMap(dir string, namespace string, namePrefix string, app *v1alpha1.SparkApplication, kubeClient clientset.Interface) (string, error) {
	configMap, err := buildConfigMapFromConfigDir(dir, namePrefix, namespace, string(app.UID))
	if err != nil {
		return configMap.Name, err
	}
	configMap, err = kubeClient.CoreV1().ConfigMaps(namespace).Create(configMap)
	if err != nil {
		return configMap.Name, err
	}
	return configMap.Name, nil
}

func buildConfigMapFromConfigDir(dir string, namePrefix string, namespace string, appUID string) (*apiv1.ConfigMap, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	configMap := &apiv1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
		},
	}

	hasher := util.NewHash32()
	data := make(map[string]string)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		bytes, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return nil, err
		}
		hasher.Write(bytes)
		data[file.Name()] = string(bytes)
	}
	configMap.Data = data

	hasher.Write([]byte(dir))
	hasher.Write([]byte(namespace))
	hasher.Write([]byte(appUID))
	configMap.Name = fmt.Sprintf("%s-%d", namePrefix, hasher.Sum32())

	return configMap, nil
}
