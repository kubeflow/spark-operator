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

const (
	// DefaultSparkConfDir is the default directory for Spark configuration files if not specified.
	// This directory is where the Spark ConfigMap is mounted in the driver and executor containers.
	DefaultSparkConfDir = "/etc/spark/conf"
	// SparkConfigMapNamePrefix is the name prefix of the Spark ConfigMap created from the directory
	// in the submission client container specified by SparkApplicationSpec.SparkConfDir.
	SparkConfigMapNamePrefix = "spark-config-map"
	// SparkConfigMapAnnotation is the name of the annotation added to the driver and executor Pods
	// through the "--conf" option of spark-submit that indicates the presence of a Spark ConfigMap.
	SparkConfigMapAnnotation = "apache-spark-on-k8s/spark-config-map"
	// SparkConfigMapVolumeName is the name of the ConfigMap volume of Spark configuration files.
	SparkConfigMapVolumeName = "spark-config-map-volume"
	// DefaultHadoopConfDir is the default directory for Spark configuration files if not specified.
	// This directory is where the Hadoop ConfigMap is mounted in the driver and executor containers.
	DefaultHadoopConfDir = "/etc/hadoop/conf"
	// HadoopConfigMapNamePrefix is the name prefix of the Hadoop ConfigMap created from the directory
	// in the submission client container specified by.
	HadoopConfigMapNamePrefix = "hadoop-config-map"
	// HadoopConfigMapAnnotation is the name of the annotation added to the driver and executor Pods
	// through the "--conf" option of spark-submitthat indicates the presence of a Hadoop ConfigMap.
	HadoopConfigMapAnnotation = "apache-spark-on-k8s/hadoop-config-map"
	// HadoopConfigMapVolumeName is the name of the ConfigMap volume of Hadoop configuration files.
	HadoopConfigMapVolumeName = "hadoop-config-map-volume"
)

const (
	// SparkDriverAnnotationsKey is the Spark configuation key for annotations on the driver Pod.
	SparkDriverAnnotationsKey = "spark.kubernetes.driver.annotations"
	// SparkExecutorAnnotationsKey is the Spark configuation key for annotations on the executor Pod.
	SparkExecutorAnnotationsKey = "spark.kubernetes.executor.annotations"
	// SparkConfDirEnvVar is the environment variable to add to the driver and executor Pods that point
	// to the directory where the Spark ConfigMap is mounted.
	SparkConfDirEnvVar = "SPARK_CONF_DIR"
	// HadoopConfDirEnvVar is the environment variable to add to the driver and executor Pods that point
	// to the directory where the Hadoop ConfigMap is mounted.
	HadoopConfDirEnvVar = "HADOOP_CONF_DIR"
)

// CreateSparkConfigMap is to be used by the SparkApplication controller to create a ConfigMap from a directory of Spark configuration files.
func CreateSparkConfigMap(sparkConfDir string, namespace string, app *v1alpha1.SparkApplication, kubeClient clientset.Interface) error {
	name, err := createConfigMap(sparkConfDir, namespace, SparkConfigMapNamePrefix, app, kubeClient)
	if err != nil {
		return err
	}

	updateAnnotation(app, SparkDriverAnnotationsKey, SparkConfigMapAnnotation, name)
	updateAnnotation(app, SparkExecutorAnnotationsKey, SparkConfigMapAnnotation, name)
	app.Status.SparkConfigMapName = new(string)
	*app.Status.SparkConfigMapName = name

	return nil
}

// CreateHadoopConfigMap is to be used by the SparkApplication controller to create a ConfigMap from a directory of Hadoop configuration files.
func CreateHadoopConfigMap(hadoopConfDir string, namespace string, app *v1alpha1.SparkApplication, kubeClient clientset.Interface) error {
	name, err := createConfigMap(hadoopConfDir, namespace, HadoopConfigMapNamePrefix, app, kubeClient)
	if err != nil {
		return err
	}

	updateAnnotation(app, SparkDriverAnnotationsKey, HadoopConfigMapAnnotation, name)
	updateAnnotation(app, SparkExecutorAnnotationsKey, HadoopConfigMapAnnotation, name)
	app.Status.HadoopConfigMapName = new(string)
	*app.Status.HadoopConfigMapName = name

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

func mountConfigMapToContainer(volumeName string, mountPath string, env string, container *apiv1.Container) {
	volumeMount := apiv1.VolumeMount{
		Name:      volumeName,
		ReadOnly:  true,
		MountPath: mountPath,
	}
	container.VolumeMounts = append(container.VolumeMounts, volumeMount)
	appCredentialEnvVar := apiv1.EnvVar{Name: env, Value: mountPath}
	container.Env = append(container.Env, appCredentialEnvVar)
}

func createConfigMap(dir string, namespace string, namePrefix string, app *v1alpha1.SparkApplication, kubeClient clientset.Interface) (string, error) {
	configMap, err := buildConfigMapFromConfigDir(dir, namePrefix, namespace)
	if err != nil {
		return configMap.Name, err
	}
	configMap, err = kubeClient.CoreV1().ConfigMaps(namespace).Create(configMap)
	if err != nil {
		return configMap.Name, err
	}
	return configMap.Name, nil
}

func updateAnnotation(app *v1alpha1.SparkApplication, annotationConfKey string, key string, value string) {
	annotations, ok := app.Spec.SparkConf[annotationConfKey]
	if ok {
		app.Spec.SparkConf[annotationConfKey] = fmt.Sprintf("%s,%s=%s", annotations, key, value)
	} else {
		app.Spec.SparkConf[annotationConfKey] = fmt.Sprintf("%s=%s", key, value)
	}
}

func buildConfigMapFromConfigDir(dir string, namePrefix string, namespace string) (*apiv1.ConfigMap, error) {
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
	configMap.Name = fmt.Sprintf("%s-%d", namePrefix, hasher.Sum32())

	return configMap, nil
}
