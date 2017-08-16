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
	// DefaultHadoopConfDir is the default directory for Spark configuration files if not specified.
	// This directory is where the Hadoop ConfigMap is mounted in the driver and executor containers.
	DefaultHadoopConfDir = "/etc/hadoop/conf"
	// HadoopConfigMapNamePrefix is the name prefix of the Hadoop ConfigMap created from the directory
	// in the submission client container specified by.
	HadoopConfigMapNamePrefix = "hadoop-config-map"
	// SparkDriverAnnotationsKey is the Spark configuation key for annotations on the driver Pod.
	SparkDriverAnnotationsKey = "spark.kubernetes.driver.annotations"
	// SparkExecutorAnnotationsKey is the Spark configuation key for annotations on the executor Pod.
	SparkExecutorAnnotationsKey = "spark.kubernetes.executor.annotations"
	// SparkConfigMapAnnotation is the name of the annotation added to the driver and executor Pods
	// through the "--conf" option of spark-submit that indicates the presence of a Spark ConfigMap.
	SparkConfigMapAnnotation = "spark-config-map"
	// HadoopConfigMapAnnotation is the name of the annotation added to the driver and executor Pods
	// through the "--conf" option of spark-submitthat indicates the presence of a Hadoop ConfigMap.
	HadoopConfigMapAnnotation = "hadoop-config-map"
	// SparkConfDirEnvVar is the environment variable to add to the driver and executor Pods that point
	// to the directory where the Spark ConfigMap is mounted.
	SparkConfDirEnvVar = "SPARK_CONF_DIR"
	// HadoopConfDirEnvVar is the environment variable to add to the driver and executor Pods that point
	// to the directory where the Hadoop ConfigMap is mounted.
	HadoopConfDirEnvVar = "HADOOP_CONF_DIR"
)

// CreateSparkConfigMap is to be used by the SparkApplication controller to create a ConfigMap from a directory of Spark configuration files.
func CreateSparkConfigMap(sparkConfDir string, namespace string, app *v1alpha1.SparkApplication, kubeClient clientset.Interface) error {
	configMap, err := buildConfigMapFromConfigDir(sparkConfDir, SparkConfigMapNamePrefix, namespace)
	if err != nil {
		return err
	}
	configMap, err = kubeClient.CoreV1().ConfigMaps(namespace).Create(configMap)
	if err != nil {
		return err
	}

	driverAnnotations, ok := app.Spec.SparkConf[SparkDriverAnnotationsKey]
	if !ok {
		app.Spec.SparkConf[SparkDriverAnnotationsKey] = fmt.Sprintf("%s=%s", SparkConfigMapAnnotation, configMap.Name)
	} else {
		app.Spec.SparkConf[SparkDriverAnnotationsKey] = fmt.Sprintf("%s,%s=%s", driverAnnotations, SparkConfigMapAnnotation, configMap.Name)
	}

	executorAnnotations, ok := app.Spec.SparkConf[SparkExecutorAnnotationsKey]
	if !ok {
		app.Spec.SparkConf[SparkExecutorAnnotationsKey] = fmt.Sprintf("%s=%s", SparkConfigMapAnnotation, configMap.Name)
	} else {
		app.Spec.SparkConf[SparkExecutorAnnotationsKey] = fmt.Sprintf("%s,%s=%s", executorAnnotations, SparkConfigMapAnnotation, configMap.Name)
	}

	app.Status.SparkConfigMapName = new(string)
	*app.Status.SparkConfigMapName = configMap.Name
	return nil
}

// CreateHadoopConfigMap is to be used by the SparkApplication controller to create a ConfigMap from a directory of Hadoop configuration files.
func CreateHadoopConfigMap(hadoopConfDir string, namespace string, app *v1alpha1.SparkApplication, kubeClient clientset.Interface) error {
	configMap, err := buildConfigMapFromConfigDir(hadoopConfDir, HadoopConfigMapNamePrefix, namespace)
	if err != nil {
		return err
	}
	configMap, err = kubeClient.CoreV1().ConfigMaps(namespace).Create(configMap)
	if err != nil {
		return err
	}

	driverAnnotations, ok := app.Spec.SparkConf[SparkDriverAnnotationsKey]
	if !ok {
		app.Spec.SparkConf[SparkDriverAnnotationsKey] = fmt.Sprintf("%s=%s", HadoopConfigMapAnnotation, configMap.Name)
	} else {
		app.Spec.SparkConf[SparkDriverAnnotationsKey] = fmt.Sprintf("%s,%s=%s", driverAnnotations, HadoopConfigMapAnnotation, configMap.Name)
	}

	executorAnnotations, ok := app.Spec.SparkConf[SparkExecutorAnnotationsKey]
	if !ok {
		app.Spec.SparkConf[SparkExecutorAnnotationsKey] = fmt.Sprintf("%s=%s", HadoopConfigMapAnnotation, configMap.Name)
	} else {
		app.Spec.SparkConf[SparkExecutorAnnotationsKey] = fmt.Sprintf("%s,%s=%s", executorAnnotations, HadoopConfigMapAnnotation, configMap.Name)
	}

	app.Status.HadoopConfigMapName = new(string)
	*app.Status.HadoopConfigMapName = configMap.Name
	return nil
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
