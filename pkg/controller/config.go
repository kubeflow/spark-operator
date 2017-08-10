package controller

import (
	"io/ioutil"
	"path/filepath"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
)

const (
	DefaultSparkConfDir       = "/etc/spark/conf"
	SparkConfigMapNamePrefix  = "spark-config-map"
	DefaultHadoopConfDir      = "/etc/hadoop/conf"
	HadoopConfigMapNamePrefix = "hadoop-config-map"
)

func createConfigMapFromConfigDir(dir string, namespace string, kubeClient clientset.Interface) (*apiv1.ConfigMap, error) {
	configMap, err := buildConfigMapFromConfigDir(dir, namespace)
	if err != nil {
		return nil, err
	}

	return kubeClient.CoreV1().ConfigMaps(namespace).Create(configMap)
}

func buildConfigMapFromConfigDir(dir string, namespace string) (*apiv1.ConfigMap, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	configMap := &apiv1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      SparkConfigMapNamePrefix,
			Namespace: namespace,
		},
	}

	data := make(map[string]string)
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		bytes, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return nil, err
		}
		data[file.Name()] = string(bytes)
	}
	configMap.Data = data
	return configMap, nil
}
