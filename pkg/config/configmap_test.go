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

	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
)

func TestFindGeneralConfigMaps(t *testing.T) {
	annotations := make(map[string]string)
	annotations[fmt.Sprintf("%s%s", GeneralConfigMapsAnnotationPrefix, "configmap1")] = "/etc/config"
	annotations[fmt.Sprintf("%s%s", GeneralConfigMapsAnnotationPrefix, "configmap2")] = "/etc/config"

	configMaps := FindGeneralConfigMaps(annotations)
	assert.Equal(t, 2, len(configMaps))
	assert.Equal(t, "/etc/config", configMaps["configmap1"])
	assert.Equal(t, "/etc/config", configMaps["configmap2"])
}

func TestGetDriverConfigMapConfOptions(t *testing.T) {
	app := &v1alpha1.SparkApplication{
		Spec: v1alpha1.SparkApplicationSpec{
			Driver: v1alpha1.DriverSpec{
				SparkPodSpec: v1alpha1.SparkPodSpec{
					ConfigMaps: []v1alpha1.NamePath{
						{
							Name: "configmap1",
							Path: "/etc/config",
						},
						{
							Name: "configmap2",
							Path: "/etc/config",
						},
					},
				},
			},
		},
	}

	options := GetDriverConfigMapConfOptions(app)
	optionsMap := map[string]bool{
		strings.TrimPrefix(options[0],
			fmt.Sprintf("%s%s", SparkDriverAnnotationKeyPrefix, GeneralConfigMapsAnnotationPrefix)): true,
		strings.TrimPrefix(options[1],
			fmt.Sprintf("%s%s", SparkDriverAnnotationKeyPrefix, GeneralConfigMapsAnnotationPrefix)): true,
	}
	assert.Equal(t, 2, len(options))
	assert.True(t, optionsMap["configmap1=/etc/config"])
	assert.True(t, optionsMap["configmap2=/etc/config"])
}

func TestGetExecutorConfigMapConfOptions(t *testing.T) {
	app := &v1alpha1.SparkApplication{
		Spec: v1alpha1.SparkApplicationSpec{
			Executor: v1alpha1.ExecutorSpec{
				SparkPodSpec: v1alpha1.SparkPodSpec{
					ConfigMaps: []v1alpha1.NamePath{
						{
							Name: "configmap1",
							Path: "/etc/config",
						},
						{
							Name: "configmap2",
							Path: "/etc/config",
						},
					},
				},
			},
		},
	}

	options := GetExecutorConfigMapConfOptions(app)
	optionsMap := map[string]bool{
		strings.TrimPrefix(options[0],
			fmt.Sprintf("%s%s", SparkExecutorAnnotationKeyPrefix, GeneralConfigMapsAnnotationPrefix)): true,
		strings.TrimPrefix(options[1],
			fmt.Sprintf("%s%s", SparkExecutorAnnotationKeyPrefix, GeneralConfigMapsAnnotationPrefix)): true,
	}
	assert.Equal(t, 2, len(optionsMap))
	assert.True(t, optionsMap["configmap1=/etc/config"])
	assert.True(t, optionsMap["configmap2=/etc/config"])
}

func TestGetConfigMapAnnotations(t *testing.T) {
	namePaths := []v1alpha1.NamePath{
		{
			Name: "configmap1",
			Path: "/etc/config",
		},
		{
			Name: "configmap2",
			Path: "/etc/config",
		},
	}

	annotations := getConfigMapAnnotations(namePaths)
	value, ok := annotations[fmt.Sprintf("%s%s", GeneralConfigMapsAnnotationPrefix, "configmap1")]
	assert.True(t, ok)
	assert.Equal(t, "/etc/config", value)
	value, ok = annotations[fmt.Sprintf("%s%s", GeneralConfigMapsAnnotationPrefix, "configmap2")]
	assert.True(t, ok)
	assert.Equal(t, "/etc/config", value)
}
