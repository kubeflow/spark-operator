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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
)

func TestGetDriverEnvVarConfOptions(t *testing.T) {
	app := &v1beta2.SparkApplication{
		Spec: v1beta2.SparkApplicationSpec{
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					EnvVars: map[string]string{
						"ENV1": "VALUE1",
						"ENV2": "VALUE2",
					},
				},
			},
		},
	}

	options := GetDriverEnvVarConfOptions(app)
	optionsMap := map[string]bool{
		strings.TrimPrefix(options[0], SparkDriverEnvVarConfigKeyPrefix): true,
		strings.TrimPrefix(options[1], SparkDriverEnvVarConfigKeyPrefix): true,
	}
	assert.Equal(t, 2, len(optionsMap))
	assert.True(t, optionsMap["ENV1=VALUE1"])
	assert.True(t, optionsMap["ENV2=VALUE2"])
}

func TestGetExecutorEnvVarConfOptions(t *testing.T) {
	app := &v1beta2.SparkApplication{
		Spec: v1beta2.SparkApplicationSpec{
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					EnvVars: map[string]string{
						"ENV1": "VALUE1",
						"ENV2": "VALUE2",
					},
				},
			},
		},
	}

	options := GetExecutorEnvVarConfOptions(app)
	optionsMap := map[string]bool{
		strings.TrimPrefix(options[0], SparkExecutorEnvVarConfigKeyPrefix): true,
		strings.TrimPrefix(options[1], SparkExecutorEnvVarConfigKeyPrefix): true,
	}
	assert.Equal(t, 2, len(optionsMap))
	assert.True(t, optionsMap["ENV1=VALUE1"])
	assert.True(t, optionsMap["ENV2=VALUE2"])
}
