/*
Copyright 2024 The Kubeflow authors.

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

package util_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
)

func TestGetDriverSecretConfOptions(t *testing.T) {
	app := &v1beta2.SparkApplication{
		Spec: v1beta2.SparkApplicationSpec{
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Secrets: []v1beta2.SecretInfo{
						{
							Name: "db-credentials",
							Path: "/etc/secrets",
						},
						{
							Name: "gcp-service-account",
							Path: "/etc/secrets",
							Type: v1beta2.SecretTypeGCPServiceAccount,
						},
						{
							Name: "hadoop-token",
							Path: "/etc/secrets",
							Type: v1beta2.SecretTypeHadoopDelegationToken,
						},
					},
				},
			},
		},
	}

	options := util.GetDriverSecretConfOptions(app)
	assert.Len(t, options, 5)
	assert.Equal(t, fmt.Sprintf("%s=%s", "db-credentials", "/etc/secrets"), strings.TrimPrefix(options[0],
		common.SparkKubernetesDriverSecretsPrefix))
	assert.Equal(t, fmt.Sprintf("%s=%s", "gcp-service-account", "/etc/secrets"),
		strings.TrimPrefix(options[1], common.SparkKubernetesDriverSecretsPrefix))
	assert.Equal(t, fmt.Sprintf("%s=%s%s", common.EnvGoogleApplicationCredentials, "/etc/secrets/",
		common.ServiceAccountJSONKeyFileName), strings.TrimPrefix(options[2], common.SparkKubernetesDriverEnvPrefix))
	assert.Equal(t, fmt.Sprintf("%s=%s", "hadoop-token", "/etc/secrets"), strings.TrimPrefix(options[3],
		common.SparkKubernetesDriverSecretsPrefix))
	assert.Equal(t, fmt.Sprintf("%s=%s%s", common.EnvHadoopTokenFileLocation, "/etc/secrets/",
		common.HadoopDelegationTokenFileName), strings.TrimPrefix(options[4], common.SparkKubernetesDriverEnvPrefix))
}

func TestGetExecutorSecretConfOptions(t *testing.T) {
	app := &v1beta2.SparkApplication{
		Spec: v1beta2.SparkApplicationSpec{
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Secrets: []v1beta2.SecretInfo{
						{
							Name: "db-credentials",
							Path: "/etc/secrets",
						},
						{
							Name: "gcp-service-account",
							Path: "/etc/secrets",
							Type: v1beta2.SecretTypeGCPServiceAccount,
						},
						{
							Name: "hadoop-token",
							Path: "/etc/secrets",
							Type: v1beta2.SecretTypeHadoopDelegationToken,
						},
					},
				},
			},
		},
	}

	options := util.GetExecutorSecretConfOptions(app)
	assert.Len(t, options, 5)
	assert.Equal(t, fmt.Sprintf("%s=%s", "db-credentials", "/etc/secrets"), strings.TrimPrefix(options[0],
		common.SparkKubernetesExecutorSecretsPrefix))
	assert.Equal(t, fmt.Sprintf("%s=%s", "gcp-service-account", "/etc/secrets"),
		strings.TrimPrefix(options[1], common.SparkKubernetesExecutorSecretsPrefix))
	assert.Equal(t, fmt.Sprintf("%s=%s%s", common.EnvGoogleApplicationCredentials, "/etc/secrets/",
		common.ServiceAccountJSONKeyFileName), strings.TrimPrefix(options[2], common.SparkExecutorEnvVarConfigKeyPrefix))
	assert.Equal(t, fmt.Sprintf("%s=%s", "hadoop-token", "/etc/secrets"), strings.TrimPrefix(options[3],
		common.SparkKubernetesExecutorSecretsPrefix))
	assert.Equal(t, fmt.Sprintf("%s=%s%s", common.EnvHadoopTokenFileLocation, "/etc/secrets/",
		common.HadoopDelegationTokenFileName), strings.TrimPrefix(options[4], common.SparkExecutorEnvVarConfigKeyPrefix))
}
