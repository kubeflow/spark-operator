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

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
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
							Type: v1beta2.GCPServiceAccountSecret,
						},
						{
							Name: "hadoop-token",
							Path: "/etc/secrets",
							Type: v1beta2.HadoopDelegationTokenSecret,
						},
					},
				},
			},
		},
	}

	options := GetDriverSecretConfOptions(app)
	assert.Equal(t, 5, len(options))
	assert.Equal(t, fmt.Sprintf("%s=%s", "db-credentials", "/etc/secrets"), strings.TrimPrefix(options[0],
		SparkDriverSecretKeyPrefix))
	assert.Equal(t, fmt.Sprintf("%s=%s", "gcp-service-account", "/etc/secrets"),
		strings.TrimPrefix(options[1], SparkDriverSecretKeyPrefix))
	assert.Equal(t, fmt.Sprintf("%s=%s%s", GoogleApplicationCredentialsEnvVar, "/etc/secrets/",
		ServiceAccountJSONKeyFileName), strings.TrimPrefix(options[2], SparkDriverEnvVarConfigKeyPrefix))
	assert.Equal(t, fmt.Sprintf("%s=%s", "hadoop-token", "/etc/secrets"), strings.TrimPrefix(options[3],
		SparkDriverSecretKeyPrefix))
	assert.Equal(t, fmt.Sprintf("%s=%s%s", HadoopTokenFileLocationEnvVar, "/etc/secrets/",
		HadoopDelegationTokenFileName), strings.TrimPrefix(options[4], SparkDriverEnvVarConfigKeyPrefix))
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
							Type: v1beta2.GCPServiceAccountSecret,
						},
						{
							Name: "hadoop-token",
							Path: "/etc/secrets",
							Type: v1beta2.HadoopDelegationTokenSecret,
						},
					},
				},
			},
		},
	}

	options := GetExecutorSecretConfOptions(app)
	assert.Equal(t, 5, len(options))
	assert.Equal(t, fmt.Sprintf("%s=%s", "db-credentials", "/etc/secrets"), strings.TrimPrefix(options[0],
		SparkExecutorSecretKeyPrefix))
	assert.Equal(t, fmt.Sprintf("%s=%s", "gcp-service-account", "/etc/secrets"),
		strings.TrimPrefix(options[1], SparkExecutorSecretKeyPrefix))
	assert.Equal(t, fmt.Sprintf("%s=%s%s", GoogleApplicationCredentialsEnvVar, "/etc/secrets/",
		ServiceAccountJSONKeyFileName), strings.TrimPrefix(options[2], SparkExecutorEnvVarConfigKeyPrefix))
	assert.Equal(t, fmt.Sprintf("%s=%s", "hadoop-token", "/etc/secrets"), strings.TrimPrefix(options[3],
		SparkExecutorSecretKeyPrefix))
	assert.Equal(t, fmt.Sprintf("%s=%s%s", HadoopTokenFileLocationEnvVar, "/etc/secrets/",
		HadoopDelegationTokenFileName), strings.TrimPrefix(options[4], SparkExecutorEnvVarConfigKeyPrefix))
}
