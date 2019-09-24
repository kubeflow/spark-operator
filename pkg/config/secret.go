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
	"path/filepath"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
)

// GetDriverSecretConfOptions returns a list of spark-submit options for mounting driver secrets.
func GetDriverSecretConfOptions(app *v1beta2.SparkApplication) []string {
	var secretConfOptions []string
	for _, s := range app.Spec.Driver.Secrets {
		conf := fmt.Sprintf("%s%s=%s", SparkDriverSecretKeyPrefix, s.Name, s.Path)
		secretConfOptions = append(secretConfOptions, conf)
		if s.Type == v1beta2.GCPServiceAccountSecret {
			conf = fmt.Sprintf(
				"%s%s=%s",
				SparkDriverEnvVarConfigKeyPrefix,
				GoogleApplicationCredentialsEnvVar,
				filepath.Join(s.Path, ServiceAccountJSONKeyFileName))
			secretConfOptions = append(secretConfOptions, conf)
		} else if s.Type == v1beta2.HadoopDelegationTokenSecret {
			conf = fmt.Sprintf(
				"%s%s=%s",
				SparkDriverEnvVarConfigKeyPrefix,
				HadoopTokenFileLocationEnvVar,
				filepath.Join(s.Path, HadoopDelegationTokenFileName))
			secretConfOptions = append(secretConfOptions, conf)
		}
	}
	return secretConfOptions
}

// GetExecutorSecretConfOptions returns a list of spark-submit options for mounting executor secrets.
func GetExecutorSecretConfOptions(app *v1beta2.SparkApplication) []string {
	var secretConfOptions []string
	for _, s := range app.Spec.Executor.Secrets {
		conf := fmt.Sprintf("%s%s=%s", SparkExecutorSecretKeyPrefix, s.Name, s.Path)
		secretConfOptions = append(secretConfOptions, conf)
		if s.Type == v1beta2.GCPServiceAccountSecret {
			conf = fmt.Sprintf(
				"%s%s=%s",
				SparkExecutorEnvVarConfigKeyPrefix,
				GoogleApplicationCredentialsEnvVar,
				filepath.Join(s.Path, ServiceAccountJSONKeyFileName))
			secretConfOptions = append(secretConfOptions, conf)
		} else if s.Type == v1beta2.HadoopDelegationTokenSecret {
			conf = fmt.Sprintf(
				"%s%s=%s",
				SparkExecutorEnvVarConfigKeyPrefix,
				HadoopTokenFileLocationEnvVar,
				filepath.Join(s.Path, HadoopDelegationTokenFileName))
			secretConfOptions = append(secretConfOptions, conf)
		}
	}
	return secretConfOptions
}
