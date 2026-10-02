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

package webhook

import (
	"fmt"

	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/kubeflow/spark-operator/v2/pkg/common"
)

var deniedSparkConfKeys = map[string]string{
	common.SparkKubernetesAuthenticateDriverServiceAccountName:   "configure the service account via the CRD spec or pod template instead",
	common.SparkKubernetesAuthenticateExecutorServiceAccountName: "configure the service account via the CRD spec or pod template instead",
	common.SparkKubernetesAuthenticateOAuthTokenFile:             "authentication credentials are managed by the operator",
	common.SparkKubernetesAuthenticateOAuthToken:                 "authentication credentials are managed by the operator",
	common.SparkKubernetesAuthenticateDriverOAuthTokenFile:       "authentication credentials are managed by the operator",
	common.SparkKubernetesAuthenticateDriverOAuthToken:           "authentication credentials are managed by the operator",
	common.SparkKubernetesAuthenticateExecutorOAuthTokenFile:     "authentication credentials are managed by the operator",
	common.SparkKubernetesAuthenticateExecutorOAuthToken:         "authentication credentials are managed by the operator",
	common.SparkMaster:                           "this value is managed by the operator",
	common.SparkKubernetesDriverMaster:           "this value is managed by the operator",
	common.SparkKubernetesContainerImage:         "use the image field on the CRD instead",
	common.SparkKubernetesDriverContainerImage:   "use the image field on the CRD instead",
	common.SparkKubernetesExecutorContainerImage: "use the image field on the CRD instead",
}

func validateSparkConf(path *field.Path, sparkConf map[string]string, namespace string) field.ErrorList {
	for key, value := range sparkConf {
		if msg, denied := deniedSparkConfKeys[key]; denied {
			return field.ErrorList{field.Forbidden(path.Key(key), msg)}
		}
		if key == common.SparkKubernetesNamespace && value != namespace {
			return field.ErrorList{field.Invalid(path.Key(key), value, fmt.Sprintf("must equal the application namespace %q", namespace))}
		}
	}
	return nil
}
