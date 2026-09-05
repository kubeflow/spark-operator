/*
Copyright The Kubeflow Authors.

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
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

// validateConfigMaps rejects ConfigMap references no ConfigMap could satisfy, and names
// repeated within one list, which would mount two pod volumes under the same name. Neither
// surfaces before the API server rejects the pod the mutating webhook has already built.
func validateConfigMaps(spec *v1beta2.SparkApplicationSpec, root *field.Path) field.ErrorList {
	var errs field.ErrorList

	if spec.SparkConfigMap != nil {
		errs = append(errs, validateConfigMapName(root.Child("sparkConfigMap"), *spec.SparkConfigMap)...)
	}
	if spec.HadoopConfigMap != nil {
		errs = append(errs, validateConfigMapName(root.Child("hadoopConfigMap"), *spec.HadoopConfigMap)...)
	}

	errs = append(errs, validateConfigMapList(root.Child("driver", "configMaps"), spec.Driver.ConfigMaps)...)
	errs = append(errs, validateConfigMapList(root.Child("executor", "configMaps"), spec.Executor.ConfigMaps)...)

	return errs
}

func validateConfigMapList(path *field.Path, configMaps []v1beta2.NamePath) field.ErrorList {
	var errs field.ErrorList
	// A repeated name only collides because the mutating webhook builds one volume per entry.
	// Once it builds one volume per distinct ConfigMap, mount paths become the thing to keep
	// unique instead; tracked at https://github.com/kubeflow/spark-operator/issues/3134
	seen := make(map[string]bool, len(configMaps))
	for i, configMap := range configMaps {
		namePath := path.Index(i).Child("name")
		errs = append(errs, validateConfigMapName(namePath, configMap.Name)...)
		if seen[configMap.Name] {
			errs = append(errs, field.Duplicate(namePath, configMap.Name))
		}
		seen[configMap.Name] = true
	}
	return errs
}

func validateConfigMapName(path *field.Path, name string) field.ErrorList {
	return newInvalidErrors(path, name, validation.IsDNS1123Subdomain(name))
}
