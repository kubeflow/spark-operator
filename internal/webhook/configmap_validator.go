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
	"errors"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

// validateConfigMaps rejects ConfigMap references no ConfigMap could satisfy, and mount
// paths repeated within one list, which would mount two volumes at the same place. Neither
// surfaces before the API server rejects the pod the mutating webhook has already built.
func validateConfigMaps(spec *v1beta2.SparkApplicationSpec, root *field.Path) error {
	var errs []error

	if spec.SparkConfigMap != nil {
		if err := validateConfigMapName(root.Child("sparkConfigMap"), *spec.SparkConfigMap); err != nil {
			errs = append(errs, err)
		}
	}
	if spec.HadoopConfigMap != nil {
		if err := validateConfigMapName(root.Child("hadoopConfigMap"), *spec.HadoopConfigMap); err != nil {
			errs = append(errs, err)
		}
	}

	errs = append(errs, validateConfigMapList(root.Child("driver", "configMaps"), spec.Driver.ConfigMaps)...)
	errs = append(errs, validateConfigMapList(root.Child("executor", "configMaps"), spec.Executor.ConfigMaps)...)

	return errors.Join(errs...)
}

func validateConfigMapList(path *field.Path, configMaps []v1beta2.NamePath) []error {
	var errs []error
	// One ConfigMap may be mounted at several paths, but a path holds a single volume, so
	// the name is free to repeat and the path is not.
	seen := make(map[string]bool, len(configMaps))
	for i, configMap := range configMaps {
		if err := validateConfigMapName(path.Index(i).Child("name"), configMap.Name); err != nil {
			errs = append(errs, err)
		}
		if seen[configMap.Path] {
			errs = append(errs, fmt.Errorf("%s has duplicate mount path %q", path.Index(i).Child("path"), configMap.Path))
		}
		seen[configMap.Path] = true
	}
	return errs
}

func validateConfigMapName(path *field.Path, name string) error {
	if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
		return fmt.Errorf("%s has invalid ConfigMap name %q: %s", path, name, strings.Join(errs, ", "))
	}
	return nil
}
