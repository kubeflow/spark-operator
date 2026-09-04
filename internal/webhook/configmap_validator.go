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
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// validateConfigMaps rejects ConfigMap references no ConfigMap could satisfy, entries with no
// mount path, and mount paths that collide with another entry or with a path the webhook
// reserves for the Spark, Hadoop, or Prometheus ConfigMap. None of these surface before the
// API server rejects the pod the mutating webhook has already built.
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

	errs = append(errs, validateConfigMapList(root.Child("driver", "configMaps"), spec.Driver.ConfigMaps, reservedConfigMapMountPaths(spec, true))...)
	errs = append(errs, validateConfigMapList(root.Child("executor", "configMaps"), spec.Executor.ConfigMaps, reservedConfigMapMountPaths(spec, false))...)

	return errors.Join(errs...)
}

// reservedConfigMapMountPaths returns the mount paths the mutating webhook itself fills in for
// the given pod (driver if isDriver, else executor), keyed by the spec field responsible, so
// validateConfigMapList can reject a spec.{driver,executor}.configMaps entry that collides with
// one of them. It mirrors the same conditions addSparkConfigMap, addHadoopConfigMap, and
// addPrometheusConfig use to decide whether to add their volumeMount.
func reservedConfigMapMountPaths(spec *v1beta2.SparkApplicationSpec, isDriver bool) map[string]string {
	reserved := make(map[string]string, 3)
	if spec.SparkConfigMap != nil {
		reserved[common.DefaultSparkConfDir] = "sparkConfigMap"
	}
	if spec.HadoopConfigMap != nil {
		reserved[common.DefaultHadoopConfDir] = "hadoopConfigMap"
	}
	if prometheusConfigMapMounted(spec, isDriver) {
		reserved[common.PrometheusConfigMapMountPath] = "monitoring.prometheus"
	}
	return reserved
}

func prometheusConfigMapMounted(spec *v1beta2.SparkApplicationSpec, isDriver bool) bool {
	app := &v1beta2.SparkApplication{Spec: *spec}
	if !util.PrometheusMonitoringEnabled(app) || (util.HasMetricsPropertiesFile(app) && util.HasPrometheusConfigFile(app)) {
		return false
	}
	if isDriver {
		return util.ExposeDriverMetrics(app)
	}
	return util.ExposeExecutorMetrics(app)
}

func validateConfigMapList(path *field.Path, configMaps []v1beta2.NamePath, reserved map[string]string) []error {
	var errs []error
	// The same ConfigMap may be mounted more than once, at different paths, but two entries
	// mounted at the same path collide: the mutating webhook would emit two volumeMounts with
	// identical mountPaths, which the API server rejects. The same collision happens if an
	// entry reuses a path the webhook already reserves for the Spark, Hadoop, or Prometheus
	// ConfigMap it mounts on its own.
	seen := make(map[string]bool, len(configMaps))
	for i, configMap := range configMaps {
		if err := validateConfigMapName(path.Index(i).Child("name"), configMap.Name); err != nil {
			errs = append(errs, err)
		}

		pathField := path.Index(i).Child("path")
		switch {
		case configMap.Path == "":
			errs = append(errs, fmt.Errorf("%s must not be empty", pathField))
			continue
		case seen[configMap.Path]:
			errs = append(errs, fmt.Errorf("%s has duplicate mount path %q", pathField, configMap.Path))
		case reserved[configMap.Path] != "":
			errs = append(errs, fmt.Errorf("%s has mount path %q reserved for %s", pathField, configMap.Path, reserved[configMap.Path]))
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
