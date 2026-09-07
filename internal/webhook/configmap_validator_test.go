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
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

func TestValidateConfigMaps_Success(t *testing.T) {
	spec := &v1beta2.SparkApplicationSpec{
		SparkConfigMap:  ptr.To("spark-conf"),
		HadoopConfigMap: ptr.To("hadoop-conf"),
		Driver: v1beta2.DriverSpec{
			SparkPodSpec: v1beta2.SparkPodSpec{
				ConfigMaps: []v1beta2.NamePath{{Name: "driver-cm", Path: "/etc/driver"}},
			},
		},
		Executor: v1beta2.ExecutorSpec{
			SparkPodSpec: v1beta2.SparkPodSpec{
				ConfigMaps: []v1beta2.NamePath{{Name: "executor-cm", Path: "/etc/executor"}},
			},
		},
	}

	if err := validateConfigMaps(spec, field.NewPath("spec")); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
}

func TestValidateConfigMaps_InvalidSparkConfigMapName(t *testing.T) {
	spec := &v1beta2.SparkApplicationSpec{SparkConfigMap: ptr.To("Invalid_Name")}

	err := validateConfigMaps(spec, field.NewPath("spec"))
	if err == nil || !strings.Contains(err.Error(), "spec.sparkConfigMap") {
		t.Fatalf("expected invalid sparkConfigMap name error, got %v", err)
	}
}

func TestValidateConfigMaps_InvalidHadoopConfigMapName(t *testing.T) {
	spec := &v1beta2.SparkApplicationSpec{HadoopConfigMap: ptr.To("Invalid_Name")}

	err := validateConfigMaps(spec, field.NewPath("spec"))
	if err == nil || !strings.Contains(err.Error(), "spec.hadoopConfigMap") {
		t.Fatalf("expected invalid hadoopConfigMap name error, got %v", err)
	}
}

func TestValidateConfigMaps_DuplicateNameInDriver(t *testing.T) {
	spec := &v1beta2.SparkApplicationSpec{
		Driver: v1beta2.DriverSpec{
			SparkPodSpec: v1beta2.SparkPodSpec{
				ConfigMaps: []v1beta2.NamePath{
					{Name: "cm", Path: "/etc/one"},
					{Name: "cm", Path: "/etc/two"},
				},
			},
		},
	}

	err := validateConfigMaps(spec, field.NewPath("spec"))
	if err == nil || !strings.Contains(err.Error(), "duplicate ConfigMap name") {
		t.Fatalf("expected duplicate ConfigMap name error, got %v", err)
	}
}

func TestValidateConfigMaps_AggregatesAllErrors(t *testing.T) {
	spec := &v1beta2.SparkApplicationSpec{
		SparkConfigMap: ptr.To("Bad_Name"),
		Driver: v1beta2.DriverSpec{
			SparkPodSpec: v1beta2.SparkPodSpec{
				ConfigMaps: []v1beta2.NamePath{
					{Name: "cm", Path: "/etc/one"},
					{Name: "cm", Path: "/etc/two"},
				},
			},
		},
	}

	err := validateConfigMaps(spec, field.NewPath("spec"))
	if err == nil {
		t.Fatal("expected an aggregated error")
	}
	if !strings.Contains(err.Error(), "spec.sparkConfigMap") || !strings.Contains(err.Error(), "duplicate ConfigMap name") {
		t.Fatalf("expected both the sparkConfigMap and duplicate errors joined, got %v", err)
	}
}
