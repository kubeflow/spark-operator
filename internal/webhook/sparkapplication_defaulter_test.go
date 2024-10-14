/*
Copyright 2018 Google LLC

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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	// "github.com/evanphx/json-patch"
	// "github.com/stretchr/testify/assert"
	// corev1 "k8s.io/api/core/v1"
	// "k8s.io/apimachinery/pkg/api/resource"
	// metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	// "gomodules.xyz/jsonpatch/v2"

	"github.com/ghodss/yaml"
	"github.com/kubeflow/spark-operator/api/v1beta2"

	// "github.com/kubeflow/spark-operator/pkg/common"
	"github.com/evanphx/json-patch"
	"github.com/getkin/kin-openapi/openapi3"
)



func runWebhook(rawJSON []byte) ([]byte, error) {
	request := new(admission.Request)
	request.Object = runtime.RawExtension{Raw: rawJSON}
	webhook := admission.WithCustomDefaulter(runtime.NewScheme(), &v1beta2.SparkApplication{}, NewSparkApplicationDefaulter())
	response := webhook.Handle(context.TODO(), *request)

	data_patches, encode_err := json.Marshal(response.Patches)
	if encode_err != nil {
		return nil, fmt.Errorf("Failed to encode patches: %v", encode_err)
	}

	decoded_patch, decode_err := jsonpatch.DecodePatch(data_patches)
	if decode_err != nil {
		return nil, fmt.Errorf("Failed to decode patches: %v", decode_err)
	}
	rawJSON, apply_err := decoded_patch.Apply(rawJSON)
	if apply_err != nil {
		return nil, fmt.Errorf("Failed to apply patches: %v", apply_err)
	}

	return rawJSON, nil
}

// func getFieldFromMapInterface(m interface{}, field string) (map[string]interface{}, error) {
// 	var unmarshalledData map[string]interface{}
// 	parse_err := yaml.Unmarshal(yamlData, &unmarshalledData)
// 	if parse_err != nil {
// 		return "", fmt.Errorf("Failed to parse YAML: %v", parse_err)
// 	}

// 	return unmarshalledData[field].(string), nil
// 	if m, ok = data.(map[string]interface{}); ok {
//         // Iterate over the map
//         for key, value := range m {
//             fmt.Printf("Key: %s, Value: %v\n", key, value)
//         }
//     } else {
//         fmt.Println("Provided data is not a map[string]interface{}")
//     }
// }

func readField(data interface{}, field_name string) (interface{}, error) {
	if m, ok := data.(map[string]interface{}); ok {
		return m[field_name], nil
	}
	return nil, fmt.Errorf("Failed to read field")
}

func readIndex(data interface{}, index int) (interface{}, error) {
	if m, ok := data.([]interface{}); ok {
		return m[index], nil
	}
	return nil, fmt.Errorf("Failed to read field")
}

func convertMapKeys(i interface{},  field ...any) (interface{}, error) {
	for _, field := range fields {
		switch x := field.(type) {
		case string:
			if m, ok := data.([string]interface{}); ok {
				i = m[field], nil
			}
		case int:
			if m, ok := data.([]interface{}); ok {
				i = m[field], nil
			}
		return i
}

func validateAgainstCRD(rawJSON []byte) error {
	crd_path := filepath.Join("..", "..", "charts/spark-operator-chart/crds/sparkoperator.k8s.io_sparkapplications.yaml")
	yamlData, read_crd_err := os.ReadFile(crd_path)
	if read_crd_err != nil {
		return fmt.Errorf("Failed to read CRD: %v", read_crd_err)
	}

	var unmarshalledData interface{}
	parse_crd := yaml.Unmarshal(yamlData, &unmarshalledData)
	if parse_crd != nil {
		return fmt.Errorf("Failed to unmarshal CRD: %v", parse_crd)
	}

	spec, err := readField(unmarshalledData, "spec")
	if err != nil {
		return fmt.Errorf("Failed to read 'spec' field: %v", err)
	}

	versions, err := readField(spec, "versions")
	if err != nil {
		return fmt.Errorf("Failed to read 'versions' field: %v", err)
	}

	version, err := readIndex(versions, 0)
	if err != nil {
		return fmt.Errorf("Failed to read index 0: %v", err)
	}

	schema, err := readField(version, "schema")
	if err != nil {
		return fmt.Errorf("Failed to read 'schema' field: %v", err)
	}

	openapi3Schema, err := readField(schema, "openAPIV3Schema")
	if err != nil {
		return fmt.Errorf("Failed to read 'openAPIV3Schema' field: %v", err)
	}

	openapi3SchemaJSON, marshal_err := json.Marshal(openapi3Schema)
    if marshal_err != nil {
        return fmt.Errorf("Failed to marshal spec to JSON: %v", marshal_err)
    }

	schema2 := openapi3.Schema{} 
	parse_as_schema_err := json.Unmarshal(openapi3SchemaJSON, &schema2)
	if parse_as_schema_err != nil {
		return fmt.Errorf("Failed to unmarshal CRD: %v", parse_as_schema_err)
	}

	var data map[string]interface{}
	unmarshal_json_err := json.Unmarshal([]byte(rawJSON), &data)
	if unmarshal_json_err != nil {
		return fmt.Errorf("Failed to unmarshal JSON: %v", unmarshal_json_err)
	}

	return schema2.VisitJSON(data)
}

func TestDefaultSparkApplicationSparkUIOptions(t *testing.T) {
	rawJSON := []byte(`{
		"apiVersion": "sparkoperator.k8s.io/v1beta2",
		"kind": "SparkApplication",
		"metadata": {
		  "name": "test"
		},
		"spec": {
		  "image": "dummy",
		  "mainApplicationFile": "local:///dummy.py",
		  "mode": "cluster",
		  "type": "Python",
		  "sparkUIOptions": {
			"ingressTLS": [
			  {
				"hosts": [
				  "*.dummy"
				],
				"secretName": "spark-ui-tls-secret"
			  }
			]
		  },
		  "sparkVersion": "3.5.0",
		  "driver": {},
		  "executor": {}
		}
	  }`)

	err := validateAgainstCRD(rawJSON)
	if err != nil {
		t.Fatal(err)
	}

	// ctx := context.TODO()
	// request := new(admission.Request)
	// request.Object = runtime.RawExtension{Raw: rawJSON}
	// webhook := admission.WithCustomDefaulter(runtime.NewScheme(), &v1beta2.SparkApplication{}, NewSparkApplicationDefaulter())
	// response := webhook.Handle(ctx, *request)

	// data_patches, encode_err := json.Marshal(response.Patches)
	// if encode_err != nil {
	// 	t.Fatal(encode_err)
	// }
	// decoded_patch, err := jsonpatch.DecodePatch(data_patches)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// rawJSON, err = decoded_patch.Apply(rawJSON)
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// var marshalledJSON map[string]interface{}
	// _ = json.Unmarshal(rawJSON, &marshalledJSON)

	// yamlData, _ := os.ReadFile("/home/tomnewton/spark_operator_private/charts/spark-operator-chart/crds/schema.yaml")

	// schema := openapi3.Schema{} 
	// err = yaml.Unmarshal(yamlData, &schema)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// logger.Info("Schema", "schema", schema)

	// var data map[string]interface{}

	// err = json.Unmarshal([]byte(rawJSON), &data)
	// if err != nil {
	// 	t.Fatalf("Failed to parse JSON: %v", err)
	// }

	// err = schema.VisitJSON(data)
	// if err != nil {
	// 	t.Fatalf("Failed to validate JSON: %v", err)
	// }

}

// func TestPatchSparkPod_OwnerReference(t *testing.T) {
// 	app := &v1beta2.SparkApplication{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Name: "spark-test",
// 			UID:  "spark-test-1",
// 		},
// 	}

// 	pod := &corev1.Pod{
// 		ObjectMeta: metav1.ObjectMeta{
// 			Name: "spark-driver",
// 			Labels: map[string]string{
// 				common.LabelSparkRole:               common.SparkRoleDriver,
// 				common.LabelLaunchedBySparkOperator: "true",
// 			},
// 		},
// 		Spec: corev1.PodSpec{
// 			Containers: []corev1.Container{
// 				{
// 					Name:  common.SparkDriverContainerName,
// 					Image: "spark-driver:latest",
// 				},
// 			},
// 		},
// 	}

// 	// Test patching a pod without existing OwnerReference and Volume.
// 	modifiedPod, err := getModifiedPod(pod, app)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	assert.Len(t, modifiedPod.OwnerReferences, 1)

// 	// Test patching a pod with existing OwnerReference and Volume.
// 	pod.OwnerReferences = append(pod.OwnerReferences, metav1.OwnerReference{Name: "owner-reference1"})

// 	modifiedPod, err = getModifiedPod(pod, app)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	assert.Len(t, modifiedPod.OwnerReferences, 2)
// }
