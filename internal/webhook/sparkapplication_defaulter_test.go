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

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/ghodss/yaml"
	"github.com/kubeflow/spark-operator/api/v1beta2"

	jsonpatch "github.com/evanphx/json-patch"
	"github.com/getkin/kin-openapi/openapi3"
)

func runDefaultingWebhook(json_data []byte) ([]byte, error) {
	request := new(admission.Request)
	request.Object = runtime.RawExtension{Raw: json_data}
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
	json_data, apply_err := decoded_patch.Apply(json_data)
	if apply_err != nil {
		return nil, fmt.Errorf("Failed to apply patches: %v", apply_err)
	}

	return json_data, nil
}

func getField(i interface{}, fields ...any) (interface{}, error) {
	for _, field := range fields {
		switch x := field.(type) {
		case string:
			if m, ok := i.(map[string]interface{}); ok {
				i = m[x]
			}
		case int:
			if m, ok := i.([]interface{}); ok {
				i = m[x]
			}
		}
	}
	return i, nil
}

func getSchemaFromCRD() (*openapi3.Schema, error) {
	crdPath := filepath.Join("..", "..", "charts/spark-operator-chart/crds/sparkoperator.k8s.io_sparkapplications.yaml")
	yamlData, readCrdErr := os.ReadFile(crdPath)
	if readCrdErr != nil {
		return nil, fmt.Errorf("Failed to read CRD: %v", readCrdErr)
	}

	var crdInterface interface{}
	crdInterfaceErr := yaml.Unmarshal(yamlData, &crdInterface)
	if crdInterfaceErr != nil {
		return nil, fmt.Errorf("Failed to unmarshal CRD: %v", crdInterfaceErr)
	}
	schemaInterface, schemaInterfaceErr := getField(crdInterface, "spec", "versions", 0, "schema", "openAPIV3Schema")
	if schemaInterfaceErr != nil {
		return nil, fmt.Errorf("Failed to get field: %v", schemaInterfaceErr)
	}

	schemaJson, schemaJsonErr := json.Marshal(schemaInterface)
	if schemaJsonErr != nil {
		return nil, fmt.Errorf("Failed to convert to json: %v", schemaJsonErr)
	}

	schema := openapi3.Schema{}
	schemaErr := json.Unmarshal(schemaJson, &schema)
	if schemaErr != nil {
		return nil, fmt.Errorf("Failed to unmarshal CRD: %v", schemaErr)
	}

	return &schema, nil
}

func validateAgainstCRD(jsonData []byte) error {
	var data map[string]interface{}
	unmarshalJsonErr := json.Unmarshal([]byte(jsonData), &data)
	if unmarshalJsonErr != nil {
		return fmt.Errorf("Failed to unmarshal JSON: %v", unmarshalJsonErr)
	}

	schema, err := getSchemaFromCRD()
	if err != nil {
		return fmt.Errorf("Failed to get schema from CRD: %v", err)
	}
	return schema.VisitJSON(data)
}

func TestDefaultSparkApplicationSparkUIOptions(t *testing.T) {
	jsonData := []byte(`{
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

	assert.NoError(t, validateAgainstCRD(jsonData), "Test input must be valid against CRD")
	defaultedData, err := runDefaultingWebhook(jsonData)
	assert.NoError(t, err)
	assert.NoError(t, validateAgainstCRD(defaultedData), "Ensure still valid against CRD after defaulting webhook")
}
