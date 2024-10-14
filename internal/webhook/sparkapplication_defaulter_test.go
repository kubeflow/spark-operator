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
	"io"
	"os"
	"testing"

	// "github.com/evanphx/json-patch"
	// "github.com/stretchr/testify/assert"
	// corev1 "k8s.io/api/core/v1"
	// "k8s.io/apimachinery/pkg/api/resource"
	// metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	// "gomodules.xyz/jsonpatch/v2"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"gopkg.in/yaml.v2"

	// "github.com/kubeflow/spark-operator/pkg/common"
	jsonpatch "github.com/evanphx/json-patch"
	"github.com/getkin/kin-openapi/openapi3"
)

func readYamlAndConvertToJSON(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	var unmarshalledData map[string]interface{}
	fileData, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	err = yaml.Unmarshal(fileData, &unmarshalledData)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML: %w", err)
	}

	jsonBytes, err := json.Marshal(unmarshalledData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}

	return jsonBytes, nil
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
		  "mainApplicationFile": "dummy",
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
		  "sparkVersion": "3.5.0"
		}
	  }`)

	ctx := context.TODO()
	request := new(admission.Request)
	request.Object = runtime.RawExtension{Raw: rawJSON}
	webhook := admission.WithCustomDefaulter(runtime.NewScheme(), &v1beta2.SparkApplication{}, NewSparkApplicationDefaulter())
	response := webhook.Handle(ctx, *request)

	for _, patch := range response.Patches {
		jsonpatch.MergePatch(rawJSON, []byte(patch.Json()))
	}

	var marshalledJSON map[string]interface{}
	_ = json.Unmarshal(rawJSON, &marshalledJSON)

	schema := &openapi3.Schema{}
	// 	Type: openapi3.TypeObject,
	// 	Properties: map[string]*openapi3.SchemaRef{
	// 		"replicas": {Value: &openapi3.Schema{
	// 			Type:    "integer",
	// 			Minimum: openapi3.Float64Ptr(1),
	// 		}},
	// 		"image": {Value: &openapi3.Schema{
	// 			Type: openapi3.TypeString,
	// 		}},
	// 	},
	// 	Required: []string{"image"},
	// }

	jsonData := []byte(`{
		"replicas": 5,
		"image": "nginx:latest"
	}`)

	schema = &openapi3.Schema{}
	schema.VisitJSON(jsonData)

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
