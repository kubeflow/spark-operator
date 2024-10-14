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
	"github.com/ghodss/yaml"

	// "github.com/kubeflow/spark-operator/pkg/common"
	"github.com/evanphx/json-patch"
	"github.com/getkin/kin-openapi/openapi3"
)


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

	ctx := context.TODO()
	request := new(admission.Request)
	request.Object = runtime.RawExtension{Raw: rawJSON}
	webhook := admission.WithCustomDefaulter(runtime.NewScheme(), &v1beta2.SparkApplication{}, NewSparkApplicationDefaulter())
	response := webhook.Handle(ctx, *request)

	data_patches, encode_err := json.Marshal(response.Patches)
	if encode_err != nil {
		t.Fatal(encode_err)
	}
	decoded_patch, err := jsonpatch.DecodePatch(data_patches)
	if err != nil {
		t.Fatal(err)
	}
	rawJSON, err = decoded_patch.Apply(rawJSON)
	if err != nil {
		t.Fatal(err)
	}
	// for _, patch := range response.Patches {
	// 	patch_data := []byte(patch.Json())
	// 	decoded_patch, err := jsonpatch.DecodePatch(patches)
	// 	if err != nil {
	// 		t.Fatal(err)
	// 	}
	// 	rawJSON, err = decoded_patch.Apply(rawJSON)
	// 	if err != nil {
	// 		t.Fatal(err)
	// 	}
	// }

	var marshalledJSON map[string]interface{}
	_ = json.Unmarshal(rawJSON, &marshalledJSON)

	yamlData, _ := os.ReadFile("/home/tomnewton/spark_operator_private/charts/spark-operator-chart/crds/schema.yaml")

	schema := openapi3.Schema{} 
	err = yaml.Unmarshal(yamlData, &schema)
	if err != nil {
		t.Fatal(err)
	}
	logger.Info("Schema", "schema", schema)

	var data map[string]interface{}

	err = json.Unmarshal([]byte(rawJSON), &data)
	if err != nil {
		t.Fatalf("Failed to parse JSON: %v", err)
	}

	err = schema.VisitJSON(data)
	if err != nil {
		t.Fatalf("Failed to validate JSON: %v", err)
	}

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
