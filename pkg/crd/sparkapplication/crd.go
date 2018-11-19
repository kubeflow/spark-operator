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

package sparkapplication

import (
	"reflect"

	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
)

// CRD metadata.
const (
	Plural    = "sparkapplications"
	Singular  = "sparkapplication"
	ShortName = "sparkapp"
	Group     = sparkoperator.GroupName
	Version   = v1alpha1.Version
	FullName  = Plural + "." + Group
)

func GetCRD() *apiextensionsv1beta1.CustomResourceDefinition {
	return &apiextensionsv1beta1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: FullName,
		},
		Spec: apiextensionsv1beta1.CustomResourceDefinitionSpec{
			Group:   Group,
			Version: Version,
			Scope:   apiextensionsv1beta1.NamespaceScoped,
			Names: apiextensionsv1beta1.CustomResourceDefinitionNames{
				Plural:     Plural,
				Singular:   Singular,
				ShortNames: []string{ShortName},
				Kind:       reflect.TypeOf(v1alpha1.SparkApplication{}).Name(),
			},
			Validation: getCustomResourceValidation(),
		},
	}
}

func getCustomResourceValidation() *apiextensionsv1beta1.CustomResourceValidation {
	return &apiextensionsv1beta1.CustomResourceValidation{
		OpenAPIV3Schema: &apiextensionsv1beta1.JSONSchemaProps{
			Properties: map[string]apiextensionsv1beta1.JSONSchemaProps{
				"spec": {
					Properties: map[string]apiextensionsv1beta1.JSONSchemaProps{
						"type": {
							Enum: []apiextensionsv1beta1.JSON{
								{Raw: []byte(`"Java"`)},
								{Raw: []byte(`"Scala"`)},
								{Raw: []byte(`"Python"`)},
								{Raw: []byte(`"R"`)},
							},
						},
						"mode": {
							Enum: []apiextensionsv1beta1.JSON{
								{Raw: []byte(`"cluster"`)},
								{Raw: []byte(`"client"`)},
							},
						},
						"driver": {
							Properties: map[string]apiextensionsv1beta1.JSONSchemaProps{
								"cores": {
									Type:             "number",
									Minimum:          float64Ptr(0),
									ExclusiveMinimum: true,
								},
								"podName": {
									Pattern: "[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*",
								},
							},
						},
						"executor": {
							Properties: map[string]apiextensionsv1beta1.JSONSchemaProps{
								"cores": {
									Type:             "number",
									Minimum:          float64Ptr(0),
									ExclusiveMinimum: true,
								},
								"instances": {
									Type:    "integer",
									Minimum: float64Ptr(1),
								},
							},
						},
						"deps": {
							Properties: map[string]apiextensionsv1beta1.JSONSchemaProps{
								"downloadTimeout": {
									Type:    "integer",
									Minimum: float64Ptr(1),
								},
								"maxSimultaneousDownloads": {
									Type:    "integer",
									Minimum: float64Ptr(1),
								},
							},
						},
						"restartPolicy": {
							Properties: map[string]apiextensionsv1beta1.JSONSchemaProps{
								"type": {
									Enum: []apiextensionsv1beta1.JSON{
										{Raw: []byte(`"Never"`)},
										{Raw: []byte(`"OnFailure"`)},
										{Raw: []byte(`"Always"`)},
									},
								},
								"onSubmissionFailureRetries": {
									Type:    "integer",
									Minimum: float64Ptr(0),
								},
								"onFailureRetries": {
									Type:    "integer",
									Minimum: float64Ptr(0),
								},
								"onSubmissionFailureRetryInterval": {
									Type:    "integer",
									Minimum: float64Ptr(1),
								},
								"onFailureRetryInterval": {
									Type:    "integer",
									Minimum: float64Ptr(1),
								},
							},
						},
						"pythonVersion": {
							Enum: []apiextensionsv1beta1.JSON{
								{Raw: []byte(`"2"`)},
								{Raw: []byte(`"3"`)},
							},
						},
						"monitoring": {
							Properties: map[string]apiextensionsv1beta1.JSONSchemaProps{
								"prometheus": {
									Properties: map[string]apiextensionsv1beta1.JSONSchemaProps{
										"port": {
											Type:    "integer",
											Minimum: float64Ptr(1024),
											Maximum: float64Ptr(49151),
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func float64Ptr(f float64) *float64 {
	return &f
}
