/*
Copyright 2026 The Kubeflow Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"encoding/json"
	"fmt"
	"strings"

	"k8s.io/klog/v2"
	"k8s.io/kube-openapi/pkg/builder3"
	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/spec3"
	"k8s.io/kube-openapi/pkg/validation/spec"

	sparkv1alpha1 "github.com/kubeflow/spark-operator/v2/api/v1alpha1"
	sparkv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
)

// Generate Kubeflow Spark Operator OpenAPI specification.
func main() {
	var definitions = map[string]common.OpenAPIDefinition{}

	refCallback := func(name string) spec.Ref {
		return spec.MustCreateRef(
			"#/components/schemas/" + common.EscapeJsonPointer(swaggify(name)),
		)
	}

	// Load definitions from both API versions
	for k, v := range sparkv1alpha1.GetOpenAPIDefinitions(refCallback) {
		definitions[k] = v
	}

	for k, v := range sparkv1beta2.GetOpenAPIDefinitions(refCallback) {
		definitions[k] = v
	}

	// OpenAPI generator incorrectly creates models if enum doesn't have
	// a default value. Remove the empty default for enum properties.
	for defName, val := range definitions {
		if defName == "k8s.io/apimachinery/pkg/apis/meta/v1.InternalEvent" {
			delete(definitions, defName)
			continue
		}

		for property, schema := range val.Schema.Properties {
			if schema.Enum != nil && schema.Default == "" {
				schema.Default = nil
				val.Schema.SetProperty(property, schema)
			}
		}

		definitions[defName] = val
	}

	config := &common.OpenAPIV3Config{
		Info: &spec.Info{
			InfoProps: spec.InfoProps{
				Title:   "Kubeflow Spark Operator OpenAPI Spec",
				Version: "unversioned",
			},
		},
		Definitions: definitions,
		GetDefinitionName: func(name string) (string, spec.Extensions) {
			return swaggify(name), nil
		},
	}

	// Build OpenAPI v3 schemas and their dependencies.
	names := make([]string, 0, len(definitions))
	for name := range definitions {
		names = append(names, name)
	}

	defs, err := builder3.BuildOpenAPIDefinitionsForResources(config, names...)
	if err != nil {
		klog.Fatal(err.Error())
	}

	openAPIV3 := &spec3.OpenAPI{
		Version: "3.0.0",
		Info: &spec.Info{
			InfoProps: spec.InfoProps{
				Title:   "Kubeflow Spark Operator OpenAPI Spec",
				Version: "unversioned",
			},
		},
		Paths: &spec3.Paths{
			Paths: map[string]*spec3.Path{},
		},
		Components: &spec3.Components{
			Schemas: defs,
		},
	}

	jsonBytes, err := json.MarshalIndent(openAPIV3, "", "  ")
	if err != nil {
		klog.Fatal(err.Error())
	}
	fmt.Println(string(jsonBytes))
}

func swaggify(name string) string {
	name = strings.ReplaceAll(name, "github.com/kubeflow/spark-operator/v2/api/", "spark.")
	name = strings.ReplaceAll(name, "k8s.io", "io.k8s")
	name = strings.ReplaceAll(name, "/", ".")
	return name
}
