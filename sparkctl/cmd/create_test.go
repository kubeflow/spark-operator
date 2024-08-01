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

package cmd

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kubeflow/spark-operator/api/v1beta2"
)

func TestIsLocalFile(t *testing.T) {
	type testcase struct {
		file    string
		isLocal bool
	}

	testFn := func(test testcase, t *testing.T) {
		isLocal, err := isLocalFile(test.file)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, test.isLocal, isLocal, "%s: expected %v got %v", test.file, test.isLocal, isLocal)
	}

	testcases := []testcase{
		{file: "/path/to/file", isLocal: true},
		{file: "file:///path/to/file", isLocal: true},
		{file: "local:///path/to/file", isLocal: false},
		{file: "http://localhost/path/to/file", isLocal: false},
	}

	for _, test := range testcases {
		testFn(test, t)
	}
}

func TestFilterLocalFiles(t *testing.T) {
	files := []string{
		"path/to/file",
		"/path/to/file",
		"file:///file/to/path",
		"http://localhost/path/to/file",
		"hdfs://localhost/path/to/file",
		"gs://bucket/path/to/file",
	}

	expected := []string{
		"path/to/file",
		"/path/to/file",
		"file:///file/to/path",
	}

	actual, err := filterLocalFiles(files)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, expected, actual)
}

func TestValidateSpec(t *testing.T) {
	type testcase struct {
		name                   string
		spec                   v1beta2.SparkApplicationSpec
		expectsValidationError bool
	}

	testFn := func(test testcase, t *testing.T) {
		err := validateSpec(test.spec)
		if test.expectsValidationError {
			assert.Error(t, err, "%s: expected error got nothing", test.name)
		} else {
			assert.NoError(t, err, "%s: did not expect error got %v", test.name, err)
		}
	}

	image := "spark"
	remoteMainAppFile := "https://localhost/path/to/main/app/file"
	containerLocalMainAppFile := "local:///path/to/main/app/file"
	testcases := []testcase{
		{
			name: "application with spec.image set",
			spec: v1beta2.SparkApplicationSpec{
				Image: &image,
			},
			expectsValidationError: false,
		},
		{
			name: "application with no spec.image and spec.driver.image",
			spec: v1beta2.SparkApplicationSpec{
				Executor: v1beta2.ExecutorSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Image: &image,
					},
				},
			},
			expectsValidationError: true,
		},
		{
			name: "application with no spec.image and spec.executor.image",
			spec: v1beta2.SparkApplicationSpec{
				Driver: v1beta2.DriverSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Image: &image,
					},
				},
			},
			expectsValidationError: true,
		},
		{
			name: "application with no spec.image but spec.driver.image and spec.executor.image",
			spec: v1beta2.SparkApplicationSpec{
				MainApplicationFile: &containerLocalMainAppFile,
				Driver: v1beta2.DriverSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Image: &image,
					},
				},
				Executor: v1beta2.ExecutorSpec{
					SparkPodSpec: v1beta2.SparkPodSpec{
						Image: &image,
					},
				},
			},
			expectsValidationError: false,
		},
		{
			name: "application with remote main file and spec.image",
			spec: v1beta2.SparkApplicationSpec{
				Image:               &image,
				MainApplicationFile: &remoteMainAppFile,
			},
			expectsValidationError: false,
		},
	}

	for _, test := range testcases {
		testFn(test, t)
	}
}

func TestLoadFromYAML(t *testing.T) {
	app, err := loadFromYAML("testdata/test-app.yaml")
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "example", app.Name)
	assert.Equal(t, "org.examples.SparkExample", *app.Spec.MainClass)
	assert.Equal(t, "local:///path/to/example.jar", *app.Spec.MainApplicationFile)
	assert.Equal(t, "spark", *app.Spec.Driver.Image)
	assert.Equal(t, "spark", *app.Spec.Executor.Image)
	assert.Equal(t, 1, int(*app.Spec.Executor.Instances))
}

func TestHandleHadoopConfiguration(t *testing.T) {
	configMap, err := buildHadoopConfigMap("test", "testdata/hadoop-conf")
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "test-hadoop-config", configMap.Name)
	assert.Len(t, configMap.BinaryData, 1)
	assert.Len(t, configMap.Data, 1)
	assert.True(t, strings.Contains(configMap.Data["core-site.xml"], "fs.gs.impl"))
}
