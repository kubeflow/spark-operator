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

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
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

func TestIsContainerLocalFile(t *testing.T) {
	type testcase struct {
		file             string
		isContainerLocal bool
	}

	testFn := func(test testcase, t *testing.T) {
		isLocal, err := isContainerLocalFile(test.file)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, test.isContainerLocal, isLocal,
			"%s: expected %v got %v", test.file, test.isContainerLocal, isLocal)
	}

	testcases := []testcase{
		{file: "/path/to/file", isContainerLocal: false},
		{file: "file:///path/to/file", isContainerLocal: false},
		{file: "local:///path/to/file", isContainerLocal: true},
		{file: "https://localhost/path/to/file", isContainerLocal: false},
	}

	for _, test := range testcases {
		testFn(test, t)
	}
}

func TestHasNonContainerLocalFiles(t *testing.T) {
	type testcase struct {
		name                      string
		spec                      v1alpha1.SparkApplicationSpec
		hasNonContainerLocalFiles bool
	}

	testFn := func(test testcase, t *testing.T) {
		yes, err := hasNonContainerLocalFiles(test.spec)
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, test.hasNonContainerLocalFiles, yes, "%s: expected %v got %v",
			test.name, test.hasNonContainerLocalFiles, yes)
	}

	mainAppFile := "/path/to/main/app/file"
	containerLocalMainAppFile := "local:///path/to/main/app/file"
	testcases := []testcase{
		{
			name: "application with submission local main file",
			spec: v1alpha1.SparkApplicationSpec{
				MainApplicationFile: &mainAppFile,
			},
			hasNonContainerLocalFiles: true,
		},
		{
			name: "application with container local main file",
			spec: v1alpha1.SparkApplicationSpec{
				MainApplicationFile: &containerLocalMainAppFile,
			},
			hasNonContainerLocalFiles: false,
		},
		{
			name: "application with remote jars",
			spec: v1alpha1.SparkApplicationSpec{
				Deps: v1alpha1.Dependencies{
					Jars: []string{"https://localhost/path/to/foo.jar"},
				},
			},
			hasNonContainerLocalFiles: true,
		},
		{
			name: "application with container-local jars",
			spec: v1alpha1.SparkApplicationSpec{
				Deps: v1alpha1.Dependencies{
					Jars: []string{"local:///path/to/foo.jar"},
				},
			},
			hasNonContainerLocalFiles: false,
		},
	}

	for _, test := range testcases {
		testFn(test, t)
	}
}

func TestValidateSpec(t *testing.T) {
	type testcase struct {
		name                   string
		spec                   v1alpha1.SparkApplicationSpec
		expectsValidationError bool
	}

	testFn := func(test testcase, t *testing.T) {
		err := validateSpec(test.spec)
		if test.expectsValidationError {
			assert.True(t, err != nil, "%s: expected error got nothing", test.name)
		} else {
			assert.True(t, err == nil, "%s: did not expect error got %v", test.name, err)
		}
	}

	image := "spark"
	remoteMainAppFile := "https://localhost/path/to/main/app/file"
	containerLocalMainAppFile := "local:///path/to/main/app/file"
	testcases := []testcase{
		{
			name: "application with spec.image set",
			spec: v1alpha1.SparkApplicationSpec{
				Image: &image,
			},
			expectsValidationError: false,
		},
		{
			name: "application with no spec.image and spec.driver.image",
			spec: v1alpha1.SparkApplicationSpec{
				Executor: v1alpha1.ExecutorSpec{
					SparkPodSpec: v1alpha1.SparkPodSpec{
						Image: &image,
					},
				},
			},
			expectsValidationError: true,
		},
		{
			name: "application with no spec.image and spec.executor.image",
			spec: v1alpha1.SparkApplicationSpec{
				Driver: v1alpha1.DriverSpec{
					SparkPodSpec: v1alpha1.SparkPodSpec{
						Image: &image,
					},
				},
			},
			expectsValidationError: true,
		},
		{
			name: "application with remote main file and no init-container",
			spec: v1alpha1.SparkApplicationSpec{
				MainApplicationFile: &remoteMainAppFile,
			},
			expectsValidationError: true,
		},
		{
			name: "application with remote main file and spec.image",
			spec: v1alpha1.SparkApplicationSpec{
				Image:               &image,
				MainApplicationFile: &remoteMainAppFile,
			},
			expectsValidationError: false,
		},
		{
			name: "application with remote main file and spec.initContainerImage",
			spec: v1alpha1.SparkApplicationSpec{
				InitContainerImage:  &image,
				MainApplicationFile: &remoteMainAppFile,
				Driver: v1alpha1.DriverSpec{
					SparkPodSpec: v1alpha1.SparkPodSpec{
						Image: &image,
					},
				},
				Executor: v1alpha1.ExecutorSpec{
					SparkPodSpec: v1alpha1.SparkPodSpec{
						Image: &image,
					},
				},
			},
			expectsValidationError: false,
		},
		{
			name: "application with container local main file and no init-container",
			spec: v1alpha1.SparkApplicationSpec{
				MainApplicationFile: &containerLocalMainAppFile,
				Driver: v1alpha1.DriverSpec{
					SparkPodSpec: v1alpha1.SparkPodSpec{
						Image: &image,
					},
				},
				Executor: v1alpha1.ExecutorSpec{
					SparkPodSpec: v1alpha1.SparkPodSpec{
						Image: &image,
					},
				},
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

	assert.Equal(t, app.Name, "example")
	assert.Equal(t, *app.Spec.MainClass, "org.examples.SparkExample")
	assert.Equal(t, *app.Spec.MainApplicationFile, "local:///path/to/example.jar")
	assert.Equal(t, *app.Spec.Driver.Image, "spark")
	assert.Equal(t, *app.Spec.Executor.Image, "spark")
	assert.Equal(t, int(*app.Spec.Executor.Instances), 1)
}

func TestHandleHadoopConfiguration(t *testing.T) {
	configMap, err := buildHadoopConfigMap("test", "testdata/hadoop-conf")
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, configMap.Name, "test-hadoop-config")
	assert.Equal(t, len(configMap.BinaryData), 1)
	assert.Equal(t, len(configMap.Data), 1)
	assert.True(t, strings.Contains(configMap.Data["core-site.xml"], "fs.gs.impl"))
}
