/*
Copyright 2018 Google LLC

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

package framework

import (
	"encoding/json"
	"io"
	"os"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
)

func CreateJob(kubeClient kubernetes.Interface, ns string, relativePath string) (finalizerFn, error) {
	finalizerFn := func() error {
		return DeleteJob(kubeClient, relativePath)
	}
	job, err := parseJobYaml(relativePath)
	if err != nil {
		return finalizerFn, err
	}

	job.Namespace = ns

	_, err = kubeClient.BatchV1().Jobs(ns).Get(job.Name, metav1.GetOptions{})

	if err == nil {
		// Job already exists -> Update
		_, err = kubeClient.BatchV1().Jobs(ns).Update(job)
		if err != nil {
			return finalizerFn, err
		}
	} else {
		// Job doesn't exists -> Create
		_, err = kubeClient.BatchV1().Jobs(ns).Create(job)
		if err != nil {
			return finalizerFn, err
		}
	}

	return finalizerFn, err
}

func DeleteJob(kubeClient kubernetes.Interface, relativePath string) error {
	job, err := parseClusterRoleYaml(relativePath)
	if err != nil {
		return err
	}

	if err := kubeClient.BatchV1().Jobs(job.Namespace).Delete(job.Name, &metav1.DeleteOptions{}); err != nil {
		return err
	}

	return nil
}

func parseJobYaml(relativePath string) (*batchv1.Job, error) {
	var manifest *os.File
	var err error

	var job batchv1.Job
	if manifest, err = PathToOSFile(relativePath); err != nil {
		return nil, err
	}

	decoder := yaml.NewYAMLOrJSONDecoder(manifest, 100)
	for {
		var out unstructured.Unstructured
		err = decoder.Decode(&out)
		if err != nil {
			// this would indicate it's malformed YAML.
			break
		}

		if out.GetKind() == "Job" {
			var marshaled []byte
			marshaled, err = out.MarshalJSON()
			json.Unmarshal(marshaled, &job)
			break
		}
	}

	if err != io.EOF && err != nil {
		return nil, err
	}
	return &job, nil
}
