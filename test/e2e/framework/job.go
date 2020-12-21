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
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"k8s.io/apimachinery/pkg/util/wait"
	"os"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
)

func MakeJob(pathToYaml string) (*batchv1.Job, error) {
	job, err := parseJobYaml(pathToYaml)
	if err != nil {
		return nil, err
	}

	return job, nil
}

func CreateJob(kubeClient kubernetes.Interface, namespace string, job *batchv1.Job) error {
	_, err := kubeClient.BatchV1().Jobs(namespace).Create(context.TODO(), job, metav1.CreateOptions{})
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to create job %s", job.Name))
	}
	return nil
}

func DeleteJob(kubeClient kubernetes.Interface, namespace, name string) error {
	deleteProp := metav1.DeletePropagationForeground
	return kubeClient.BatchV1().Jobs(namespace).Delete(
		context.TODO(),
		name,
		metav1.DeleteOptions{PropagationPolicy: &deleteProp},
	)
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

func WaitUntilJobCompleted(kubeClient kubernetes.Interface, namespace, name string, timeout time.Duration) error {
	return wait.Poll(time.Second, timeout, func() (bool, error) {
		job, _ := kubeClient.
			BatchV1().Jobs(namespace).
			Get(context.TODO(), name, metav1.GetOptions{})

		if job.Status.Succeeded == 1 {
			return true, nil
		} else {
			return false, nil
		}
	})
}
