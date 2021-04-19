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
	"io"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
	"os"
	"time"

	"github.com/pkg/errors"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

func CreateService(kubeClient kubernetes.Interface, ns string, relativePath string) (finalizerFn, error) {
	finalizerFn := func() error {
		return DeleteService(kubeClient, ns, relativePath)
	}
	service, err := parseServiceYaml(relativePath)
	if err != nil {
		return finalizerFn, err
	}

	service.Namespace = ns

	_, err = kubeClient.CoreV1().Services(ns).Get(context.TODO(), service.Name, metav1.GetOptions{})

	if err == nil {
		// Service already exists -> Update
		_, err = kubeClient.CoreV1().Services(ns).Update(context.TODO(), service, metav1.UpdateOptions{})
		if err != nil {
			return finalizerFn, err
		}
	} else {
		// Service doesn't exists -> Create
		_, err = kubeClient.CoreV1().Services(ns).Create(context.TODO(), service, metav1.CreateOptions{})
		if err != nil {
			return finalizerFn, err
		}
	}

	return finalizerFn, err
}

func WaitForServiceReady(kubeClient kubernetes.Interface, namespace string, serviceName string) error {
	err := wait.Poll(time.Second, time.Minute*5, func() (bool, error) {
		endpoints, err := getEndpoints(kubeClient, namespace, serviceName)
		if err != nil {
			return false, err
		}
		if len(endpoints.Subsets) != 0 && len(endpoints.Subsets[0].Addresses) > 0 {
			return true, nil
		}
		return false, nil
	})
	return err
}

func getEndpoints(kubeClient kubernetes.Interface, namespace, serviceName string) (*v1.Endpoints, error) {
	endpoints, err := kubeClient.CoreV1().Endpoints(namespace).Get(context.TODO(), serviceName, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("requesting endpoints for service %v failed", serviceName))
	}
	return endpoints, nil
}

func parseServiceYaml(relativePath string) (*v1.Service, error) {
	var manifest *os.File
	var err error

	var service v1.Service
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

		if out.GetKind() == "Service" {
			var marshaled []byte
			marshaled, err = out.MarshalJSON()
			json.Unmarshal(marshaled, &service)
			break
		}
	}

	if err != io.EOF && err != nil {
		return nil, err
	}
	return &service, nil
}

func DeleteService(kubeClient kubernetes.Interface, name string, namespace string) error {
	return kubeClient.CoreV1().Services(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
}
