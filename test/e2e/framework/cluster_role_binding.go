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
	"io"
	"os"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
)

func CreateClusterRoleBinding(kubeClient kubernetes.Interface, relativePath string) (finalizerFn, error) {
	finalizerFn := func() error {
		return DeleteClusterRoleBinding(kubeClient, relativePath)
	}
	clusterRoleBinding, err := parseClusterRoleBindingYaml(relativePath)
	if err != nil {
		return finalizerFn, err
	}

	_, err = kubeClient.RbacV1().ClusterRoleBindings().Get(context.TODO(), clusterRoleBinding.Name, metav1.GetOptions{})

	if err == nil {
		// ClusterRoleBinding already exists -> Update
		_, err = kubeClient.RbacV1().ClusterRoleBindings().Update(context.TODO(), clusterRoleBinding, metav1.UpdateOptions{})
		if err != nil {
			return finalizerFn, err
		}
	} else {
		// ClusterRoleBinding doesn't exists -> Create
		_, err = kubeClient.RbacV1().ClusterRoleBindings().Create(context.TODO(), clusterRoleBinding, metav1.CreateOptions{})
		if err != nil {
			return finalizerFn, err
		}
	}

	return finalizerFn, err
}

func DeleteClusterRoleBinding(kubeClient kubernetes.Interface, relativePath string) error {
	clusterRoleBinding, err := parseClusterRoleYaml(relativePath)
	if err != nil {
		return err
	}

	if err := kubeClient.RbacV1().ClusterRoleBindings().Delete(context.TODO(), clusterRoleBinding.Name, metav1.DeleteOptions{}); err != nil {
		return err
	}

	return nil
}

func parseClusterRoleBindingYaml(relativePath string) (*rbacv1.ClusterRoleBinding, error) {
	var manifest *os.File
	var err error

	var clusterRoleBinding rbacv1.ClusterRoleBinding
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

		if out.GetKind() == "ClusterRoleBinding" {
			var marshaled []byte
			marshaled, err = out.MarshalJSON()
			json.Unmarshal(marshaled, &clusterRoleBinding)
			break
		}
	}

	if err != io.EOF && err != nil {
		return nil, err
	}
	return &clusterRoleBinding, nil
}
