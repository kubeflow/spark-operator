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
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	"os"
)

func CreateRoleBinding(kubeClient kubernetes.Interface, ns string, relativePath string) (finalizerFn, error) {
	finalizerFn := func() error {
		return DeleteRoleBinding(kubeClient, ns, relativePath)
	}
	roleBinding, err := parseRoleBindingYaml(relativePath)
	if err != nil {
		return finalizerFn, err
	}

	roleBinding.Namespace = ns

	_, err = kubeClient.RbacV1().RoleBindings(ns).Get(context.TODO(), roleBinding.Name, metav1.GetOptions{})

	if err == nil {
		// RoleBinding already exists -> Update
		_, err = kubeClient.RbacV1().RoleBindings(ns).Update(context.TODO(), roleBinding, metav1.UpdateOptions{})
		if err != nil {
			return finalizerFn, err
		}
	} else {
		// RoleBinding doesn't exists -> Create
		_, err = kubeClient.RbacV1().RoleBindings(ns).Create(context.TODO(), roleBinding, metav1.CreateOptions{})
		if err != nil {
			return finalizerFn, err
		}
	}

	return finalizerFn, err
}

func DeleteRoleBinding(kubeClient kubernetes.Interface, ns string, relativePath string) error {
	roleBinding, err := parseRoleBindingYaml(relativePath)
	if err != nil {
		return err
	}

	if err := kubeClient.RbacV1().RoleBindings(ns).Delete(
		context.TODO(),
		roleBinding.Name,
		metav1.DeleteOptions{},
	); err != nil {
		return err
	}

	return nil
}

func parseRoleBindingYaml(relativePath string) (*rbacv1.RoleBinding, error) {
	var manifest *os.File
	var err error

	var roleBinding rbacv1.RoleBinding
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

		if out.GetKind() == "RoleBinding" {
			var marshaled []byte
			marshaled, err = out.MarshalJSON()
			json.Unmarshal(marshaled, &roleBinding)
			break
		}
	}

	if err != io.EOF && err != nil {
		return nil, err
	}
	return &roleBinding, nil
}
