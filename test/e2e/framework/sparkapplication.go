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
	"fmt"

	"github.com/pkg/errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	crdclientset "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
)

func MakeSparkApplicationFromYaml(pathToYaml string) (*v1beta2.SparkApplication, error) {
	manifest, err := PathToOSFile(pathToYaml)
	if err != nil {
		return nil, err
	}
	tectonicPromOp := v1beta2.SparkApplication{}
	if err := yaml.NewYAMLOrJSONDecoder(manifest, 100).Decode(&tectonicPromOp); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to decode file %s", pathToYaml))
	}

	return &tectonicPromOp, nil
}

func CreateSparkApplication(crdclientset crdclientset.Interface, namespace string, sa *v1beta2.SparkApplication) error {
	_, err := crdclientset.SparkoperatorV1beta2().SparkApplications(namespace).Create(context.TODO(), sa, metav1.CreateOptions{})
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to create SparkApplication %s", sa.Name))
	}
	return nil
}

func UpdateSparkApplication(crdclientset crdclientset.Interface, namespace string, sa *v1beta2.SparkApplication) error {
	_, err := crdclientset.SparkoperatorV1beta2().SparkApplications(namespace).Update(context.TODO(), sa, metav1.UpdateOptions{})
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to update SparkApplication %s", sa.Name))
	}
	return nil
}

func GetSparkApplication(crdclientset crdclientset.Interface, namespace, name string) (*v1beta2.SparkApplication, error) {
	sa, err := crdclientset.SparkoperatorV1beta2().SparkApplications(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return sa, nil
}

func DeleteSparkApplication(crdclientset crdclientset.Interface, namespace, name string) error {
	err := crdclientset.SparkoperatorV1beta2().SparkApplications(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
	if err != nil {
		return err
	}
	return nil
}
