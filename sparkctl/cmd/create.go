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
	"os"

	"github.com/spf13/cobra"
	"github.com/golang/glog"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
)

const bufferSize = 1024

var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a SparkApplication object",
	Long:  `Create a SparkApplication from a given YAML file storing the application specification`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			glog.Fatal(rootCmd.Usage())
		}

		if err := doCreate(args[0]); err != nil {
			glog.Fatalf("failed to create a SparkApplication from file %s: %v", args[0], err)
		}
	},
}

func doCreate(yamlFile string) error {
	file, err := os.Open(yamlFile)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := yaml.NewYAMLOrJSONDecoder(file, bufferSize)
	app := &v1alpha1.SparkApplication{}
	err = decoder.Decode(app)
	if err != nil {
		return err
	}

	if err = handleLocalDependencies(app); err != nil {
		return err
	}

	clientset, err := getSparkApplicationClient()
	if err != nil {
		return err
	}

	if _, err = clientset.SparkoperatorV1alpha1().SparkApplications(Namespace).Create(app); err != nil {
		return err
	}

	return nil
}

func handleLocalDependencies(app *v1alpha1.SparkApplication) error {
	return nil
}
