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
	"github.com/spf13/cobra"
	"github.com/golang/glog"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Check status of a SparkApplication",
	Long:  `Check status of a SparkApplication with a given name`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			glog.Fatal(rootCmd.Usage())
		}

		if err := doStatus(args[0]); err != nil {
			glog.Fatalf("failed to check status of SparkApplication %s: %v", args[0], err)
		}
	},
}

func doStatus(name string) error {
	clientset, err := getSparkApplicationClient()
	if err != nil {
		return err
	}

	app, err := clientset.SparkoperatorV1alpha1().SparkApplications(Namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	printStatus(app)

	return nil
}

func printStatus(app *v1alpha1.SparkApplication) {

}
