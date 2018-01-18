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
)

var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete a SparkApplication object",
	Long:  `Delete a SparkApplication object with a given name`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			glog.Fatal(rootCmd.Usage())
		}

		if err := doDelete(args[0]); err != nil {
			glog.Fatalf("failed to delete SparkApplication %s: %v", args[0], err)
		}
	},
}

func doDelete(name string) error {
	clientset, err := getSparkApplicationClient()
	if err != nil {
		return err
	}

	err = clientset.SparkoperatorV1alpha1().SparkApplications(Namespace).Delete(name, &metav1.DeleteOptions{})
	if err != nil {
		return err
	}

	return nil
}
