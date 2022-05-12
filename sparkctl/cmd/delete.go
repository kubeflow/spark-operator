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
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	crdclientset "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
)

var deleteCmd = &cobra.Command{
	Use:   "delete <name>",
	Short: "Delete a SparkApplication object",
	Long:  `Delete a SparkApplication object with a given name`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			fmt.Fprintln(os.Stderr, "must specify a SparkApplication name")
			return
		}

		crdClientset, err := getSparkApplicationClient()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get SparkApplication client: %v\n", err)
			return
		}

		if err := doDelete(args[0], crdClientset); err != nil {
			fmt.Fprintf(os.Stderr, "failed to delete SparkApplication %s: %v\n", args[0], err)
		}
	},
}

func doDelete(name string, crdClientset crdclientset.Interface) error {
	if err := deleteSparkApplication(name, crdClientset); err != nil {
		return err
	}

	fmt.Printf("SparkApplication \"%s\" deleted\n", name)

	return nil
}

func deleteSparkApplication(name string, crdClientset crdclientset.Interface) error {
	return crdClientset.SparkoperatorV1beta2().SparkApplications(Namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
}
