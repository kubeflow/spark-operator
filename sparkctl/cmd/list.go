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
	"fmt"
	"os"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	crdclientset "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List SparkApplication objects",
	Long:  `List SparkApplication objects in a given namespaces.`,
	Run: func(cmd *cobra.Command, args []string) {
		crdClientset, err := getSparkApplicationClient()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get SparkApplication client: %v\n", err)
			return
		}

		if err = doList(crdClientset); err != nil {
			fmt.Fprintf(os.Stderr, "failed to list SparkApplications: %v\n", err)
		}
	},
}

func doList(crdClientset crdclientset.Interface) error {
	apps, err := crdClientset.SparkoperatorV1alpha1().SparkApplications(Namespace).List(metav1.ListOptions{})
	if err != nil {
		return err
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Name", "State", "Submission Age", "Termination Age"})
	for _, app := range apps.Items {
		table.Append([]string{
			string(app.Name),
			string(app.Status.AppState.State),
			getSinceTime(app.Status.LastSubmissionAttemptTime),
			getSinceTime(app.Status.TerminationTime),
		})
	}
	table.Render()

	return nil
}
