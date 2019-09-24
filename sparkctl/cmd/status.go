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

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	crdclientset "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
)

var statusCmd = &cobra.Command{
	Use:   "status <name>",
	Short: "Check status of a SparkApplication",
	Long:  `Check status of a SparkApplication with a given name`,
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

		if err := doStatus(args[0], crdClientset); err != nil {
			fmt.Fprintf(os.Stderr, "failed to check status of SparkApplication %s: %v\n", args[0], err)
		}
	},
}

func doStatus(name string, crdClientset crdclientset.Interface) error {
	app, err := getSparkApplication(name, crdClientset)
	if err != nil {
		return fmt.Errorf("failed to get SparkApplication %s: %v", name, err)
	}

	printStatus(app)

	return nil
}

func printStatus(app *v1beta2.SparkApplication) {
	fmt.Println("application state:")
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"State", "Submission Age", "Completion Age", "Driver Pod", "Driver UI", "SubmissionAttempts", "ExecutionAttempts"})
	table.Append([]string{
		string(app.Status.AppState.State),
		getSinceTime(app.Status.LastSubmissionAttemptTime),
		getSinceTime(app.Status.TerminationTime),
		formatNotAvailable(app.Status.DriverInfo.PodName),
		formatNotAvailable(app.Status.DriverInfo.WebUIAddress),
		fmt.Sprintf("%v", app.Status.SubmissionAttempts),
		fmt.Sprintf("%v", app.Status.ExecutionAttempts),
	})
	table.Render()

	if len(app.Status.ExecutorState) > 0 {
		fmt.Println("executor state:")
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Executor Pod", "State"})
		for executorPod, state := range app.Status.ExecutorState {
			table.Append([]string{executorPod, string(state)})
		}
		table.Render()
	}

	if app.Status.AppState.ErrorMessage != "" {
		fmt.Printf("\napplication error message: %s\n", app.Status.AppState.ErrorMessage)
	}
}
