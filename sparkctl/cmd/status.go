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

	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	crdclientset "k8s.io/spark-on-k8s-operator/pkg/client/clientset/versioned"
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

func printStatus(app *v1alpha1.SparkApplication) {
	fmt.Printf("application state: %s\n", app.Status.AppState.State)
	if app.Status.AppState.ErrorMessage != "" {
		fmt.Printf("application error message: %s\n", app.Status.AppState.ErrorMessage)
	}

	if app.Status.DriverInfo.PodName != "" {
		fmt.Printf("driver pod name:   %s\n", app.Status.DriverInfo.PodName)
	}
	if app.Status.DriverInfo.WebUIAddress != "" {
		fmt.Printf("driver UI address: %s\n", app.Status.DriverInfo.WebUIAddress)
	}

	if len(app.Status.ExecutorState) > 0 {
		fmt.Println("executor state:")
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Executor Pod", "State"})
		table.SetColumnColor(tablewriter.Colors{tablewriter.FgBlueColor}, tablewriter.Colors{tablewriter.FgGreenColor})
		for executorPod, state := range app.Status.ExecutorState {
			table.Append([]string{executorPod, string(state)})
		}
		table.Render()
	}
}
