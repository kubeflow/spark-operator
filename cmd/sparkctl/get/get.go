/*
Copyright 2024 The Kubeflow authors.

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

package get

import (
	"context"
	"fmt"
	"os"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"k8s.io/apimachinery/pkg/types"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/util"
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <name>",
		Short: "Get status of a SparkApplication",
		Long:  "Get status of a SparkApplication with the given name",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			name := args[0]
			namespace := viper.GetString("namespace")

			k8sClient, err := util.GetK8sClient()
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %v", err)
			}

			key := types.NamespacedName{Namespace: namespace, Name: name}
			app := &v1beta2.SparkApplication{}
			if err := k8sClient.Get(context.TODO(), key, app); err != nil {
				return fmt.Errorf("failed to get SparkApplication %s: %v", name, err)
			}

			printStatus(app)

			return nil
		},
	}
	return cmd
}

func printStatus(app *v1beta2.SparkApplication) {
	fmt.Println("application state:")
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"State", "Submission Age", "Completion Age", "Driver Pod", "Driver UI", "Submission Attempts", "Execution Attempts"})
	table.Append([]string{
		string(app.Status.AppState.State),
		util.GetSinceTime(app.Status.LastSubmissionAttemptTime),
		util.GetSinceTime(app.Status.TerminationTime),
		util.FormatNotAvailable(app.Status.DriverInfo.PodName),
		util.FormatNotAvailable(app.Status.DriverInfo.WebUIAddress),
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
