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

package list

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/util"
)

const (
	TimeLayout = "2006-01-02T15:04:05Z"
)

var (
	k8sClient     client.Client
	allNamespaces bool
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List SparkApplications in a given namespace",
		RunE: func(_ *cobra.Command, args []string) error {
			namespace := viper.GetString("namespace")

			var err error
			k8sClient, err = util.GetK8sClient()
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %v", err)
			}

			return doList(namespace)
		},
	}

	cmd.Flags().BoolVarP(&allNamespaces, "all-namespaces", "A", false, "If present, list the SparkApplications across all namespaces.")

	return cmd
}

func doList(namespace string) error {
	apps := v1beta2.SparkApplicationList{}
	listOptions := []client.ListOption{}
	if !allNamespaces {
		listOptions = append(listOptions, client.InNamespace(namespace))
	}
	if err := k8sClient.List(context.TODO(), &apps, listOptions...); err != nil {
		return fmt.Errorf("failed to list SparkApplications: %v", err)
	}

	writer := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer writer.Flush()
	fmt.Fprintf(writer, "Name\tStatus\tSubmission Attempts\tSubmission Time\tExecution Attempts\tTermination Time\n")
	for _, app := range apps.Items {
		var lastSubmissionTime, executionTime string
		if !app.Status.LastSubmissionAttemptTime.IsZero() {
			lastSubmissionTime = app.Status.LastSubmissionAttemptTime.Format(TimeLayout)
		}
		if !app.Status.TerminationTime.IsZero() {
			executionTime = app.Status.TerminationTime.Format(TimeLayout)
		}

		fmt.Fprintf(writer,
			"%s\t%s\t%d\t%s\t%d\t%s\n",
			app.Name,
			string(app.Status.AppState.State),
			app.Status.SubmissionAttempts,
			lastSubmissionTime,
			app.Status.ExecutionAttempts,
			executionTime,
		)
	}

	return nil
}
