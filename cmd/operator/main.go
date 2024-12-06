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

package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/kubeflow/spark-operator/cmd/operator/controller"
	"github.com/kubeflow/spark-operator/cmd/operator/version"
	"github.com/kubeflow/spark-operator/cmd/operator/webhook"
)

func NewCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "spark-operator",
		Short: "Spark operator",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	command.AddCommand(controller.NewCommand())
	command.AddCommand(webhook.NewCommand())
	command.AddCommand(version.NewCommand())
	return command
}

func main() {
	if err := NewCommand().Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
