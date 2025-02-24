/*
Copyright 2024 The Kubeflow authors.

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

package version

import (
	"github.com/spf13/cobra"

	sparkoperator "github.com/kubeflow/spark-operator"
)

var (
	short bool
)

func NewCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		RunE: func(cmd *cobra.Command, args []string) error {
			sparkoperator.PrintVersion(short)
			return nil
		},
	}
	command.Flags().BoolVar(&short, "short", false, "Print just the version string.")
	return command
}
