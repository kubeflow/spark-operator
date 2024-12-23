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
	"flag"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/kubeflow/spark-operator/cmd/sparkctl/create"
	"github.com/kubeflow/spark-operator/cmd/sparkctl/delete"
	"github.com/kubeflow/spark-operator/cmd/sparkctl/event"
	"github.com/kubeflow/spark-operator/cmd/sparkctl/forward"
	"github.com/kubeflow/spark-operator/cmd/sparkctl/get"
	"github.com/kubeflow/spark-operator/cmd/sparkctl/list"
	"github.com/kubeflow/spark-operator/cmd/sparkctl/log"
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sparkctl",
		Short: "sparkctl is the command-line tool for working with the Spark Operator",
		Long: `sparkctl is the command-line tool for working with the Spark Operator.
It supports creating, deleting and checking status of SparkApplication objects. It also supports fetching application logs.`,
	}

	cmd.PersistentFlags().StringP("namespace", "n", "default", "The namespace in which the SparkApplication is to be created")
	viper.BindPFlag("namespace", cmd.PersistentFlags().Lookup("namespace"))

	flagSet := flag.NewFlagSet("controller", flag.ExitOnError)
	ctrl.RegisterFlags(flagSet)
	cmd.Flags().AddGoFlagSet(flagSet)

	cmd.AddCommand(get.NewCommand())
	cmd.AddCommand(list.NewCommand())
	cmd.AddCommand(event.NewCommand())
	cmd.AddCommand(log.NewCommand())
	cmd.AddCommand(forward.NewCommand())
	cmd.AddCommand(create.NewCommand())
	cmd.AddCommand(delete.NewCommand())

	return cmd
}

func main() {
	if err := NewCommand().Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
