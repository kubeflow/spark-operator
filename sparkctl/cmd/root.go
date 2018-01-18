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
	"os"

	"github.com/spf13/cobra"
)

var defaultKubeConfig = os.Getenv("HOME" + "/.kube/config")

var Namespace string
var KubeConfig string

var rootCmd = &cobra.Command{
	Use:   "sparkctl",
	Short: "sparkctl is the command-line tool for working with the Spark Operator",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		// Do Stuff Here
	},
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&Namespace, "namespace", "n", "default",
		"The namespace in which the SparkApplication is to be created")
	rootCmd.PersistentFlags().StringVarP(&KubeConfig, "kubeconfig", "c", defaultKubeConfig,
		"The namespace in which the SparkApplication is to be created")
	rootCmd.AddCommand(createCmd, deleteCmd, statusCmd)
}

func Execute() {
	rootCmd.Execute()
}
