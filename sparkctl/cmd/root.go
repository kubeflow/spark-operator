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

	"github.com/spf13/cobra"
)

func getKubeConfigPath() string {
	var kubeConfigEnv = os.Getenv("KUBECONFIG")
	if len(kubeConfigEnv) == 0 {
		return os.Getenv("HOME") + "/.kube/config"
	}
	return kubeConfigEnv
}

var defaultKubeConfig = getKubeConfigPath()

var Namespace string
var KubeConfig string

var rootCmd = &cobra.Command{
	Use:   "sparkctl",
	Short: "sparkctl is the command-line tool for working with the Spark Operator",
	Long: `sparkctl is the command-line tool for working with the Spark Operator. It supports creating, deleting and 
           checking status of SparkApplication objects. It also supports fetching application logs.`,
}

func init() {
	rootCmd.PersistentFlags().StringVarP(&Namespace, "namespace", "n", "default",
		"The namespace in which the SparkApplication is to be created")
	rootCmd.PersistentFlags().StringVarP(&KubeConfig, "kubeconfig", "k", defaultKubeConfig,
		"The path to the local Kubernetes configuration file")
	rootCmd.AddCommand(createCmd, deleteCmd, eventCommand, statusCmd, logCommand, listCmd, forwardCmd)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%v", err)
	}
}
