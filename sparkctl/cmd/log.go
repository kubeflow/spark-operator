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
	"strings"

	"github.com/spf13/cobra"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	crdclientset "k8s.io/spark-on-k8s-operator/pkg/client/clientset/versioned"
)

var ExecutorId int32

var logCommand = &cobra.Command{
	Use:   "log <name>",
	Short: "log is a sub-command of sparkctl that fetches logs of a Spark application.",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			fmt.Fprintln(os.Stderr, "must specify a SparkApplication name")
			return
		}

		kubeClientset, err := getKubeClient()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get Kubernetes client: %v\n", err)
			return
		}

		crdClientset, err := getSparkApplicationClient()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get SparkApplication client: %v\n", err)
			return
		}

		if err := doLog(args[0], kubeClientset, crdClientset); err != nil {
			fmt.Fprintf(os.Stderr, "failed to get driver logs of SparkApplication %s: %v\n", args[0], err)
		}
	},
}

func init() {
	logCommand.Flags().Int32VarP(&ExecutorId, "executor", "e", -1, "executor id")
}

func doLog(name string, kubeClientset clientset.Interface, crdClientset crdclientset.Interface) error {
	app, err := crdClientset.SparkoperatorV1alpha1().SparkApplications(Namespace).Get(name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get SparkApplication %s: %v", name, err)
	}

	var podName string
	if ExecutorId < 0 {
		podName = app.Status.DriverInfo.PodName
	} else {
		podName = strings.NewReplacer("driver", fmt.Sprintf("exec-%d", ExecutorId)).
			Replace(app.Status.DriverInfo.PodName)
	}

	if podName == "" {
		return fmt.Errorf("unable to fetch logs as the name of the target pod is empty")
	}

	rawLogs, err := kubeClientset.CoreV1().Pods(Namespace).GetLogs(podName, &apiv1.PodLogOptions{}).Do().Raw()
	if err != nil {
		return err
	}
	fmt.Println(string(rawLogs))

	return nil
}
