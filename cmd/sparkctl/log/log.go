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

package log

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/util"
)

var (
	k8sClient  client.Client
	clientset  kubernetes.Interface
	executorID int32
	followLogs bool
	timeout    time.Duration
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "log <name>",
		Short: "Fetch logs of the driver pod of a SparkApplication",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			namespace := viper.GetString("namespace")

			var err error
			k8sClient, err = util.GetK8sClient()
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes client: %v", err)
			}

			clientset, err = util.GetClientset()
			if err != nil {
				return fmt.Errorf("failed to create Kubernetes clientset: %v", err)
			}

			if err := doLog(name, namespace, followLogs); err != nil {
				return err
			}

			return nil
		},
	}

	cmd.Flags().Int32VarP(&executorID, "executor", "e", -1, "Executor id to fetch logs from.")
	cmd.Flags().BoolVarP(&followLogs, "follow", "f", false, "Specify if the logs should be streamed.")
	cmd.Flags().DurationVar(&timeout, "timeout", 30*time.Second, "Timeout for fetching logs.")

	return cmd
}

func doLog(name string, namespace string, stream bool) error {
	key := types.NamespacedName{Namespace: namespace, Name: name}
	app := &v1beta2.SparkApplication{}
	if err := k8sClient.Get(context.TODO(), key, app); err != nil {
		return fmt.Errorf("failed to get SparkApplication %s: %v", name, err)
	}

	driverPodName := app.Status.DriverInfo.PodName
	if driverPodName == "" {
		return fmt.Errorf("driver pod not found")
	}

	if stream {
		return streamLogs(driverPodName, namespace, os.Stdout)
	}
	return printLogs(driverPodName, namespace, os.Stdout)
}

func getPodNameChannel(name string, namespace string, k8sClient client.Client) chan string {
	key := types.NamespacedName{Namespace: namespace, Name: name}
	channel := make(chan string, 1)
	go func() {
		for {
			app := &v1beta2.SparkApplication{}
			if err := k8sClient.Get(context.TODO(), key, app); err != nil {
				continue
			}
			if app.Status.DriverInfo.PodName != "" {
				channel <- app.Status.DriverInfo.PodName
				break
			}
		}
	}()
	return channel
}

// printLogs is a one time operation that prints the fetched logs of the given pod.
func printLogs(name string, namespace string, out io.Writer) error {
	rawLogs, err := clientset.CoreV1().Pods(namespace).GetLogs(name, &corev1.PodLogOptions{}).Do(context.TODO()).Raw()
	if err != nil {
		return err
	}

	fmt.Fprintln(out, string(rawLogs))
	return nil
}

// streamLogs streams the logs of the given pod until there are no more logs available.
func streamLogs(name string, namespace string, out io.Writer) error {
	request := clientset.CoreV1().Pods(namespace).GetLogs(name, &corev1.PodLogOptions{Follow: true})
	reader, err := request.Stream(context.TODO())
	if err != nil {
		return err
	}
	defer reader.Close()

	if _, err := io.Copy(out, reader); err != nil {
		return err
	}
	return nil
}
