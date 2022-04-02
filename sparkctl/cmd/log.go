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
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	crdclientset "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
)

var ExecutorId int32
var FollowLogs bool

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

		if err := doLog(args[0], FollowLogs, kubeClientset, crdClientset); err != nil {
			fmt.Fprintf(os.Stderr, "failed to get driver logs of SparkApplication %s: %v\n", args[0], err)
		}
	},
}

func init() {
	logCommand.Flags().Int32VarP(&ExecutorId, "executor", "e", -1,
		"id of the executor to fetch logs for")
	logCommand.Flags().BoolVarP(&FollowLogs, "follow", "f", false, "whether to stream the logs")
}

func doLog(
	name string,
	followLogs bool,
	kubeClient clientset.Interface,
	crdClient crdclientset.Interface) error {

	timeout := 30 * time.Second

	podNameChannel := getPodNameChannel(name, crdClient)
	var podName string

	select {
	case podName = <-podNameChannel:
	case <-time.After(timeout):
		return fmt.Errorf("not found pod name")
	}

	waitLogsChannel := waitForLogsFromPodChannel(podName, kubeClient, crdClient)

	select {
	case <-waitLogsChannel:
	case <-time.After(timeout):
		return fmt.Errorf("timeout to fetch logs from pod \"%s\"", podName)
	}

	if followLogs {
		return streamLogs(os.Stdout, kubeClient, podName)
	} else {
		return printLogs(os.Stdout, kubeClient, podName)
	}
}

func getPodNameChannel(
	sparkApplicationName string,
	crdClient crdclientset.Interface) chan string {

	channel := make(chan string, 1)
	go func() {
		for true {
			app, _ := crdClient.SparkoperatorV1beta2().SparkApplications(Namespace).Get(
				context.TODO(),
				sparkApplicationName,
				metav1.GetOptions{})

			if app.Status.DriverInfo.PodName != "" {
				channel <- app.Status.DriverInfo.PodName
				break
			}
		}
	}()
	return channel
}

func waitForLogsFromPodChannel(
	podName string,
	kubeClient clientset.Interface,
	crdClient crdclientset.Interface) chan bool {

	channel := make(chan bool, 1)
	go func() {
		for true {
			_, err := kubeClient.CoreV1().Pods(Namespace).GetLogs(podName, &apiv1.PodLogOptions{}).Do(context.TODO()).Raw()

			if err == nil {
				channel <- true
				break
			}
		}
	}()
	return channel
}

// printLogs is a one time operation that prints the fetched logs of the given pod.
func printLogs(out io.Writer, kubeClientset clientset.Interface, podName string) error {
	rawLogs, err := kubeClientset.CoreV1().Pods(Namespace).GetLogs(podName, &apiv1.PodLogOptions{}).Do(context.TODO()).Raw()
	if err != nil {
		return err
	}
	fmt.Fprintln(out, string(rawLogs))
	return nil
}

// streamLogs streams the logs of the given pod until there are no more logs available.
func streamLogs(out io.Writer, kubeClientset clientset.Interface, podName string) error {
	request := kubeClientset.CoreV1().Pods(Namespace).GetLogs(podName, &apiv1.PodLogOptions{Follow: true})
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
