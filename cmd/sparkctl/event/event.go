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

package event

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	clientWatch "k8s.io/client-go/tools/watch"
	"k8s.io/kubernetes/pkg/util/interrupt"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/util"
)

var (
	followEvents bool
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "event <name>",
		Short: "Show events associated with a SparkApplication",
		Long:  "Show events associated with a SparkApplication of the given name",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			namespace := viper.GetString("namespace")

			k8sClient, err := util.GetK8sClient()
			if err != nil {
				return fmt.Errorf("failed to get Kubernetes client: %v", err)
			}

			clientset, err := util.GetClientset()
			if err != nil {
				return fmt.Errorf("failed to get Kubernetes clientset: %v", err)
			}

			if err := doShowEvents(name, namespace, k8sClient, clientset); err != nil {
				return err
			}

			return nil
		},
	}
	cmd.Flags().BoolVarP(&followEvents, "follow", "f", false, "whether to stream the events for the specified SparkApplication name")
	return cmd
}

func doShowEvents(name string, namespace string, k8sClient client.Client, clientset kubernetes.Interface) error {
	key := types.NamespacedName{Namespace: namespace, Name: name}
	app := v1beta2.SparkApplication{}
	if err := k8sClient.Get(context.TODO(), key, &app); err != nil {
		return fmt.Errorf("failed to get SparkApplication %s: %v", name, err)
	}

	eventsClient := clientset.CoreV1().Events(namespace)
	if followEvents {
		// watch for all events for this specific SparkApplication name
		selector := eventsClient.GetFieldSelector(&app.Name, &app.Namespace, &app.Kind, nil)
		options := metav1.ListOptions{FieldSelector: selector.String(), Watch: true}
		events, err := eventsClient.Watch(context.TODO(), options)
		if err != nil {
			return err
		}
		if err := streamEvents(events, app.CreationTimestamp.Unix()); err != nil {
			return err
		}
	} else {
		// print only events for current SparkApplication UID
		uid := string(app.UID)
		selector := eventsClient.GetFieldSelector(&app.Name, &app.Namespace, &app.Kind, &uid)
		options := metav1.ListOptions{FieldSelector: selector.String()}
		events, err := eventsClient.List(context.TODO(), options)
		if err != nil {
			return err
		}
		if err := printEvents(events); err != nil {
			return err
		}
	}

	return nil
}

func streamEvents(events watch.Interface, streamSince int64) error {
	// Render just table header, without a additional header line as we stream
	table := prepareNewTable()
	table = prepareEventsHeader(table)
	table.SetHeaderLine(false)
	table.Render()

	// Set 10 minutes inactivity timeout
	watchExpire := 10 * time.Minute
	intr := interrupt.New(nil, events.Stop)
	return intr.Run(func() error {
		// Start rendering contents of the table without table header as it is already printed
		table = prepareNewTable()
		table.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
		ctx, cancel := context.WithTimeout(context.TODO(), watchExpire)
		defer cancel()

		_, err := clientWatch.UntilWithoutRetry(ctx, events, func(e watch.Event) (bool, error) {
			if event, ok := e.Object.(*corev1.Event); ok {
				// Ensure to display events which are newer than last creation time of SparkApplication
				// for this specific application name
				if streamSince <= event.CreationTimestamp.Unix() {
					// Render each row separately
					table.ClearRows()
					table.Append([]string{
						event.Type,
						util.GetSinceTime(event.LastTimestamp),
						strings.TrimSpace(event.Message),
					})
					table.Render()
				}
			} else {
				fmt.Printf("info: %v", e.Object)
			}

			return false, nil
		})
		return err
	})
}

func printEvents(events *corev1.EventList) error {
	// Render all event rows
	table := prepareNewTable()
	table = prepareEventsHeader(table)
	for _, event := range events.Items {
		table.Append([]string{
			event.Type,
			util.GetSinceTime(event.LastTimestamp),
			strings.TrimSpace(event.Message),
		})
	}

	table.Render()
	return nil
}

func prepareNewTable() *tablewriter.Table {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetColMinWidth(0, 10)
	table.SetColMinWidth(1, 6)
	table.SetColMinWidth(2, 50)
	return table
}

func prepareEventsHeader(table *tablewriter.Table) *tablewriter.Table {
	table.SetBorders(tablewriter.Border{Left: true, Top: true, Right: true, Bottom: true})
	table.SetHeader([]string{"Type", "Age", "Message"})
	table.SetHeaderLine(true)
	return table
}
