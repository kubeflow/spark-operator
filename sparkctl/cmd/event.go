/*
Copyright 2018 Google LLC

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
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/util/interrupt"

	crdclientset "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
)

var FollowEvents bool

var eventCommand = &cobra.Command{
	Use:   "event <name>",
	Short: "Shows SparkApplication events",
	Long:  `Shows events associated with SparkApplication of a given name`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			fmt.Fprintln(os.Stderr, "must specify a SparkApplication name")
			return
		}

		crdClientset, err := getSparkApplicationClient()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get SparkApplication client: %v\n", err)
			return
		}

		kubeClientset, err := getKubeClient()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get KubeClient: %v\n", err)
			return
		}

		if err := doShowEvents(args[0], crdClientset, kubeClientset); err != nil {
			fmt.Fprintf(os.Stderr, "failed to check events of SparkApplication %s: %v\n", args[0], err)
		}
	},
}

func init() {
	eventCommand.Flags().BoolVarP(&FollowEvents, "follow", "f", false,
		"whether to stream the events for the specified SparkApplication name")
}

func doShowEvents(name string, crdClientset crdclientset.Interface, kubeClientset kubernetes.Interface) error {
	app, err := getSparkApplication(name, crdClientset)
	if err != nil {
		return fmt.Errorf("failed to get SparkApplication %s: %v", name, err)
	}
	app.Kind = "SparkApplication"

	eventsInterface := kubeClientset.CoreV1().Events(Namespace)
	if FollowEvents {
		// watch for all events for this specific SparkApplication name
		selector := eventsInterface.GetFieldSelector(&app.Name, &app.Namespace, &app.Kind, nil)
		options := metav1.ListOptions{FieldSelector: selector.String(), Watch: true}
		events, err := eventsInterface.Watch(options)
		if err != nil {
			return err
		}
		if err := streamEvents(events, app.CreationTimestamp.Unix()); err != nil {
			return err
		}
	} else {
		// print only events for current SparkApplication UID
		stringUID := string(app.UID)
		selector := eventsInterface.GetFieldSelector(&app.Name, &app.Namespace, &app.Kind, &stringUID)
		options := metav1.ListOptions{FieldSelector: selector.String()}
		events, err := eventsInterface.List(options)
		if err != nil {
			return err
		}
		if err := printEvents(events); err != nil {
			return err
		}
	}

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

func printEvents(events *v1.EventList) error {
	// Render all event rows
	table := prepareNewTable()
	table = prepareEventsHeader(table)
	for _, event := range events.Items {
		table.Append([]string{
			event.Type,
			getSinceTime(event.LastTimestamp),
			strings.TrimSpace(event.Message),
		})
	}

	table.Render()
	return nil
}

func streamEvents(events watch.Interface, streamSince int64) error {
	// Render just table header, without a additional header line as we stream
	table := prepareNewTable()
	table = prepareEventsHeader(table)
	table.SetHeaderLine(false)
	table.Render()

	// Set 10 minutes inactivity timeout
	watchExpire := time.Duration(10 * time.Minute)
	intr := interrupt.New(nil, events.Stop)
	return intr.Run(func() error {
		// Start rendering contents of the table without table header as it is already printed
		table = prepareNewTable()
		table.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
		_, err := watch.Until(watchExpire, events, func(ev watch.Event) (bool, error) {
			if event, isEvent := ev.Object.(*v1.Event); isEvent {
				// Ensure to display events which are newer than last creation time of SparkApplication
				// for this specific application name
				if streamSince <= event.CreationTimestamp.Unix() {
					// Render each row separately
					table.ClearRows()
					table.Append([]string{
						event.Type,
						getSinceTime(event.LastTimestamp),
						strings.TrimSpace(event.Message),
					})
					table.Render()
				}
			} else {
				fmt.Printf("info: %v", ev.Object)
			}

			return false, nil
		})

		return err
	})
}
