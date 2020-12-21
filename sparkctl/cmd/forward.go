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
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"time"

	"github.com/spf13/cobra"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"

	crdclientset "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
)

var LocalPort int32
var RemotePort int32

var forwardCmd = &cobra.Command{
	Use:   "forward [--local-port <local port>] [--remote-port <remote port>]",
	Short: "Start to forward a local port to the remote port of the driver UI",
	Long:  `Start to forward a local port to the remote port of the driver UI so the UI can be accessed locally.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			fmt.Fprintln(os.Stderr, "must specify a SparkApplication name")
			return
		}

		config, err := buildConfig(KubeConfig)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get kubeconfig: %v\n", err)
			return
		}

		crdClientset, err := getSparkApplicationClientForConfig(config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get SparkApplication client: %v\n", err)
			return
		}

		kubeClientset, err := getKubeClientForConfig(config)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get REST client: %v\n", err)
			return
		}
		restClient := kubeClientset.CoreV1().RESTClient()

		driverPodUrl, driverPodName, err := getDriverPodUrlAndName(args[0], restClient, crdClientset)
		if err != nil {
			fmt.Fprintf(os.Stderr,
				"failed to get an API server URL of the driver pod of SparkApplication %s: %v\n",
				args[0], err)
			return
		}

		stopCh := make(chan struct{}, 1)
		readyCh := make(chan struct{})

		forwarder, err := newPortForwarder(config, driverPodUrl, stopCh, readyCh)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to get a port forwarder: %v\n", err)
			return
		}

		fmt.Printf("forwarding from %d -> %d\n", LocalPort, RemotePort)
		if err = runPortForward(driverPodName, stopCh, forwarder, kubeClientset); err != nil {
			fmt.Fprintf(os.Stderr, "failed to run port forwarding: %v\n", err)
		}
	},
}

func init() {
	forwardCmd.Flags().Int32VarP(&LocalPort, "local-port", "l", 4040,
		"local port to forward from")
	forwardCmd.Flags().Int32VarP(&RemotePort, "remote-port", "r", 4040,
		"remote port to forward to")
}

func newPortForwarder(
	config *rest.Config,
	url *url.URL,
	stopCh chan struct{},
	readyCh chan struct{}) (*portforward.PortForwarder, error) {
	transport, upgrader, err := spdy.RoundTripperFor(config)
	if err != nil {
		return nil, err
	}

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", url)
	ports := []string{fmt.Sprintf("%d:%d", LocalPort, RemotePort)}
	fw, err := portforward.New(dialer, ports, stopCh, readyCh, nil, os.Stderr)
	if err != nil {
		return nil, err
	}

	return fw, nil
}

func getDriverPodUrlAndName(
	name string,
	restClient rest.Interface,
	crdClientset crdclientset.Interface) (*url.URL, string, error) {
	app, err := getSparkApplication(name, crdClientset)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get SparkApplication %s: %v", name, err)
	}

	if app.Status.DriverInfo.PodName != "" {
		request := restClient.Post().
			Resource("pods").
			Namespace(Namespace).
			Name(app.Status.DriverInfo.PodName).
			SubResource("portforward")
		return request.URL(), app.Status.DriverInfo.PodName, nil
	}

	return nil, "", fmt.Errorf("driver pod name of SparkApplication %s is not available yet", name)
}

func runPortForward(
	driverPodName string,
	stopCh chan struct{},
	forwarder *portforward.PortForwarder,
	kubeClientset clientset.Interface) error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	defer signal.Stop(signals)

	go func() {
		defer close(stopCh)
		for {
			pod, err := kubeClientset.CoreV1().Pods(Namespace).Get(context.TODO(), driverPodName, metav1.GetOptions{})
			if err != nil {
				break
			}
			if pod.Status.Phase == apiv1.PodSucceeded || pod.Status.Phase == apiv1.PodFailed {
				break
			}
			time.Sleep(1 * time.Second)
		}
		fmt.Println("stopping forwarding as the driver pod has terminated")
	}()

	go func() {
		<-signals
		close(stopCh)
	}()

	return forwarder.ForwardPorts()
}
