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

package forward

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/util"
)

var (
	localPort  int32
	remotePort int32
)

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "forward <name> [--local-port <local port>] [--remote-port <remote port>]",
		Short: "Start to forward a local port to the remote port of the driver UI",
		Long:  `Start to forward a local port to the remote port of the driver UI so the UI can be accessed locally.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			namespace := viper.GetString("namespace")

			config, err := ctrl.GetConfig()
			if err != nil {
				return fmt.Errorf("failed to get rest config: %v", err)
			}

			k8sClient, err := util.GetK8sClient()
			if err != nil {
				return fmt.Errorf("failed to get Kubernetes client: %v", err)
			}

			clientset, err := util.GetClientset()
			if err != nil {
				return fmt.Errorf("failed to get Kubernetes clientset: %v", err)
			}

			return doPortForward(name, namespace, config, k8sClient, clientset)
		},
	}

	cmd.Flags().Int32VarP(&localPort, "local-port", "l", 4040, "local port to forward from")
	cmd.Flags().Int32VarP(&remotePort, "remote-port", "r", 4040, "remote port to forward to")
	return cmd
}

func doPortForward(
	name string,
	namespace string,
	config *rest.Config,
	k8sClient client.Client,
	clientset kubernetes.Interface,
) error {
	key := types.NamespacedName{Namespace: namespace, Name: name}
	app := &v1beta2.SparkApplication{}
	if err := k8sClient.Get(context.TODO(), key, app); err != nil {
		return fmt.Errorf("failed to get SparkApplication %s: %v", name, err)
	}

	driverPodName := app.Status.DriverInfo.PodName
	if driverPodName == "" {
		return fmt.Errorf("driver pod not found")
	}

	url := clientset.CoreV1().
		RESTClient().
		Post().
		Resource("pods").
		Namespace(namespace).
		Name(app.Status.DriverInfo.PodName).
		SubResource("portforward").
		URL()
	if url == nil {
		return fmt.Errorf("failed to get URL for port forwarding")
	}

	stopCh := make(chan struct{}, 1)
	readyCh := make(chan struct{})

	forwarder, err := newPortForwarder(config, url, stopCh, readyCh)
	if err != nil {
		return fmt.Errorf("failed to get port forwarder: %v", err)
	}

	fmt.Printf("Forwarding from %d -> %d\n", localPort, remotePort)
	if err = runPortForward(driverPodName, namespace, forwarder, clientset, stopCh); err != nil {
		return fmt.Errorf("failed to do port forwarding: %v", err)
	}

	return nil
}

func newPortForwarder(
	config *rest.Config,
	url *url.URL,
	stopCh chan struct{},
	readyCh chan struct{},
) (*portforward.PortForwarder, error) {
	transport, upgrader, err := spdy.RoundTripperFor(config)
	if err != nil {
		return nil, err
	}

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", url)
	ports := []string{fmt.Sprintf("%d:%d", localPort, remotePort)}
	forwarder, err := portforward.New(dialer, ports, stopCh, readyCh, nil, os.Stderr)
	if err != nil {
		return nil, err
	}

	return forwarder, nil
}

func runPortForward(
	driverPodName string,
	namespace string,
	forwarder *portforward.PortForwarder,
	clientset kubernetes.Interface,
	stopCh chan struct{},
) error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT, syscall.SIGSTOP)
	defer signal.Stop(signals)

	go func() {
		defer close(stopCh)
		for {
			pod, err := clientset.CoreV1().Pods(namespace).Get(context.TODO(), driverPodName, metav1.GetOptions{})
			if err != nil {
				break
			}
			if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
				break
			}
			time.Sleep(1 * time.Second)
		}
		fmt.Println("Stop forwarding as the driver pod has terminated")
	}()

	go func() {
		<-signals
		close(stopCh)
	}()

	return forwarder.ForwardPorts()
}
