/*
Copyright 2018 Google LLC

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

package e2e

import (
	"flag"
	"log"
	"os"
	"testing"

	operatorFramework "github.com/GoogleCloudPlatform/spark-on-k8s-operator/test/e2e/framework"
)

var framework *operatorFramework.Framework

func TestMain(m *testing.M) {
	kubeconfig := flag.String("kubeconfig", "", "kube config path, e.g. $HOME/.kube/config")
	opImage := flag.String("operator-image", "", "operator image, e.g. image:tag")
	ns := flag.String("namespace", "spark-operator", "e2e test namespace")
	flag.Parse()

	if *kubeconfig == "" {
		log.Printf("No kubeconfig found. Bypassing e2e tests")
		os.Exit(0)
	}

	var err error
	if framework, err = operatorFramework.New(*ns, *kubeconfig, *opImage); err != nil {
		log.Fatalf("failed to set up framework: %v\n", err)
	}

	code := m.Run()

	if err := framework.Teardown(); err != nil {
		log.Fatalf("failed to tear down framework: %v\n", err)
	}
	os.Exit(0)

	os.Exit(code)
}
