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
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
	"time"

	operatorFramework "github.com/GoogleCloudPlatform/spark-on-k8s-operator/test/e2e/framework"
)

var framework *operatorFramework.Framework

// Wait for test job to finish. Poll for updates once a second. Time out after 240 seconds.
var TIMEOUT = 240 * time.Second
var INTERVAL = 1 * time.Second

var STATES = [9]string{
	"",
	"SUBMITTED",
	"RUNNING",
	"COMPLETED",
	"INVALIDATING",
	"PENDING_RERUN",
	"SUBMITTED",
	"RUNNING",
	"COMPLETED",
}

func GetJobStatus(t *testing.T, sparkAppName string) v1beta2.ApplicationStateType {
	app, err := operatorFramework.GetSparkApplication(framework.SparkApplicationClient, operatorFramework.SparkTestNamespace, sparkAppName)
	assert.Equal(t, nil, err)
	return app.Status.AppState.State
}

func TestMain(m *testing.M) {
	kubeconfig := flag.String("kubeconfig", "", "kube config path, e.g. $HOME/.kube/config")
	opImage := flag.String("operator-image", "", "operator image, e.g. image:tag")
	opImagePullPolicy := flag.String("operator-image-pullPolicy", "IfNotPresent", "pull policy, e.g. Always")
	ns := flag.String("namespace", "spark-operator", "e2e test namespace")
	sparkTestNamespace := flag.String("spark", "spark", "e2e test spark-test-namespace")
	sparkTestImage := flag.String("spark-test-image", "", "spark test image, e.g. image:tag")
	sparkTestServiceAccount := flag.String("spark-test-service-account", "spark", "e2e test spark test service account")
	flag.Parse()

	if *kubeconfig == "" {
		log.Printf("No kubeconfig found. Bypassing e2e tests")
		os.Exit(0)
	}
	var err error
	if framework, err = operatorFramework.New(*ns, *sparkTestNamespace, *kubeconfig, *opImage, *opImagePullPolicy); err != nil {
		log.Fatalf("failed to set up framework: %v\n", err)
	}

	operatorFramework.SparkTestNamespace = *sparkTestNamespace
	operatorFramework.SparkTestImage = *sparkTestImage
	operatorFramework.SparkTestServiceAccount = *sparkTestServiceAccount
	code := m.Run()

	if err := framework.Teardown(); err != nil {
		log.Fatalf("failed to tear down framework: %v\n", err)
	}

	os.Exit(code)
}
