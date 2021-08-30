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

// This integration test verifies that a volume can be successfully
// mounted in the driver and executor pods.

package e2e

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubectl/pkg/describe"

	appFramework "github.com/GoogleCloudPlatform/spark-on-k8s-operator/test/e2e/framework"
)

type describeClient struct {
	T         *testing.T
	Namespace string
	Err       error
	kubernetes.Interface
}

func TestMountConfigMap(t *testing.T) {
	appName := "spark-pi"

	sa, err := appFramework.MakeSparkApplicationFromYaml("../../examples/spark-pi-configmap.yaml")
	assert.Equal(t, nil, err)

	if appFramework.SparkTestNamespace != "" {
		sa.ObjectMeta.Namespace = appFramework.SparkTestNamespace
	}

	if appFramework.SparkTestServiceAccount != "" {
		sa.Spec.Driver.ServiceAccount = &appFramework.SparkTestServiceAccount
	}

	if appFramework.SparkTestImage != "" {
		sa.Spec.Image = &appFramework.SparkTestImage
	}

	_, err = appFramework.CreateConfigMap(framework.KubeClient, "dummy-cm", appFramework.SparkTestNamespace)
	assert.Equal(t, nil, err)

	err = appFramework.CreateSparkApplication(framework.SparkApplicationClient, appFramework.SparkTestNamespace, sa)
	assert.Equal(t, nil, err)

	status := GetJobStatus(t, appName)
	err = wait.Poll(INTERVAL, TIMEOUT, func() (done bool, err error) {
		if status == "RUNNING" {
			return true, nil
		}
		status = GetJobStatus(t, appName)
		return false, nil
	})
	assert.Equal(t, nil, err)

	app, err := appFramework.GetSparkApplication(framework.SparkApplicationClient, appFramework.SparkTestNamespace, appName)
	assert.Equal(t, nil, err)
	podName := app.Status.DriverInfo.PodName

	describeClient := &describeClient{T: t, Namespace: appFramework.SparkTestNamespace, Interface: framework.KubeClient}
	describer := describe.PodDescriber{Interface: describeClient}

	podDesc, err := describer.Describe(appFramework.SparkTestNamespace, podName, describe.DescriberSettings{ShowEvents: true})
	assert.Equal(t, nil, err)

	matched, err := regexp.MatchString(`dummy-cm`, podDesc)
	assert.Equal(t, true, matched)
	assert.Equal(t, nil, err)

	err = appFramework.DeleteSparkApplication(framework.SparkApplicationClient, appFramework.SparkTestNamespace, appName)
	assert.Equal(t, nil, err)
}
