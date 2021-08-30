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
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	appFramework "github.com/GoogleCloudPlatform/spark-on-k8s-operator/test/e2e/framework"
)

func TestSubmitSparkPiYaml(t *testing.T) {
	t.Parallel()

	appName := "spark-pi"
	sa, err := appFramework.MakeSparkApplicationFromYaml("../../examples/spark-pi.yaml")
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

	err = appFramework.CreateSparkApplication(framework.SparkApplicationClient, appFramework.SparkTestNamespace, sa)
	assert.Equal(t, nil, err)

	status := GetJobStatus(t, appName)

	err = wait.Poll(INTERVAL, TIMEOUT, func() (done bool, err error) {
		if status == "COMPLETED" {
			return true, nil
		}
		status = GetJobStatus(t, appName)
		return false, nil
	})
	assert.Equal(t, nil, err)

	app, _ := appFramework.GetSparkApplication(framework.SparkApplicationClient, appFramework.SparkTestNamespace, appName)
	podName := app.Status.DriverInfo.PodName
	rawLogs, err := framework.KubeClient.CoreV1().Pods(appFramework.SparkTestNamespace).GetLogs(podName, &v1.PodLogOptions{}).Do(context.TODO()).Raw()
	assert.Equal(t, nil, err)
	assert.NotEqual(t, -1, strings.Index(string(rawLogs), "Pi is roughly 3"))

	err = appFramework.DeleteSparkApplication(framework.SparkApplicationClient, appFramework.SparkTestNamespace, appName)
	assert.Equal(t, nil, err)
}

func TestSubmitSparkPiCustomResourceYaml(t *testing.T) {
	t.Parallel()

	appName := "spark-pi-custom-resource"
	sa, err := appFramework.MakeSparkApplicationFromYaml("../../examples/spark-pi-custom-resource.yaml")
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

	err = appFramework.CreateSparkApplication(framework.SparkApplicationClient, appFramework.SparkTestNamespace, sa)
	assert.Equal(t, nil, err)

	status := GetJobStatus(t, appName)

	err = wait.Poll(INTERVAL, TIMEOUT, func() (done bool, err error) {
		if status == "COMPLETED" {
			return true, nil
		}
		status = GetJobStatus(t, appName)
		return false, nil
	})
	assert.Equal(t, nil, err)

	app, _ := appFramework.GetSparkApplication(framework.SparkApplicationClient, appFramework.SparkTestNamespace, appName)
	podName := app.Status.DriverInfo.PodName
	rawLogs, err := framework.KubeClient.CoreV1().Pods(appFramework.SparkTestNamespace).GetLogs(podName, &v1.PodLogOptions{}).Do(context.TODO()).Raw()
	assert.Equal(t, nil, err)
	assert.NotEqual(t, -1, strings.Index(string(rawLogs), "Pi is roughly 3"))

	err = appFramework.DeleteSparkApplication(framework.SparkApplicationClient, appFramework.SparkTestNamespace, appName)
	assert.Equal(t, nil, err)
}
