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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	appFramework "github.com/GoogleCloudPlatform/spark-on-k8s-operator/test/e2e/framework"
)

func getJobStatus(t *testing.T) v1alpha1.ApplicationStateType {
	app, err := appFramework.GetSparkApplication(framework.SparkApplicationClient, "default", "spark-pi")
	assert.Equal(t, nil, err)
	return app.Status.AppState.State
}

func TestSubmitSparkPiYaml(t *testing.T) {
	t.Parallel()

	// Wait for test job to finish. Time out after 90 seconds.
	timeout := 100 * time.Second
	interval := 5 * time.Second

	sa, err := appFramework.MakeSparkApplicationFromYaml("../../examples/spark-pi.yaml")
	assert.Equal(t, nil, err)
	err = appFramework.CreateSparkApplication(framework.SparkApplicationClient, "default", sa)
	assert.Equal(t, nil, err)

	status := getJobStatus(t)

	wait.Poll(interval, timeout, func() (done bool, err error) {
		if status == "COMPLETED" {
			return true, nil
		}
		status = getJobStatus(t)
		return false, nil
	})

	app, _ := appFramework.GetSparkApplication(framework.SparkApplicationClient, "default", "spark-pi")
	podName := app.Status.DriverInfo.PodName
	rawLogs, err := framework.KubeClient.CoreV1().Pods("default").GetLogs(podName, &v1.PodLogOptions{}).Do().Raw()
	assert.Equal(t, nil, err)
	assert.NotEqual(t, -1, strings.Index(string(rawLogs), "Pi is roughly 3"))

	err = appFramework.DeleteSparkApplication(framework.SparkApplicationClient, "default", "spark-pi")
	assert.Equal(t, nil, err)
}
