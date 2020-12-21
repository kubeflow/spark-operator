/*
Copyright 2019 Google LLC

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
	"container/list"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	appFramework "github.com/GoogleCloudPlatform/spark-on-k8s-operator/test/e2e/framework"
)

func TestLifeCycleManagement(t *testing.T) {
	appName := "spark-pi"
	app, err := appFramework.MakeSparkApplicationFromYaml("../../examples/spark-pi.yaml")
	assert.Equal(t, nil, err)

	if appFramework.SparkTestNamespace != "" {
		app.ObjectMeta.Namespace = appFramework.SparkTestNamespace
	}

	if appFramework.SparkTestServiceAccount != "" {
		app.Spec.Driver.ServiceAccount = &appFramework.SparkTestServiceAccount
	}

	if appFramework.SparkTestImage != "" {
		app.Spec.Image = &appFramework.SparkTestImage
	}

	err = appFramework.CreateSparkApplication(framework.SparkApplicationClient, appFramework.SparkTestNamespace, app)
	assert.Equal(t, nil, err)

	states := list.New()
	status := GetJobStatus(t, appName)
	states.PushBack(status)

	app = runApp(t, appName, states)

	newNumExecutors := int32(2)
	app.Spec.Executor.Instances = &newNumExecutors
	err = appFramework.UpdateSparkApplication(framework.SparkApplicationClient, appFramework.SparkTestNamespace, app)
	assert.Equal(t, nil, err)

	status = GetJobStatus(t, appName)
	if status != states.Back().Value {
		states.PushBack(status)
	}

	runApp(t, appName, states)

	assert.Equal(t, len(STATES), states.Len())
	index := 0
	for e := states.Front(); e != nil; e = e.Next() {
		assert.Equal(t, STATES[index], string((e.Value).(v1beta2.ApplicationStateType)))
		index += 1
	}

	err = appFramework.DeleteSparkApplication(framework.SparkApplicationClient, appFramework.SparkTestNamespace, appName)
	assert.Equal(t, nil, err)
}

func runApp(t *testing.T, appName string, states *list.List) *v1beta2.SparkApplication {
	err := wait.Poll(INTERVAL, TIMEOUT, func() (done bool, err error) {
		status := GetJobStatus(t, appName)
		if status != states.Back().Value {
			states.PushBack(status)
		}
		if status == "COMPLETED" {
			return true, nil
		}
		return false, nil
	})
	assert.Equal(t, nil, err)

	app, _ := appFramework.GetSparkApplication(framework.SparkApplicationClient, appFramework.SparkTestNamespace, appName)
	podName := app.Status.DriverInfo.PodName
	rawLogs, err := framework.KubeClient.CoreV1().Pods(appFramework.SparkTestNamespace).GetLogs(podName, &v1.PodLogOptions{}).Do(context.TODO()).Raw()
	assert.Equal(t, nil, err)
	assert.NotEqual(t, -1, strings.Index(string(rawLogs), "Pi is roughly 3"))

	return app
}
