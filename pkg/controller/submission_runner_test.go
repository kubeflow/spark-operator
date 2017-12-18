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

package controller

import (
	"testing"

	"github.com/liyinan926/spark-operator/pkg/apis/v1alpha1"
	"github.com/stretchr/testify/assert"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNewRunner(t *testing.T) {
	appStateReportingChan := make(chan<- appStateUpdate)
	runner := newSparkSubmitRunner(3, appStateReportingChan)
	assert.Equal(t, runner.workers, 3, "number of workers should be 3")
	assert.Equal(t, cap(runner.queue), 3, "capacity of the work queue should be 3")
}

func TestSubmit(t *testing.T) {
	appStateReportingChan := make(chan<- appStateUpdate)
	runner := newSparkSubmitRunner(1, appStateReportingChan)
	app := &v1alpha1.SparkApplication{ObjectMeta: metav1.ObjectMeta{Name: "foo"}}
	submitCommandArgs := []string{"--master", "localhost", "-class", "foo"}
	go func() {
		runner.submit(newSubmission(submitCommandArgs, app))
	}()
	s := <-runner.queue
	assert.Equal(t, s.args, submitCommandArgs, "arguments of received and added submissions should be equal")
	assert.Equal(t, s.appName, "foo", "names of received and added submissions should be equal")
}
