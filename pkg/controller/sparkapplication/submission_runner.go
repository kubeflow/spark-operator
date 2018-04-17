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

package sparkapplication

import (
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/golang/glog"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"

	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
)

// sparkSubmitRunner is responsible for running user-specified Spark applications.
type sparkSubmitRunner struct {
	workers               int
	queue                 chan *submission
	appStateReportingChan chan<- *appStateUpdate
}

// appStateUpdate encapsulates overall state update of a Spark application.
type appStateUpdate struct {
	namespace      string
	name           string
	submissionTime metav1.Time
	state          v1alpha1.ApplicationStateType
	errorMessage   string
}

func newSparkSubmitRunner(workers int, appStateReportingChan chan<- *appStateUpdate) *sparkSubmitRunner {
	return &sparkSubmitRunner{
		workers: workers,
		queue:   make(chan *submission, workers),
		appStateReportingChan: appStateReportingChan,
	}
}

func (r *sparkSubmitRunner) run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()

	glog.Info("Starting the spark-submit runner")
	defer glog.Info("Stopping the spark-submit runner")

	for i := 0; i < r.workers; i++ {
		go wait.Until(r.runWorker, time.Second, stopCh)
	}

	<-stopCh
	close(r.appStateReportingChan)
}

func (r *sparkSubmitRunner) runWorker() {
	sparkHome, present := os.LookupEnv(sparkHomeEnvVar)
	if !present {
		glog.Error("SPARK_HOME is not specified")
	}
	var command = filepath.Join(sparkHome, "/bin/spark-submit")

	for s := range r.queue {
		cmd := exec.Command(command, s.args...)
		glog.Infof("spark-submit arguments: %v", cmd.Args)

		// Send an application state update that tells if the submission succeeded or failed.
		// Once the application is submitted, the application state is solely based on the driver pod phase.
		stateUpdate := appStateUpdate{
			namespace:      s.namespace,
			name:           s.name,
			submissionTime: metav1.Now(),
		}

		if _, err := cmd.Output(); err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				glog.Errorf("failed to run spark-submit for SparkApplication %s in namespace %s: %s", s.name,
					s.namespace, string(exitErr.Stderr))
				stateUpdate.state = v1alpha1.FailedSubmissionState
				stateUpdate.errorMessage = string(exitErr.Stderr)
			}
		} else {
			glog.Infof("spark-submit completed for SparkApplication %s in namespace %s", s.name, s.namespace)
			stateUpdate.state = v1alpha1.SubmittedState
		}

		r.appStateReportingChan <- &stateUpdate
	}
}

func (r *sparkSubmitRunner) submit(s *submission) {
	r.queue <- s
}
