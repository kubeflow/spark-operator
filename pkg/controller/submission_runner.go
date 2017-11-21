package controller

import (
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/liyinan926/spark-operator/pkg/apis/v1alpha1"

	"github.com/golang/glog"

	"k8s.io/apimachinery/pkg/util/wait"
)

// sparkSubmitRunner is responsible for running user-specified Spark applications.
type sparkSubmitRunner struct {
	workers               int
	queue                 chan *submission
	appStateReportingChan chan<- appStateUpdate
}

// appStateUpdate encapsulates overall state update of a Spark application.
type appStateUpdate struct {
	appID        string
	state        v1alpha1.ApplicationStateType
	errorMessage string
}

func newSparkSubmitRunner(workers int, appStateReportingChan chan<- appStateUpdate) *sparkSubmitRunner {
	return &sparkSubmitRunner{
		workers: workers,
		queue:   make(chan *submission, workers),
		appStateReportingChan: appStateReportingChan,
	}
}

func (r *sparkSubmitRunner) run(stopCh <-chan struct{}) {
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
		stateUpdate := appStateUpdate{appID: s.appID}
		if _, err := cmd.Output(); err != nil {
			stateUpdate.state = v1alpha1.FailedState
			if exitErr, ok := err.(*exec.ExitError); ok {
				glog.Errorf("failed to submit Spark application %s: %s", s.appName, string(exitErr.Stderr))
				stateUpdate.errorMessage = string(exitErr.Stderr)
			}
		} else {
			glog.Infof("Spark application %s completed", s.appName)
			stateUpdate.state = v1alpha1.CompletedState
		}
		// Report the application state back to the controller.
		r.appStateReportingChan <- stateUpdate
	}
}

func (r *sparkSubmitRunner) submit(s *submission) {
	r.queue <- s
}
