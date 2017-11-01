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

// SparkSubmitRunner is responsible for running user-specified Spark applications.
type SparkSubmitRunner struct {
	workers               int
	queue                 chan *submission
	appStateReportingChan chan *v1alpha1.SparkApplication
}

func newRunner(workers int, appStateReportingChan chan *v1alpha1.SparkApplication) *SparkSubmitRunner {
	return &SparkSubmitRunner{
		workers: workers,
		queue:   make(chan *submission, workers),
		appStateReportingChan: appStateReportingChan,
	}
}

func (r *SparkSubmitRunner) start(stopCh <-chan struct{}) {
	glog.Info("Starting the spark-submit runner")
	defer glog.Info("Shutting down the spark-submit runner")

	for i := 0; i < r.workers; i++ {
		go wait.Until(r.runWorker, time.Second, stopCh)
	}

	<-stopCh
	close(r.appStateReportingChan)
}

func (r *SparkSubmitRunner) runWorker() {
	sparkHome, present := os.LookupEnv(sparkHomeEnvVar)
	if !present {
		glog.Error("SPARK_HOME is not specified")
	}
	var command = filepath.Join(sparkHome, "/bin/spark-submit")

	for s := range r.queue {
		cmd := exec.Command(command, s.args...)
		glog.Infof("spark-submit arguments: %v", cmd.Args)
		if _, err := cmd.Output(); err != nil {
			s.app.Status.AppState.State = v1alpha1.FailedState
			if exitErr, ok := err.(*exec.ExitError); ok {
				glog.Errorf("Spark application %s failed: %s", s.app.Name, string(exitErr.Stderr))
				s.app.Status.AppState.ErrorMessage = string(exitErr.Stderr)
			}
		} else {
			glog.Infof("Spark application %s completed", s.app.Name)
			s.app.Status.AppState.State = v1alpha1.CompletedState
		}
		// Report the application state back to the controller.
		r.appStateReportingChan <- s.app
	}
}

func (r *SparkSubmitRunner) submit(s *submission) {
	s.app.Status.AppState.State = v1alpha1.SubmittedState
	r.queue <- s
}
