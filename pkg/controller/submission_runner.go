package controller

import (
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/golang/glog"

	"k8s.io/apimachinery/pkg/util/wait"
)

// SparkSubmitRunner is responsible for running user-specified Spark applications.
type SparkSubmitRunner struct {
	workers int
	queue   chan []string
}

func newRunner(workers int) *SparkSubmitRunner {
	return &SparkSubmitRunner{
		workers: workers,
		queue:   make(chan []string, workers),
	}
}

func (r *SparkSubmitRunner) start(stopCh <-chan struct{}) {
	glog.Info("Starting the spark-submit runner")
	defer glog.Info("Shutting down the spark-submit runner")

	for i := 0; i < r.workers; i++ {
		go wait.Until(r.runWorker, time.Second, stopCh)
	}

	<-stopCh
}

func (r *SparkSubmitRunner) runWorker() {
	sparkHome, present := os.LookupEnv(sparkHomeEnvVar)
	if !present {
		glog.Error("SPARK_HOME is not specified")
	}
	var command = filepath.Join(sparkHome, "/bin/spark-submit")

	for sparkSubmitCommandArgs := range r.queue {
		cmd := exec.Command(command, sparkSubmitCommandArgs...)
		glog.Infof("spark-submit arguments: %v", cmd.Args)
		if bytes, err := cmd.Output(); err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				glog.Errorf("Failed to run spark-submit command: %s", string(exitErr.Stderr))
			}
		} else {
			glog.Infof("spark-submit output: %s", string(bytes))
		}
	}
}

func (r *SparkSubmitRunner) addSparkSubmitCommand(sparkSubmitCommandArgs []string) {
	r.queue <- sparkSubmitCommandArgs
}
