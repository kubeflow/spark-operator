package controller

import (
	"os/exec"
	"time"

	"github.com/golang/glog"

	"k8s.io/apimachinery/pkg/util/wait"
)

// SparkSubmitRunner is responsible for running user-specified Spark applications.
type SparkSubmitRunner struct {
	workers int
	queue   chan string
}

func newRunner(workers int) *SparkSubmitRunner {
	return &SparkSubmitRunner{
		workers: workers,
		queue:   make(chan string, workers),
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
	for sparkSubmitCommand := range r.queue {
		err := exec.Command(sparkSubmitCommand).Run()
		if err != nil {
			glog.Errorf("Failed to run spark-submit command [%s]: %v", sparkSubmitCommand, err)
		}
	}
}

func (r *SparkSubmitRunner) addSparkSubmitCommand(sparkSubmitCommand string) {
	r.queue <- sparkSubmitCommand
}
