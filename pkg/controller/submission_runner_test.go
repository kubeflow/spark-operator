package controller

import (
	"testing"

	"github.com/liyinan926/spark-operator/pkg/apis/v1alpha1"
	"github.com/stretchr/testify/assert"
)

func TestNewRunner(t *testing.T) {
	runner := newRunner(3)
	assert.Equal(t, runner.workers, 3, "number of workers should be 3")
	assert.Equal(t, cap(runner.queue), 3, "capacity of the work queue should be 3")
}

func TestSubmit(t *testing.T) {
	runner := newRunner(1)
	app := &v1alpha1.SparkApplication{}
	submitCommandArgs := []string{"--master", "localhost", "-class", "foo"}
	go func() {
		runner.submit(newSubmission(submitCommandArgs, app))
	}()
	s := <-runner.queue
	assert.Equal(t, s.args, submitCommandArgs, "received and added arguments should be equal")
	assert.Equal(t, s.app, app, "received and added SparkApplication should be equal")
}
