package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRunner(t *testing.T) {
	runner := newRunner(3)
	assert.Equal(t, runner.workers, 3, "number of workers should be 3")
	assert.Equal(t, cap(runner.queue), 3, "capacity of the work queue should be 3")
}

func TestAddSparkSubmitCommand(t *testing.T) {
	runner := newRunner(1)
	submitCommand := "bin/spark-submit foo"
	go func() { runner.addSparkSubmitCommand(submitCommand) }()
	receivedCommand := <-runner.queue
	assert.Equal(t, receivedCommand, submitCommand, "received and added submission commands should be equal")
}
