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

	"github.com/liyinan926/spark-operator/pkg/config"
	"github.com/stretchr/testify/assert"

	"github.com/liyinan926/spark-operator/pkg/apis/v1alpha1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclientfake "k8s.io/client-go/kubernetes/fake"
)

func TestOnPodAdded(t *testing.T) {
	monitor, driverStateReportingChan, executorStateReportingChan := newMonitor()

	driverPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo-driver",
			Labels: map[string]string{
				sparkRoleLabel:         sparkDriverRole,
				config.SparkAppIDLabel: "foo-123",
			},
		},
		Status: apiv1.PodStatus{
			Phase: apiv1.PodPending,
		},
	}
	go monitor.onPodAdded(driverPod)
	driverUpdate := <-driverStateReportingChan
	assert.Equal(
		t,
		driverUpdate.podName,
		driverPod.Name,
		"wanted driver pod name %s got %s",
		driverPod.Name,
		driverUpdate.podName)
	assert.Equal(
		t,
		driverUpdate.podPhase,
		apiv1.PodPending,
		"wanted driver pod phase %s got %s",
		apiv1.PodPending,
		driverUpdate.podPhase)

	executorPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo-driver",
			Labels: map[string]string{
				sparkRoleLabel:         sparkExecutorRole,
				config.SparkAppIDLabel: "foo-123",
				sparkExecutorIDLabel:   "1",
			},
		},
		Status: apiv1.PodStatus{
			Phase: apiv1.PodRunning,
		},
	}
	go monitor.onPodAdded(executorPod)
	executorUpdate := <-executorStateReportingChan
	assert.Equal(
		t,
		executorUpdate.podName,
		executorPod.Name,
		"wanted executor pod name %s got %s",
		executorPod.Name,
		executorUpdate.podName)
	assert.Equal(
		t,
		executorUpdate.state,
		v1alpha1.ExecutorRunningState,
		"wanted executor state %s got %s",
		v1alpha1.ExecutorRunningState,
		executorUpdate.state)
}

func TestOnPodUpdated(t *testing.T) {
	monitor, driverStateReportingChan, executorStateReportingChan := newMonitor()

	oldDriverPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo-driver",
			Labels: map[string]string{
				sparkRoleLabel:         sparkDriverRole,
				config.SparkAppIDLabel: "foo-123",
			},
		},
		Status: apiv1.PodStatus{
			Phase: apiv1.PodPending,
		},
	}
	newDriverPod := oldDriverPod.DeepCopy()
	newDriverPod.Status.Phase = apiv1.PodSucceeded
	go monitor.onPodUpdated(oldDriverPod, newDriverPod)
	driverUpdate := <-driverStateReportingChan
	assert.Equal(
		t,
		driverUpdate.podName,
		newDriverPod.Name,
		"wanted driver pod name %s got %s",
		newDriverPod.Name,
		driverUpdate.podName)
	assert.Equal(
		t,
		driverUpdate.podPhase,
		apiv1.PodSucceeded,
		"wanted driver pod phase %s got %s",
		apiv1.PodSucceeded,
		driverUpdate.podPhase)

	oldExecutorPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo-driver",
			Labels: map[string]string{
				sparkRoleLabel:         sparkExecutorRole,
				config.SparkAppIDLabel: "foo-123",
				sparkExecutorIDLabel:   "1",
			},
		},
		Status: apiv1.PodStatus{
			Phase: apiv1.PodRunning,
		},
	}
	newExecutorPod := oldExecutorPod.DeepCopy()
	newExecutorPod.Status.Phase = apiv1.PodFailed
	go monitor.onPodUpdated(oldExecutorPod, newExecutorPod)
	executorUpdate := <-executorStateReportingChan
	assert.Equal(
		t,
		executorUpdate.podName,
		newExecutorPod.Name,
		"wanted executor pod name %s got %s",
		newExecutorPod.Name,
		executorUpdate.podName)
	assert.Equal(
		t,
		executorUpdate.state,
		v1alpha1.ExecutorFailedState,
		"wanted executor state %s got %s",
		v1alpha1.ExecutorFailedState,
		executorUpdate.state)
}

func TestOnPodDeleted(t *testing.T) {
	monitor, _, executorStateReportingChan := newMonitor()

	executorPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo-driver",
			Labels: map[string]string{
				sparkRoleLabel:         sparkExecutorRole,
				config.SparkAppIDLabel: "foo-123",
				sparkExecutorIDLabel:   "1",
			},
		},
		Status: apiv1.PodStatus{
			Phase: apiv1.PodSucceeded,
		},
	}
	go monitor.onPodDeleted(executorPod)
	executorUpdate := <-executorStateReportingChan
	assert.Equal(
		t,
		executorUpdate.podName,
		executorPod.Name,
		"wanted executor pod name %s got %s",
		executorPod.Name,
		executorUpdate.podName)
	assert.Equal(
		t,
		executorUpdate.state,
		v1alpha1.ExecutorCompletedState,
		"wanted executor state %s got %s",
		v1alpha1.ExecutorCompletedState,
		executorUpdate.state)

	executorPod.Status.Phase = apiv1.PodFailed
	go monitor.onPodDeleted(executorPod)
	executorUpdate = <-executorStateReportingChan
	assert.Equal(
		t,
		executorUpdate.podName,
		executorPod.Name,
		"wanted executor pod name %s got %s",
		executorPod.Name,
		executorUpdate.podName)
	assert.Equal(
		t,
		executorUpdate.state,
		v1alpha1.ExecutorFailedState,
		"wanted executor state %s got %s",
		v1alpha1.ExecutorFailedState,
		executorUpdate.state)
}

func newMonitor() (*sparkPodMonitor, <-chan driverStateUpdate, <-chan executorStateUpdate) {
	driverStateReportingChan := make(chan driverStateUpdate)
	executorStateReportingChan := make(chan executorStateUpdate)
	monitor := newSparkPodMonitor(kubeclientfake.NewSimpleClientset(), driverStateReportingChan, executorStateReportingChan)
	return monitor, driverStateReportingChan, executorStateReportingChan
}
