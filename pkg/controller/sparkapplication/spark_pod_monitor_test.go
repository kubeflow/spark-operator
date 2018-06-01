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
	"testing"

	"github.com/stretchr/testify/assert"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclientfake "k8s.io/client-go/kubernetes/fake"

	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	"k8s.io/spark-on-k8s-operator/pkg/config"
)

func TestOnPodAdded(t *testing.T) {
	monitor, podStateReportingChan := newMonitor()

	driverPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo-driver",
			Namespace: "default",
			Labels: map[string]string{
				sparkRoleLabel:           sparkDriverRole,
				config.SparkAppIDLabel:   "foo-123",
				config.SparkAppNameLabel: "foo",
			},
		},
		Status: apiv1.PodStatus{
			Phase: apiv1.PodPending,
		},
	}
	go monitor.onPodAdded(driverPod)

	update := <-podStateReportingChan
	driverUpdate := update.(*driverStateUpdate)
	assert.Equal(
		t,
		driverPod.Name,
		driverUpdate.podName,
		"wanted driver pod name %s got %s",
		driverPod.Name,
		driverUpdate.podName)
	assert.Equal(
		t,
		apiv1.PodPending,
		driverUpdate.podPhase,
		"wanted driver pod phase %s got %s",
		apiv1.PodPending,
		driverUpdate.podPhase)

	executorPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo-driver",
			Labels: map[string]string{
				sparkRoleLabel:           sparkExecutorRole,
				config.SparkAppIDLabel:   "foo-123",
				config.SparkAppNameLabel: "foo",
				sparkExecutorIDLabel:     "1",
			},
		},
		Status: apiv1.PodStatus{
			Phase: apiv1.PodRunning,
		},
	}
	go monitor.onPodAdded(executorPod)

	update = <-podStateReportingChan
	executorUpdate := update.(*executorStateUpdate)
	assert.Equal(
		t,
		executorPod.Name,
		executorUpdate.podName,
		"wanted executor pod name %s got %s",
		executorPod.Name,
		executorUpdate.podName)
	assert.Equal(
		t,
		v1alpha1.ExecutorRunningState,
		executorUpdate.state,
		"wanted executor state %s got %s",
		v1alpha1.ExecutorRunningState,
		executorUpdate.state)
}

func TestOnPodUpdated(t *testing.T) {
	monitor, podStateReportingChan := newMonitor()

	oldDriverPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo-driver",
			Labels: map[string]string{
				sparkRoleLabel:           sparkDriverRole,
				config.SparkAppIDLabel:   "foo-123",
				config.SparkAppNameLabel: "foo",
			},
			ResourceVersion: "1",
		},
		Status: apiv1.PodStatus{
			Phase: apiv1.PodPending,
		},
	}
	newDriverPod := oldDriverPod.DeepCopy()
	newDriverPod.ResourceVersion = "2"
	newDriverPod.Status.Phase = apiv1.PodSucceeded
	go monitor.onPodUpdated(oldDriverPod, newDriverPod)

	update := <-podStateReportingChan
	driverUpdate := update.(*driverStateUpdate)
	assert.Equal(
		t,
		newDriverPod.Name,
		driverUpdate.podName,
		"wanted driver pod name %s got %s",
		newDriverPod.Name,
		driverUpdate.podName)
	assert.Equal(
		t,
		apiv1.PodSucceeded,
		driverUpdate.podPhase,
		"wanted driver pod phase %s got %s",
		apiv1.PodSucceeded,
		driverUpdate.podPhase)

	oldExecutorPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo-driver",
			Labels: map[string]string{
				sparkRoleLabel:           sparkExecutorRole,
				config.SparkAppIDLabel:   "foo-123",
				config.SparkAppNameLabel: "foo",
				sparkExecutorIDLabel:     "1",
			},
			ResourceVersion: "1",
		},
		Status: apiv1.PodStatus{
			Phase: apiv1.PodRunning,
		},
	}
	newExecutorPod := oldExecutorPod.DeepCopy()
	newExecutorPod.ResourceVersion = "2"
	newExecutorPod.Status.Phase = apiv1.PodFailed
	go monitor.onPodUpdated(oldExecutorPod, newExecutorPod)

	update = <-podStateReportingChan
	executorUpdate := update.(*executorStateUpdate)
	assert.Equal(
		t,
		newExecutorPod.Name,
		executorUpdate.podName,
		"wanted executor pod name %s got %s",
		newExecutorPod.Name,
		executorUpdate.podName)
	assert.Equal(
		t,
		v1alpha1.ExecutorFailedState,
		executorUpdate.state,
		"wanted executor state %s got %s",
		v1alpha1.ExecutorFailedState,
		executorUpdate.state)
}

func TestOnPodDeleted(t *testing.T) {
	monitor, podStateReportingChan := newMonitor()

	driverPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo-driver",
			Labels: map[string]string{
				sparkRoleLabel:           sparkDriverRole,
				config.SparkAppIDLabel:   "foo-123",
				config.SparkAppNameLabel: "foo",
			},
		},
		Status: apiv1.PodStatus{
			Phase: apiv1.PodRunning,
		},
	}
	go monitor.onPodDeleted(driverPod)

	update := <-podStateReportingChan
	driverUpdate := update.(*driverStateUpdate)
	assert.Equal(
		t,
		driverPod.Name,
		driverUpdate.podName,
		"wanted driver pod name %s got %s",
		driverPod.Name,
		driverUpdate.podName)
	assert.Equal(
		t,
		apiv1.PodFailed,
		driverUpdate.podPhase,
		"wanted driver pod phase %s got %s",
		apiv1.PodFailed,
		driverUpdate.podPhase)

	driverPod.Status.Phase = apiv1.PodSucceeded
	go monitor.onPodDeleted(driverPod)
	assert.True(t, len(podStateReportingChan) == 0)

	executorPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo-exec-1",
			Labels: map[string]string{
				sparkRoleLabel:           sparkExecutorRole,
				config.SparkAppIDLabel:   "foo-123",
				config.SparkAppNameLabel: "foo",
				sparkExecutorIDLabel:     "1",
			},
		},
		Status: apiv1.PodStatus{
			Phase: apiv1.PodSucceeded,
		},
	}
	go monitor.onPodDeleted(executorPod)

	update = <-podStateReportingChan
	executorUpdate := update.(*executorStateUpdate)
	assert.Equal(
		t,
		executorPod.Name,
		executorUpdate.podName,
		"wanted executor pod name %s got %s",
		executorPod.Name,
		executorUpdate.podName)
	assert.Equal(
		t,
		v1alpha1.ExecutorCompletedState,
		executorUpdate.state,
		"wanted executor state %s got %s",
		v1alpha1.ExecutorCompletedState,
		executorUpdate.state)

	executorPod.Status.Phase = apiv1.PodFailed
	go monitor.onPodDeleted(executorPod)

	update = <-podStateReportingChan
	executorUpdate = update.(*executorStateUpdate)
	assert.Equal(
		t,
		executorPod.Name,
		executorUpdate.podName,
		"wanted executor pod name %s got %s",
		executorPod.Name,
		executorUpdate.podName)
	assert.Equal(
		t,
		v1alpha1.ExecutorFailedState,
		executorUpdate.state,
		"wanted executor state %s got %s",
		v1alpha1.ExecutorFailedState,
		executorUpdate.state)
}

func newMonitor() (*sparkPodMonitor, <-chan interface{}) {
	podStateReportingChan := make(chan interface{})
	monitor := newSparkPodMonitor(kubeclientfake.NewSimpleClientset(), "test", podStateReportingChan)
	return monitor, podStateReportingChan
}
