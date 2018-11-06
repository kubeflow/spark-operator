/*
Copyright 2018 Google LLC

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
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
)

func TestOnPodAdded(t *testing.T) {
	monitor, queue := newMonitor()

	appName := "foo-1"
	namespace := "foo-namespace"
	driverPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo-driver",
			Namespace: namespace,
			Labels: map[string]string{
				config.SparkRoleLabel:                sparkDriverRole,
				config.SparkApplicationSelectorLabel: "foo-123",
				config.SparkAppNameLabel:             appName,
			},
		},
		Status: apiv1.PodStatus{
			Phase: apiv1.PodPending,
		},
	}
	go monitor.onPodAdded(driverPod)

	key, _ := queue.Get()
	actualNamespace, actualAppName, err := cache.SplitMetaNamespaceKey(key.(string))
	assert.Nil(t, err)

	assert.Equal(
		t,
		appName,
		actualAppName,
		"wanted app name %s got %s",
		appName,
		actualAppName)

	assert.Equal(
		t,
		namespace,
		actualNamespace,
		"wanted app namespace %s got %s",
		namespace,
		actualNamespace)

	appName = "foo-2"
	executorPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo-driver",
			Namespace: "foo-namespace",
			Labels: map[string]string{
				config.SparkRoleLabel:                sparkExecutorRole,
				config.SparkApplicationSelectorLabel: "foo-123",
				config.SparkAppNameLabel:             appName,
				sparkExecutorIDLabel:                 "1",
			},
		},
		Status: apiv1.PodStatus{
			Phase: apiv1.PodRunning,
		},
	}
	go monitor.onPodAdded(executorPod)

	key, _ = queue.Get()

	actualNamespace, actualAppName, err = cache.SplitMetaNamespaceKey(key.(string))
	assert.Nil(t, err)

	assert.Equal(
		t,
		appName,
		actualAppName,
		"wanted app name %s got %s",
		appName,
		actualAppName)

	assert.Equal(
		t,
		namespace,
		actualNamespace,
		"wanted app namespace %s got %s",
		namespace,
		actualNamespace)
}

func TestOnPodUpdated(t *testing.T) {
	monitor, queue := newMonitor()

	appName := "foo-3"
	namespace := "foo-namespace"
	oldDriverPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo-driver",
			Namespace: namespace,
			Labels: map[string]string{
				config.SparkRoleLabel:                sparkDriverRole,
				config.SparkApplicationSelectorLabel: "foo-123",
				config.SparkAppNameLabel:             appName,
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

	key, _ := queue.Get()

	actualNamespace, actualAppName, err := cache.SplitMetaNamespaceKey(key.(string))
	assert.Nil(t, err)

	assert.Equal(
		t,
		appName,
		actualAppName,
		"wanted app name %s got %s",
		appName,
		actualAppName)

	assert.Equal(
		t,
		namespace,
		actualNamespace,
		"wanted app namespace %s got %s",
		namespace,
		actualNamespace)

	appName = "foo-4"
	oldExecutorPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo-driver",
			Namespace: namespace,
			Labels: map[string]string{
				config.SparkRoleLabel:                sparkExecutorRole,
				config.SparkApplicationSelectorLabel: "foo-123",
				config.SparkAppNameLabel:             appName,
				sparkExecutorIDLabel:                 "1",
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

	key, _ = queue.Get()

	actualNamespace, actualAppName, err = cache.SplitMetaNamespaceKey(key.(string))
	assert.Nil(t, err)

	assert.Equal(
		t,
		appName,
		actualAppName,
		"wanted app name %s got %s",
		appName,
		actualAppName)

	assert.Equal(
		t,
		namespace,
		actualNamespace,
		"wanted app namespace %s got %s",
		namespace,
		actualNamespace)
}

func TestOnPodDeleted(t *testing.T) {
	monitor, queue := newMonitor()

	appName := "foo-5"
	namespace := "foo-namespace"
	driverPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo-driver",
			Namespace: namespace,
			Labels: map[string]string{
				config.SparkRoleLabel:                sparkDriverRole,
				config.SparkApplicationSelectorLabel: "foo-123",
				config.SparkAppNameLabel:             appName,
			},
		},
		Status: apiv1.PodStatus{
			Phase: apiv1.PodRunning,
		},
	}
	go monitor.onPodDeleted(driverPod)

	key, _ := queue.Get()
	actualNamespace, actualAppName, err := cache.SplitMetaNamespaceKey(key.(string))
	assert.Nil(t, err)

	assert.Equal(
		t,
		appName,
		actualAppName,
		"wanted app name %s got %s",
		appName,
		actualAppName)

	assert.Equal(
		t,
		namespace,
		actualNamespace,
		"wanted app namespace %s got %s",
		namespace,
		actualNamespace)

	appName = "foo-6"
	executorPod := &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo-exec-1",
			Namespace: namespace,
			Labels: map[string]string{
				config.SparkRoleLabel:                sparkExecutorRole,
				config.SparkApplicationSelectorLabel: "foo-123",
				config.SparkAppNameLabel:             appName,
				sparkExecutorIDLabel:                 "1",
			},
		},
		Status: apiv1.PodStatus{
			Phase: apiv1.PodSucceeded,
		},
	}
	go monitor.onPodDeleted(executorPod)

	key, _ = queue.Get()
	actualNamespace, actualAppName, err = cache.SplitMetaNamespaceKey(key.(string))
	assert.Nil(t, err)

	assert.Equal(
		t,
		appName,
		actualAppName,
		"wanted app name %s got %s",
		appName,
		actualAppName)

	assert.Equal(
		t,
		namespace,
		actualNamespace,
		"wanted app namespace %s got %s",
		namespace,
		actualNamespace)
}

func newMonitor() (*sparkPodEventHandler, workqueue.RateLimitingInterface) {
	queue := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(),
		"spark-application-controller-test")
	monitor := newSparkPodEventHandler(queue.AddRateLimited)
	return monitor, queue
}
