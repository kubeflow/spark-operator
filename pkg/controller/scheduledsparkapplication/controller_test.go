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

package scheduledsparkapplication

import (
	"testing"
	"time"

	"github.com/robfig/cron"
	"github.com/stretchr/testify/assert"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/clock"
	kubeclientfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	crdclientfake "k8s.io/spark-on-k8s-operator/pkg/client/clientset/versioned/fake"
	crdinformers "k8s.io/spark-on-k8s-operator/pkg/client/informers/externalversions"
)

func TestSyncScheduledSparkApplication(t *testing.T) {
	var two int32 = 2
	app := &v1alpha1.ScheduledSparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test-app",
		},
		Spec: v1alpha1.ScheduledSparkApplicationSpec{
			Schedule:          "@every 1m",
			ConcurrencyPolicy: v1alpha1.ConcurrencyAllow,
			RunHistoryLimit:   &two,
		},
	}
	c, informer, clk := newFakeController(app)
	key, _ := cache.MetaNamespaceKeyFunc(app)

	// This sync should start the first run.
	err := c.syncScheduledSparkApplication(key)
	if err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(app.Namespace).Get(app.Name,
		metav1.GetOptions{})
	assert.Equal(t, v1alpha1.ScheduledState, app.Status.ScheduleState)
	// The first run should have been started.
	assert.Equal(t, 1, len(app.Status.PastRunNames))
	assert.False(t, app.Status.LastRun.IsZero())
	assert.True(t, app.Status.NextRun.After(app.Status.LastRun.Time))
	run, _ := c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(app.Status.PastRunNames[0],
		metav1.GetOptions{})
	assert.NotNil(t, run)

	informer.GetIndexer().Add(app)
	c.crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(app.Namespace).Update(app)
	// This sync should not start any run.
	err = c.syncScheduledSparkApplication(key)
	if err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(app.Namespace).Get(app.Name,
		metav1.GetOptions{})
	assert.Equal(t, v1alpha1.ScheduledState, app.Status.ScheduleState)
	// Next run is not due, so there's still only one past run.
	assert.Equal(t, 1, len(app.Status.PastRunNames))

	informer.GetIndexer().Add(app)
	c.crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(app.Namespace).Update(app)
	// Advance the clock to trigger the next run.
	clk.SetTime(app.Status.NextRun.Time.Add(5 * time.Second))
	// This sync should start the second run.
	err = c.syncScheduledSparkApplication(key)
	if err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(app.Namespace).Get(app.Name,
		metav1.GetOptions{})
	assert.Equal(t, v1alpha1.ScheduledState, app.Status.ScheduleState)
	// The second run should have been started.
	assert.Equal(t, 2, len(app.Status.PastRunNames))
	run, _ = c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(app.Status.PastRunNames[0],
		metav1.GetOptions{})
	assert.NotNil(t, run)
	run, _ = c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(app.Status.PastRunNames[1],
		metav1.GetOptions{})
	assert.NotNil(t, run)
}

func TestShouldStartNextRun(t *testing.T) {
	app := &v1alpha1.ScheduledSparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test-app",
		},
		Spec: v1alpha1.ScheduledSparkApplicationSpec{
			Schedule: "@every 1m",
		},
		Status: v1alpha1.ScheduledSparkApplicationStatus{
			PastRunNames: []string{"run1"},
		},
	}
	c, _, _ := newFakeController(app)

	run1 := &v1alpha1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: app.Namespace,
			Name:      "run1",
		},
	}
	c.crdClient.SparkoperatorV1alpha1().SparkApplications(run1.Namespace).Create(run1)

	// ConcurrencyAllow with a running run.
	run1.Status.AppState.State = v1alpha1.RunningState
	c.crdClient.SparkoperatorV1alpha1().SparkApplications(run1.Namespace).Update(run1)
	app.Spec.ConcurrencyPolicy = v1alpha1.ConcurrencyAllow
	ok, _ := c.shouldStartNextRun(app)
	assert.True(t, ok)

	// ConcurrencyForbid with a running run.
	app.Spec.ConcurrencyPolicy = v1alpha1.ConcurrencyForbid
	ok, _ = c.shouldStartNextRun(app)
	assert.False(t, ok)
	// ConcurrencyForbid with a completed run.
	run1.Status.AppState.State = v1alpha1.CompletedState
	c.crdClient.SparkoperatorV1alpha1().SparkApplications(run1.Namespace).Update(run1)
	ok, _ = c.shouldStartNextRun(app)
	assert.True(t, ok)

	// ConcurrencyReplace with a completed run.
	app.Spec.ConcurrencyPolicy = v1alpha1.ConcurrencyReplace
	ok, _ = c.shouldStartNextRun(app)
	assert.True(t, ok)
	// ConcurrencyReplace with a running run.
	run1.Status.AppState.State = v1alpha1.RunningState
	c.crdClient.SparkoperatorV1alpha1().SparkApplications(run1.Namespace).Update(run1)
	ok, _ = c.shouldStartNextRun(app)
	assert.True(t, ok)
	// The previous running run should have been deleted.
	existing, _ := c.crdClient.SparkoperatorV1alpha1().SparkApplications(run1.Namespace).Get(run1.Name,
		metav1.GetOptions{})
	assert.Nil(t, existing)
}

func TestStartNextRun(t *testing.T) {
	app := &v1alpha1.ScheduledSparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test-app",
		},
		Spec: v1alpha1.ScheduledSparkApplicationSpec{
			Schedule: "@every 1m",
		},
	}
	c, _, _ := newFakeController(app)

	schedule, _ := cron.ParseStandard(app.Spec.Schedule)
	status := app.Status.DeepCopy()

	err := c.startNextRun(app, status, schedule)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(status.PastRunNames))
	assert.True(t, status.NextRun.After(status.LastRun.Time))
	// Check the first run.
	firstRunName := status.PastRunNames[0]
	run, _ := c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(firstRunName, metav1.GetOptions{})
	assert.NotNil(t, run)

	err = c.startNextRun(app, status, schedule)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(status.PastRunNames))
	assert.True(t, status.NextRun.After(status.LastRun.Time))
	// Check the second run.
	secondRunName := status.PastRunNames[0]
	run, _ = c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(secondRunName, metav1.GetOptions{})
	assert.NotNil(t, run)
	// The second run should have a different name.
	assert.NotEqual(t, secondRunName, firstRunName)
	// The first run should have been deleted.
	run, _ = c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(firstRunName, metav1.GetOptions{})
	assert.Nil(t, run)
}

func newFakeController(
	apps ...*v1alpha1.ScheduledSparkApplication) (*Controller, cache.SharedIndexInformer, *clock.FakeClock) {
	crdClient := crdclientfake.NewSimpleClientset()
	kubeClient := kubeclientfake.NewSimpleClientset()
	apiExtensionsClient := apiextensionsfake.NewSimpleClientset()
	informerFactory := crdinformers.NewSharedInformerFactory(crdClient, 1*time.Second)
	clk := clock.NewFakeClock(time.Now())
	controller := NewController(crdClient, kubeClient, apiExtensionsClient, informerFactory, clk)

	informer := informerFactory.Sparkoperator().V1alpha1().ScheduledSparkApplications().Informer()
	for _, app := range apps {
		crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(app.Namespace).Create(app)
		informer.GetIndexer().Add(app)
	}

	return controller, informer, clk
}
