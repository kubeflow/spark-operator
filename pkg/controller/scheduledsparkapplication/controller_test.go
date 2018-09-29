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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/clock"
	kubeclientfake "k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	crdclientfake "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned/fake"
	crdinformers "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/informers/externalversions"
)

func TestSyncScheduledSparkApplication_Allow(t *testing.T) {
	app := &v1alpha1.ScheduledSparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test-app",
		},
		Spec: v1alpha1.ScheduledSparkApplicationSpec{
			Schedule:          "@every 1m",
			ConcurrencyPolicy: v1alpha1.ConcurrencyAllow,
		},
	}
	c, ssaInformer, saInformer, clk := newFakeController(app)
	key, _ := cache.MetaNamespaceKeyFunc(app)

	options := metav1.GetOptions{}

	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(app.Namespace).Get(app.Name, options)
	assert.Equal(t, v1alpha1.ScheduledState, app.Status.ScheduleState)
	// The first run should not have been triggered.
	assert.True(t, app.Status.LastRunName == "")

	ssaInformer.GetIndexer().Add(app)
	// Advance the clock by 1 minute.
	clk.Step(1 * time.Minute)

	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(app.Namespace).Get(app.Name, options)
	firstRunName := app.Status.LastRunName
	// The first run should have been triggered.
	assert.True(t, firstRunName != "")
	assert.False(t, app.Status.LastRun.IsZero())
	assert.True(t, app.Status.NextRun.Time.After(app.Status.LastRun.Time))
	// The first run exists.
	run, _ := c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(firstRunName, options)
	assert.NotNil(t, run)

	ssaInformer.GetIndexer().Add(app)
	saInformer.GetIndexer().Add(run)
	clk.Step(5 * time.Second)
	// The second sync should not start any new run.
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(app.Namespace).Get(app.Name, options)
	// Next run is not due, so LastRunName should stay the same.
	assert.Equal(t, firstRunName, app.Status.LastRunName)

	// Simulate completion of the first run.
	run.Status.AppState.State = v1alpha1.CompletedState
	ssaInformer.GetIndexer().Add(app)
	saInformer.GetIndexer().Add(run)
	// This sync should not start any new run, but update Status.PastSuccessfulRunNames.
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(app.Namespace).Get(app.Name, options)
	assert.Equal(t, 1, len(app.Status.PastSuccessfulRunNames))
	assert.Equal(t, firstRunName, app.Status.PastSuccessfulRunNames[0])
	run, _ = c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(firstRunName, options)
	assert.NotNil(t, run)

	ssaInformer.GetIndexer().Add(app)
	saInformer.GetIndexer().Add(run)
	// This sync should not start any new run, nor update Status.PastSuccessfulRunNames.
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(app.Namespace).Get(app.Name, options)
	assert.Equal(t, 1, len(app.Status.PastSuccessfulRunNames))
	assert.Equal(t, firstRunName, app.Status.PastSuccessfulRunNames[0])
	run, _ = c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(firstRunName, options)
	assert.NotNil(t, run)

	ssaInformer.GetIndexer().Add(app)
	saInformer.GetIndexer().Add(run)
	// Advance the clock to trigger the second run.
	clk.SetTime(app.Status.NextRun.Time.Add(5 * time.Second))
	// This sync should start the second run.
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(app.Namespace).Get(app.Name, options)
	assert.Equal(t, v1alpha1.ScheduledState, app.Status.ScheduleState)
	// The second run should have a different name.
	secondRunName := app.Status.LastRunName
	assert.NotEqual(t, firstRunName, secondRunName)
	assert.True(t, app.Status.NextRun.Time.After(app.Status.LastRun.Time))
	// The second run exists.
	run, _ = c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(secondRunName, options)
	assert.NotNil(t, run)

	// Simulate completion of the second run.
	run.Status.AppState.State = v1alpha1.CompletedState
	ssaInformer.GetIndexer().Add(app)
	saInformer.GetIndexer().Add(run)
	clk.Step(5 * time.Second)
	// This sync should not start any new run, but update Status.PastSuccessfulRunNames.
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(app.Namespace).Get(app.Name, options)
	assert.Equal(t, 1, len(app.Status.PastSuccessfulRunNames))
	assert.Equal(t, secondRunName, app.Status.PastSuccessfulRunNames[0])
	// The first run should have been deleted due to the completion of the second run.
	firstRun, _ := c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(firstRunName, options)
	assert.Nil(t, firstRun)

	ssaInformer.GetIndexer().Add(app)
	saInformer.GetIndexer().Add(run)
	// This sync should not start any new run, nor update Status.PastSuccessfulRunNames.
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(app.Namespace).Get(app.Name, options)
	assert.Equal(t, 1, len(app.Status.PastSuccessfulRunNames))
	assert.Equal(t, secondRunName, app.Status.PastSuccessfulRunNames[0])
	run, _ = c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(secondRunName, options)
	assert.NotNil(t, run)
}

func TestSyncScheduledSparkApplication_Forbid(t *testing.T) {
	app := &v1alpha1.ScheduledSparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test-app",
		},
		Spec: v1alpha1.ScheduledSparkApplicationSpec{
			Schedule:          "@every 1m",
			ConcurrencyPolicy: v1alpha1.ConcurrencyForbid,
		},
	}
	c, ssaInformer, saInformer, clk := newFakeController(app)
	key, _ := cache.MetaNamespaceKeyFunc(app)

	options := metav1.GetOptions{}

	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(app.Namespace).Get(app.Name, options)
	assert.Equal(t, v1alpha1.ScheduledState, app.Status.ScheduleState)
	// The first run should not have been triggered.
	assert.True(t, app.Status.LastRunName == "")

	ssaInformer.GetIndexer().Add(app)
	// Advance the clock by 1 minute.
	clk.Step(1 * time.Minute)

	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(app.Namespace).Get(app.Name, options)
	assert.Equal(t, v1alpha1.ScheduledState, app.Status.ScheduleState)
	firstRunName := app.Status.LastRunName
	// The first run should have been triggered.
	assert.True(t, firstRunName != "")
	assert.False(t, app.Status.LastRun.IsZero())
	assert.True(t, app.Status.NextRun.Time.After(app.Status.LastRun.Time))
	// The first run exists.
	run, _ := c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(firstRunName, options)
	assert.NotNil(t, run)

	ssaInformer.GetIndexer().Add(app)
	saInformer.GetIndexer().Add(run)
	clk.SetTime(app.Status.NextRun.Time.Add(5 * time.Second))
	// This sync should not start the next run because the first run has not completed yet.
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(app.Namespace).Get(app.Name, options)
	assert.Equal(t, firstRunName, app.Status.LastRunName)

	// Simulate completion of the first run.
	run.Status.AppState.State = v1alpha1.CompletedState
	ssaInformer.GetIndexer().Add(app)
	saInformer.GetIndexer().Add(run)
	// This sync should not start the next run because the first run has not completed yet.
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(app.Namespace).Get(app.Name, options)
	secondRunName := app.Status.LastRunName
	assert.NotEqual(t, firstRunName, secondRunName)
	// The second run exists.
	run, _ = c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(secondRunName, options)
	assert.NotNil(t, run)
}

func TestSyncScheduledSparkApplication_Replace(t *testing.T) {
	app := &v1alpha1.ScheduledSparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test-app",
		},
		Spec: v1alpha1.ScheduledSparkApplicationSpec{
			Schedule:          "@every 1m",
			ConcurrencyPolicy: v1alpha1.ConcurrencyReplace,
		},
	}
	c, ssaInformer, saInformer, clk := newFakeController(app)
	key, _ := cache.MetaNamespaceKeyFunc(app)

	options := metav1.GetOptions{}

	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(app.Namespace).Get(app.Name, options)
	assert.Equal(t, v1alpha1.ScheduledState, app.Status.ScheduleState)
	// The first run should not have been triggered.
	assert.True(t, app.Status.LastRunName == "")

	ssaInformer.GetIndexer().Add(app)
	// Advance the clock by 1 minute.
	clk.Step(1 * time.Minute)

	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(app.Namespace).Get(app.Name, options)
	assert.Equal(t, v1alpha1.ScheduledState, app.Status.ScheduleState)
	firstRunName := app.Status.LastRunName
	// The first run should have been triggered.
	assert.True(t, firstRunName != "")
	assert.False(t, app.Status.LastRun.IsZero())
	assert.True(t, app.Status.NextRun.Time.After(app.Status.LastRun.Time))
	// The first run exists.
	run, _ := c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(firstRunName, options)
	assert.NotNil(t, run)

	ssaInformer.GetIndexer().Add(app)
	saInformer.GetIndexer().Add(run)
	clk.SetTime(app.Status.NextRun.Time.Add(5 * time.Second))
	// This sync should not start the next run because the first run has not completed yet.
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(app.Namespace).Get(app.Name, options)
	secondRunName := app.Status.LastRunName
	assert.NotEqual(t, firstRunName, secondRunName)
	// The first run should have been deleted.
	run, _ = c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(firstRunName, options)
	assert.Nil(t, run)
	// The second run exists.
	run, _ = c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(secondRunName, options)
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
			LastRunName: "run1",
		},
	}
	c, _, saInformer, _ := newFakeController(app)

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
	saInformer.GetIndexer().Add(run1)
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
	saInformer.GetIndexer().Add(run1)
	ok, _ = c.shouldStartNextRun(app)
	assert.True(t, ok)

	// ConcurrencyReplace with a completed run.
	app.Spec.ConcurrencyPolicy = v1alpha1.ConcurrencyReplace
	ok, _ = c.shouldStartNextRun(app)
	assert.True(t, ok)
	// ConcurrencyReplace with a running run.
	run1.Status.AppState.State = v1alpha1.RunningState
	c.crdClient.SparkoperatorV1alpha1().SparkApplications(run1.Namespace).Update(run1)
	saInformer.GetIndexer().Add(run1)
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
	c, _, _, clk := newFakeController(app)

	schedule, _ := cron.ParseStandard(app.Spec.Schedule)
	status := app.Status.DeepCopy()

	clk.SetTime(time.Now())
	err := c.startNextRun(app, status, schedule)
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, status.NextRun.After(status.LastRun.Time))
	// Check the first run.
	firstRunName := status.LastRunName
	assert.True(t, firstRunName != "")
	run, _ := c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(firstRunName, metav1.GetOptions{})
	assert.NotNil(t, run)

	clk.SetTime(time.Now())
	err = c.startNextRun(app, status, schedule)
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, status.NextRun.After(status.LastRun.Time))
	// Check the second run.
	secondRunName := status.LastRunName
	assert.True(t, status.LastRunName != "")
	run, _ = c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(secondRunName, metav1.GetOptions{})
	assert.NotNil(t, run)
	// The second run should have a different name.
	assert.NotEqual(t, secondRunName, firstRunName)
}

func TestCheckAndUpdatePastRuns(t *testing.T) {
	var two int32 = 2
	app := &v1alpha1.ScheduledSparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test-app",
		},
		Spec: v1alpha1.ScheduledSparkApplicationSpec{
			Schedule:                  "@every 1m",
			SuccessfulRunHistoryLimit: &two,
			FailedRunHistoryLimit:     &two,
		},
	}
	c, _, saInformer, _ := newFakeController(app)

	run1 := &v1alpha1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: app.Namespace,
			Name:      "run1",
		},
		Status: v1alpha1.SparkApplicationStatus{
			AppState: v1alpha1.ApplicationState{
				State: v1alpha1.CompletedState,
			},
		},
	}
	c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Create(run1)
	saInformer.GetIndexer().Add(run1)

	// The first completed run should have been recorded.
	status := app.Status.DeepCopy()
	status.LastRunName = run1.Name
	c.checkAndUpdatePastRuns(app, status)
	assert.Equal(t, 1, len(status.PastSuccessfulRunNames))
	assert.Equal(t, run1.Name, status.PastSuccessfulRunNames[0])

	// The second run that is running should not be recorded.
	run2 := run1.DeepCopy()
	run2.Name = "run2"
	run2.Status.AppState.State = v1alpha1.RunningState
	c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Create(run2)
	saInformer.GetIndexer().Add(run2)
	c.checkAndUpdatePastRuns(app, status)
	assert.Equal(t, 1, len(status.PastSuccessfulRunNames))
	assert.Equal(t, run1.Name, status.PastSuccessfulRunNames[0])
	// The second completed run should have been recorded.
	run2.Status.AppState.State = v1alpha1.CompletedState
	c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Update(run2)
	saInformer.GetIndexer().Add(run2)
	status.LastRunName = run2.Name
	c.checkAndUpdatePastRuns(app, status)
	assert.Equal(t, 2, len(status.PastSuccessfulRunNames))
	assert.Equal(t, run2.Name, status.PastSuccessfulRunNames[0])
	assert.Equal(t, run1.Name, status.PastSuccessfulRunNames[1])
	// The second completed run has already been recorded, so should not be recorded again.
	saInformer.GetIndexer().Add(run2)
	c.checkAndUpdatePastRuns(app, status)
	assert.Equal(t, 2, len(status.PastSuccessfulRunNames))
	assert.Equal(t, run2.Name, status.PastSuccessfulRunNames[0])
	assert.Equal(t, run1.Name, status.PastSuccessfulRunNames[1])
	// SparkApplications of both of the first two completed runs should exist.
	existing, _ := c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(run2.Name,
		metav1.GetOptions{})
	assert.NotNil(t, existing)
	existing, _ = c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(run1.Name,
		metav1.GetOptions{})
	assert.NotNil(t, existing)

	// The third completed run should have been recorded.
	run3 := run1.DeepCopy()
	run3.Name = "run3"
	c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Create(run3)
	saInformer.GetIndexer().Add(run3)
	status.LastRunName = run3.Name
	c.checkAndUpdatePastRuns(app, status)
	assert.Equal(t, 2, len(status.PastSuccessfulRunNames))
	assert.Equal(t, run3.Name, status.PastSuccessfulRunNames[0])
	assert.Equal(t, run2.Name, status.PastSuccessfulRunNames[1])
	// SparkApplications of the last two completed runs should still exist,
	// but the one of the first completed run should have been deleted.
	existing, _ = c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(run3.Name,
		metav1.GetOptions{})
	assert.NotNil(t, existing)
	existing, _ = c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(run2.Name,
		metav1.GetOptions{})
	assert.NotNil(t, existing)
	existing, _ = c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(run1.Name,
		metav1.GetOptions{})
	assert.Nil(t, existing)

	// The first failed run should have been recorded.
	run4 := run1.DeepCopy()
	run4.Name = "run4"
	run4.Status.AppState.State = v1alpha1.FailedState
	c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Create(run4)
	saInformer.GetIndexer().Add(run4)
	status.LastRunName = run4.Name
	c.checkAndUpdatePastRuns(app, status)
	assert.Equal(t, 1, len(status.PastFailedRunNames))
	assert.Equal(t, run4.Name, status.PastFailedRunNames[0])

	// The second failed run should have been recorded.
	run5 := run1.DeepCopy()
	run5.Name = "run5"
	run5.Status.AppState.State = v1alpha1.FailedState
	c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Create(run5)
	saInformer.GetIndexer().Add(run5)
	status.LastRunName = run5.Name
	c.checkAndUpdatePastRuns(app, status)
	assert.Equal(t, 2, len(status.PastFailedRunNames))
	assert.Equal(t, run5.Name, status.PastFailedRunNames[0])
	assert.Equal(t, run4.Name, status.PastFailedRunNames[1])

	// The third failed run should have been recorded.
	run6 := run1.DeepCopy()
	run6.Name = "run6"
	run6.Status.AppState.State = v1alpha1.FailedState
	c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Create(run6)
	saInformer.GetIndexer().Add(run6)
	status.LastRunName = run6.Name
	c.checkAndUpdatePastRuns(app, status)
	assert.Equal(t, 2, len(status.PastFailedRunNames))
	assert.Equal(t, run6.Name, status.PastFailedRunNames[0])
	assert.Equal(t, run5.Name, status.PastFailedRunNames[1])
	// SparkApplications of the last two failed runs should still exist,
	// but the one of the first failed run should have been deleted.
	existing, _ = c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(run6.Name,
		metav1.GetOptions{})
	assert.NotNil(t, existing)
	existing, _ = c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(run5.Name,
		metav1.GetOptions{})
	assert.NotNil(t, existing)
	existing, _ = c.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(run4.Name,
		metav1.GetOptions{})
	assert.Nil(t, existing)
}

func newFakeController(apps ...*v1alpha1.ScheduledSparkApplication) (*Controller, cache.SharedIndexInformer,
	cache.SharedIndexInformer, *clock.FakeClock) {
	crdClient := crdclientfake.NewSimpleClientset()
	kubeClient := kubeclientfake.NewSimpleClientset()
	apiExtensionsClient := apiextensionsfake.NewSimpleClientset()
	informerFactory := crdinformers.NewSharedInformerFactory(crdClient, 1*time.Second)
	clk := clock.NewFakeClock(time.Now())
	controller := NewController(crdClient, kubeClient, apiExtensionsClient, informerFactory, clk)

	ssaInformer := informerFactory.Sparkoperator().V1alpha1().ScheduledSparkApplications().Informer()
	for _, app := range apps {
		crdClient.SparkoperatorV1alpha1().ScheduledSparkApplications(app.Namespace).Create(app)
		ssaInformer.GetIndexer().Add(app)
	}

	saInformer := informerFactory.Sparkoperator().V1alpha1().SparkApplications().Informer()
	crdClient.AddReactor("create", "sparkapplications",
		func(action kubetesting.Action) (bool, runtime.Object, error) {
			obj := action.(kubetesting.CreateAction).GetObject()
			saInformer.GetIndexer().Add(obj)
			return true, obj, nil
		})

	return controller, ssaInformer, saInformer, clk
}
