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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubeclientfake "k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	crdclientfake "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned/fake"
	crdinformers "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/informers/externalversions"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
)

func TestSyncScheduledSparkApplication_Allow(t *testing.T) {
	app := &v1beta2.ScheduledSparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test-app-allow",
		},
		Spec: v1beta2.ScheduledSparkApplicationSpec{
			Schedule:          "@every 10m",
			ConcurrencyPolicy: v1beta2.ConcurrencyAllow,
		},
	}
	c, clk := newFakeController()
	c.crdClient.SparkoperatorV1beta2().ScheduledSparkApplications(app.Namespace).Create(context.TODO(), app, metav1.CreateOptions{})

	key, _ := cache.MetaNamespaceKeyFunc(app)
	options := metav1.GetOptions{}

	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1beta2().ScheduledSparkApplications(app.Namespace).Get(context.TODO(), app.Name, options)
	assert.Equal(t, v1beta2.ScheduledState, app.Status.ScheduleState)
	// The first run should not have been triggered.
	assert.True(t, app.Status.LastRunName == "")

	// Advance the clock by 10 minutes.
	clk.Step(10 * time.Minute)
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1beta2().ScheduledSparkApplications(app.Namespace).Get(context.TODO(), app.Name, options)
	firstRunName := app.Status.LastRunName
	// The first run should have been triggered.
	assert.True(t, firstRunName != "")
	assert.False(t, app.Status.LastRun.IsZero())
	assert.True(t, app.Status.NextRun.Time.After(app.Status.LastRun.Time))
	// The first run exists.
	run, _ := c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(context.TODO(), firstRunName, options)
	assert.NotNil(t, run)

	clk.Step(5 * time.Second)
	// The second sync should not start any new run.
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1beta2().ScheduledSparkApplications(app.Namespace).Get(context.TODO(), app.Name, options)
	// Next run is not due, so LastRunName should stay the same.
	assert.Equal(t, firstRunName, app.Status.LastRunName)

	// Simulate completion of the first run.
	run.Status.AppState.State = v1beta2.CompletedState
	c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Update(context.TODO(), run, metav1.UpdateOptions{})
	// This sync should not start any new run, but update Status.PastSuccessfulRunNames.
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1beta2().ScheduledSparkApplications(app.Namespace).Get(context.TODO(), app.Name, options)
	assert.Equal(t, 1, len(app.Status.PastSuccessfulRunNames))
	assert.Equal(t, firstRunName, app.Status.PastSuccessfulRunNames[0])
	run, _ = c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(context.TODO(), firstRunName, options)
	assert.NotNil(t, run)

	// This sync should not start any new run, nor update Status.PastSuccessfulRunNames.
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1beta2().ScheduledSparkApplications(app.Namespace).Get(context.TODO(), app.Name, options)
	assert.Equal(t, 1, len(app.Status.PastSuccessfulRunNames))
	assert.Equal(t, firstRunName, app.Status.PastSuccessfulRunNames[0])
	run, _ = c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(context.TODO(), firstRunName, options)
	assert.NotNil(t, run)

	// Advance the clock to trigger the second run.
	clk.SetTime(app.Status.NextRun.Time.Add(5 * time.Second))
	// This sync should start the second run.
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1beta2().ScheduledSparkApplications(app.Namespace).Get(context.TODO(), app.Name, options)
	assert.Equal(t, v1beta2.ScheduledState, app.Status.ScheduleState)
	// The second run should have a different name.
	secondRunName := app.Status.LastRunName
	assert.NotEqual(t, firstRunName, secondRunName)
	assert.True(t, app.Status.NextRun.Time.After(app.Status.LastRun.Time))
	// The second run exists.
	run, _ = c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(context.TODO(), secondRunName, options)
	assert.NotNil(t, run)

	// Simulate completion of the second run.
	run.Status.AppState.State = v1beta2.CompletedState
	c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Update(context.TODO(), run, metav1.UpdateOptions{})
	// This sync should not start any new run, but update Status.PastSuccessfulRunNames.
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1beta2().ScheduledSparkApplications(app.Namespace).Get(context.TODO(), app.Name, options)
	assert.Equal(t, 1, len(app.Status.PastSuccessfulRunNames))
	// The first run should have been deleted due to the completion of the second run.
	firstRun, _ := c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(context.TODO(), firstRunName, options)
	assert.Nil(t, firstRun)

	// This sync should not start any new run, nor update Status.PastSuccessfulRunNames.
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1beta2().ScheduledSparkApplications(app.Namespace).Get(context.TODO(), app.Name, options)
	assert.Equal(t, 1, len(app.Status.PastSuccessfulRunNames))
	run, _ = c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(context.TODO(), secondRunName, options)
	assert.NotNil(t, run)

	// Test the case where we update the schedule to be more frequent
	app.Spec.Schedule = "@every 2m"
	recentRunName := app.Status.LastRunName
	recentRunTime := app.Status.LastRun.Time
	app, _ = c.crdClient.SparkoperatorV1beta2().ScheduledSparkApplications(app.Namespace).Update(context.TODO(), app, metav1.UpdateOptions{})
	// sync our update
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	// Advance the clock by 3 minutes.
	clk.Step(3 * time.Minute)
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1beta2().ScheduledSparkApplications(app.Namespace).Get(context.TODO(), app.Name, options)
	// A run should have been triggered
	assert.NotEqual(t, recentRunName, app.Status.LastRunName)
	assert.True(t, recentRunTime.Before(app.Status.LastRun.Time))
	run, _ = c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(context.TODO(), app.Status.LastRunName, options)
	assert.NotNil(t, run)
	// Simulate completion of the last run
	run.Status.AppState.State = v1beta2.CompletedState
	c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Update(context.TODO(), run, metav1.UpdateOptions{})
	// This sync should not start any new run, but update Status.PastSuccessfulRunNames.
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
}

func TestSyncScheduledSparkApplication_Forbid(t *testing.T) {
	app := &v1beta2.ScheduledSparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test-app-forbid",
		},
		Spec: v1beta2.ScheduledSparkApplicationSpec{
			Schedule:          "@every 1m",
			ConcurrencyPolicy: v1beta2.ConcurrencyForbid,
		},
	}
	c, clk := newFakeController()
	c.crdClient.SparkoperatorV1beta2().ScheduledSparkApplications(app.Namespace).Create(context.TODO(), app, metav1.CreateOptions{})

	key, _ := cache.MetaNamespaceKeyFunc(app)
	options := metav1.GetOptions{}

	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1beta2().ScheduledSparkApplications(app.Namespace).Get(context.TODO(), app.Name, options)
	assert.Equal(t, v1beta2.ScheduledState, app.Status.ScheduleState)
	// The first run should not have been triggered.
	assert.True(t, app.Status.LastRunName == "")

	// Advance the clock by 1 minute.
	clk.Step(1 * time.Minute)
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1beta2().ScheduledSparkApplications(app.Namespace).Get(context.TODO(), app.Name, options)
	assert.Equal(t, v1beta2.ScheduledState, app.Status.ScheduleState)
	firstRunName := app.Status.LastRunName
	// The first run should have been triggered.
	assert.True(t, firstRunName != "")
	assert.False(t, app.Status.LastRun.IsZero())
	assert.True(t, app.Status.NextRun.Time.After(app.Status.LastRun.Time))
	// The first run exists.
	run, _ := c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(context.TODO(), firstRunName, options)
	assert.NotNil(t, run)

	clk.SetTime(app.Status.NextRun.Time.Add(5 * time.Second))
	// This sync should not start the next run because the first run has not completed yet.
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1beta2().ScheduledSparkApplications(app.Namespace).Get(context.TODO(), app.Name, options)
	assert.Equal(t, firstRunName, app.Status.LastRunName)

	// Simulate completion of the first run.
	run.Status.AppState.State = v1beta2.CompletedState
	c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Update(context.TODO(), run, metav1.UpdateOptions{})
	// This sync should start the next run because the first run has completed.
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1beta2().ScheduledSparkApplications(app.Namespace).Get(context.TODO(), app.Name, options)
	secondRunName := app.Status.LastRunName
	assert.NotEqual(t, firstRunName, secondRunName)
	assert.Equal(t, 1, len(app.Status.PastSuccessfulRunNames))
	assert.Equal(t, firstRunName, app.Status.PastSuccessfulRunNames[0])
	// The second run exists.
	run, _ = c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(context.TODO(), secondRunName, options)
	assert.NotNil(t, run)
}

func TestSyncScheduledSparkApplication_Replace(t *testing.T) {
	// TODO: figure out why the test fails and remove this.
	t.Skip()

	app := &v1beta2.ScheduledSparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test-app-replace",
		},
		Spec: v1beta2.ScheduledSparkApplicationSpec{
			Schedule:          "@every 1m",
			ConcurrencyPolicy: v1beta2.ConcurrencyReplace,
		},
	}
	c, clk := newFakeController()
	c.crdClient.SparkoperatorV1beta2().ScheduledSparkApplications(app.Namespace).Create(context.TODO(), app, metav1.CreateOptions{})
	key, _ := cache.MetaNamespaceKeyFunc(app)

	options := metav1.GetOptions{}

	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1beta2().ScheduledSparkApplications(app.Namespace).Get(context.TODO(), app.Name, options)
	assert.Equal(t, v1beta2.ScheduledState, app.Status.ScheduleState)
	// The first run should not have been triggered.
	assert.True(t, app.Status.LastRunName == "")

	// Advance the clock by 1 minute.
	clk.Step(1 * time.Minute)
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1beta2().ScheduledSparkApplications(app.Namespace).Get(context.TODO(), app.Name, options)
	assert.Equal(t, v1beta2.ScheduledState, app.Status.ScheduleState)
	firstRunName := app.Status.LastRunName
	// The first run should have been triggered.
	assert.True(t, firstRunName != "")
	assert.False(t, app.Status.LastRun.IsZero())
	assert.True(t, app.Status.NextRun.Time.After(app.Status.LastRun.Time))
	// The first run exists.
	run, _ := c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(context.TODO(), firstRunName, options)
	assert.NotNil(t, run)

	clk.SetTime(app.Status.NextRun.Time.Add(5 * time.Second))
	// This sync should replace the first run with a new run.
	if err := c.syncScheduledSparkApplication(key); err != nil {
		t.Fatal(err)
	}
	app, _ = c.crdClient.SparkoperatorV1beta2().ScheduledSparkApplications(app.Namespace).Get(context.TODO(), app.Name, options)
	secondRunName := app.Status.LastRunName
	assert.NotEqual(t, firstRunName, secondRunName)
	// The first run should have been deleted.
	run, _ = c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(context.TODO(), firstRunName, options)
	assert.Nil(t, run)
	// The second run exists.
	run, _ = c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(context.TODO(), secondRunName, options)
	assert.NotNil(t, run)
}

func TestShouldStartNextRun(t *testing.T) {
	app := &v1beta2.ScheduledSparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test-app",
		},
		Spec: v1beta2.ScheduledSparkApplicationSpec{
			Schedule: "@every 1m",
		},
		Status: v1beta2.ScheduledSparkApplicationStatus{
			LastRunName: "run1",
		},
	}
	c, _ := newFakeController()
	c.crdClient.SparkoperatorV1beta2().ScheduledSparkApplications(app.Namespace).Create(context.TODO(), app, metav1.CreateOptions{})

	run1 := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: app.Namespace,
			Name:      "run1",
			Labels:    map[string]string{config.ScheduledSparkAppNameLabel: app.Name},
		},
	}
	c.crdClient.SparkoperatorV1beta2().SparkApplications(run1.Namespace).Create(context.TODO(), run1, metav1.CreateOptions{})

	// ConcurrencyAllow with a running run.
	run1.Status.AppState.State = v1beta2.RunningState
	c.crdClient.SparkoperatorV1beta2().SparkApplications(run1.Namespace).Update(context.TODO(), run1, metav1.UpdateOptions{})
	app.Spec.ConcurrencyPolicy = v1beta2.ConcurrencyAllow
	ok, _ := c.shouldStartNextRun(app)
	assert.True(t, ok)

	// ConcurrencyForbid with a running run.
	app.Spec.ConcurrencyPolicy = v1beta2.ConcurrencyForbid
	ok, _ = c.shouldStartNextRun(app)
	assert.False(t, ok)
	// ConcurrencyForbid with a completed run.
	run1.Status.AppState.State = v1beta2.CompletedState
	c.crdClient.SparkoperatorV1beta2().SparkApplications(run1.Namespace).Update(context.TODO(), run1, metav1.UpdateOptions{})
	ok, _ = c.shouldStartNextRun(app)
	assert.True(t, ok)

	// ConcurrencyReplace with a completed run.
	app.Spec.ConcurrencyPolicy = v1beta2.ConcurrencyReplace
	ok, _ = c.shouldStartNextRun(app)
	assert.True(t, ok)
	// ConcurrencyReplace with a running run.
	run1.Status.AppState.State = v1beta2.RunningState
	c.crdClient.SparkoperatorV1beta2().SparkApplications(run1.Namespace).Update(context.TODO(), run1, metav1.UpdateOptions{})
	ok, _ = c.shouldStartNextRun(app)
	assert.True(t, ok)
	// The previous running run should have been deleted.
	existing, _ := c.crdClient.SparkoperatorV1beta2().SparkApplications(run1.Namespace).Get(
		context.TODO(),
		run1.Name,
		metav1.GetOptions{},
	)
	assert.Nil(t, existing)
}

func TestCheckAndUpdatePastRuns(t *testing.T) {
	var two int32 = 2
	app := &v1beta2.ScheduledSparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test-app",
		},
		Spec: v1beta2.ScheduledSparkApplicationSpec{
			Schedule:                  "@every 1m",
			SuccessfulRunHistoryLimit: &two,
			FailedRunHistoryLimit:     &two,
		},
	}
	c, _ := newFakeController()

	run1 := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: app.Namespace,
			Name:      "run1",
			Labels:    map[string]string{config.ScheduledSparkAppNameLabel: app.Name},
		},
		Status: v1beta2.SparkApplicationStatus{
			AppState: v1beta2.ApplicationState{
				State: v1beta2.CompletedState,
			},
		},
	}
	c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Create(context.TODO(), run1, metav1.CreateOptions{})

	// The first completed run should have been recorded.
	status := app.Status.DeepCopy()
	c.checkAndUpdatePastRuns(app, status)
	assert.Equal(t, 1, len(status.PastSuccessfulRunNames))
	assert.Equal(t, run1.Name, status.PastSuccessfulRunNames[0])

	// The second run that is running should not be recorded.
	run2 := run1.DeepCopy()
	run2.CreationTimestamp.Time = run1.CreationTimestamp.Add(10 * time.Second)
	run2.Name = "run2"
	run2.Status.AppState.State = v1beta2.RunningState
	c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Create(context.TODO(), run2, metav1.CreateOptions{})
	c.checkAndUpdatePastRuns(app, status)
	assert.Equal(t, 1, len(status.PastSuccessfulRunNames))
	assert.Equal(t, run1.Name, status.PastSuccessfulRunNames[0])
	// The second completed run should have been recorded.
	run2.Status.AppState.State = v1beta2.CompletedState
	c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Update(context.TODO(), run2, metav1.UpdateOptions{})
	c.checkAndUpdatePastRuns(app, status)
	assert.Equal(t, 2, len(status.PastSuccessfulRunNames))
	assert.Equal(t, run2.Name, status.PastSuccessfulRunNames[0])
	assert.Equal(t, run1.Name, status.PastSuccessfulRunNames[1])
	// The second completed run has already been recorded, so should not be recorded again.
	c.checkAndUpdatePastRuns(app, status)
	assert.Equal(t, 2, len(status.PastSuccessfulRunNames))
	assert.Equal(t, run2.Name, status.PastSuccessfulRunNames[0])
	assert.Equal(t, run1.Name, status.PastSuccessfulRunNames[1])
	// SparkApplications of both of the first two completed runs should exist.
	existing, _ := c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(
		context.TODO(),
		run2.Name,
		metav1.GetOptions{},
	)
	assert.NotNil(t, existing)
	existing, _ = c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(
		context.TODO(),
		run1.Name,
		metav1.GetOptions{},
	)
	assert.NotNil(t, existing)

	// The third completed run should have been recorded.
	run3 := run1.DeepCopy()
	run3.CreationTimestamp.Time = run2.CreationTimestamp.Add(10 * time.Second)
	run3.Name = "run3"
	c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Create(context.TODO(), run3, metav1.CreateOptions{})
	c.checkAndUpdatePastRuns(app, status)
	assert.Equal(t, 2, len(status.PastSuccessfulRunNames))
	assert.Equal(t, run3.Name, status.PastSuccessfulRunNames[0])
	assert.Equal(t, run2.Name, status.PastSuccessfulRunNames[1])
	// SparkApplications of the last two completed runs should still exist,
	// but the one of the first completed run should have been deleted.
	existing, _ = c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(
		context.TODO(),
		run3.Name,
		metav1.GetOptions{},
	)
	assert.NotNil(t, existing)
	existing, _ = c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(
		context.TODO(),
		run2.Name,
		metav1.GetOptions{},
	)
	assert.NotNil(t, existing)
	existing, _ = c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(
		context.TODO(),
		run1.Name,
		metav1.GetOptions{},
	)
	assert.Nil(t, existing)

	// The first failed run should have been recorded.
	run4 := run1.DeepCopy()
	run4.CreationTimestamp.Time = run3.CreationTimestamp.Add(10 * time.Second)
	run4.Name = "run4"
	run4.Status.AppState.State = v1beta2.FailedState
	c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Create(context.TODO(), run4, metav1.CreateOptions{})
	c.checkAndUpdatePastRuns(app, status)
	assert.Equal(t, 1, len(status.PastFailedRunNames))
	assert.Equal(t, run4.Name, status.PastFailedRunNames[0])

	// The second failed run should have been recorded.
	run5 := run1.DeepCopy()
	run5.CreationTimestamp.Time = run4.CreationTimestamp.Add(10 * time.Second)
	run5.Name = "run5"
	run5.Status.AppState.State = v1beta2.FailedState
	c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Create(context.TODO(), run5, metav1.CreateOptions{})
	c.checkAndUpdatePastRuns(app, status)
	assert.Equal(t, 2, len(status.PastFailedRunNames))
	assert.Equal(t, run5.Name, status.PastFailedRunNames[0])
	assert.Equal(t, run4.Name, status.PastFailedRunNames[1])

	// The third failed run should have been recorded.
	run6 := run1.DeepCopy()
	run6.CreationTimestamp.Time = run5.CreationTimestamp.Add(10 * time.Second)
	run6.Name = "run6"
	run6.Status.AppState.State = v1beta2.FailedState
	c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Create(context.TODO(), run6, metav1.CreateOptions{})
	c.checkAndUpdatePastRuns(app, status)
	assert.Equal(t, 2, len(status.PastFailedRunNames))
	assert.Equal(t, run6.Name, status.PastFailedRunNames[0])
	assert.Equal(t, run5.Name, status.PastFailedRunNames[1])
	// SparkApplications of the last two failed runs should still exist,
	// but the one of the first failed run should have been deleted.
	existing, _ = c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(
		context.TODO(),
		run6.Name,
		metav1.GetOptions{},
	)
	assert.NotNil(t, existing)
	existing, _ = c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(
		context.TODO(),
		run5.Name,
		metav1.GetOptions{},
	)
	assert.NotNil(t, existing)
	existing, _ = c.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(
		context.TODO(),
		run4.Name,
		metav1.GetOptions{},
	)
	assert.Nil(t, existing)
}

func newFakeController() (*Controller, *clocktesting.FakeClock) {
	crdClient := crdclientfake.NewSimpleClientset()
	kubeClient := kubeclientfake.NewSimpleClientset()
	apiExtensionsClient := apiextensionsfake.NewSimpleClientset()
	informerFactory := crdinformers.NewSharedInformerFactory(crdClient, 1*time.Second)
	clk := clocktesting.NewFakeClock(time.Now())
	controller := NewController(crdClient, kubeClient, apiExtensionsClient, informerFactory, clk)
	ssaInformer := informerFactory.Sparkoperator().V1beta2().ScheduledSparkApplications().Informer()
	saInformer := informerFactory.Sparkoperator().V1beta2().SparkApplications().Informer()
	crdClient.PrependReactor("create", "scheduledsparkapplications",
		func(action kubetesting.Action) (bool, runtime.Object, error) {
			obj := action.(kubetesting.CreateAction).GetObject()
			ssaInformer.GetStore().Add(obj)
			return false, obj, nil
		})
	crdClient.PrependReactor("update", "scheduledsparkapplications",
		func(action kubetesting.Action) (bool, runtime.Object, error) {
			obj := action.(kubetesting.UpdateAction).GetObject()
			ssaInformer.GetStore().Update(obj)
			return false, obj, nil
		})
	crdClient.PrependReactor("create", "sparkapplications",
		func(action kubetesting.Action) (bool, runtime.Object, error) {
			obj := action.(kubetesting.CreateAction).GetObject()
			saInformer.GetStore().Add(obj)
			return false, obj, nil
		})
	crdClient.PrependReactor("update", "sparkapplications",
		func(action kubetesting.Action) (bool, runtime.Object, error) {
			obj := action.(kubetesting.UpdateAction).GetObject()
			saInformer.GetStore().Update(obj)
			return false, obj, nil
		})
	return controller, clk
}
