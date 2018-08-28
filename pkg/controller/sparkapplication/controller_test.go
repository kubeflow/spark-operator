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
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"fmt"
	"os/exec"

	"github.com/prometheus/client_golang/prometheus"
	prometheus_model "github.com/prometheus/client_model/go"

	apiv1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	kubeclientfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	crdclientfake "k8s.io/spark-on-k8s-operator/pkg/client/clientset/versioned/fake"
	crdinformers "k8s.io/spark-on-k8s-operator/pkg/client/informers/externalversions"
	"k8s.io/spark-on-k8s-operator/pkg/config"
	"k8s.io/spark-on-k8s-operator/pkg/util"
)

func newFakeController(app *v1alpha1.SparkApplication, pods ...*apiv1.Pod) (*Controller, *record.FakeRecorder) {
	crdclientfake.AddToScheme(scheme.Scheme)
	crdClient := crdclientfake.NewSimpleClientset()
	kubeClient := kubeclientfake.NewSimpleClientset()
	apiExtensionsClient := apiextensionsfake.NewSimpleClientset()
	informerFactory := crdinformers.NewSharedInformerFactory(crdClient, 0*time.Second)
	recorder := record.NewFakeRecorder(3)

	kubeClient.CoreV1().Nodes().Create(&apiv1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
		},
		Status: apiv1.NodeStatus{
			Addresses: []apiv1.NodeAddress{
				{
					Type:    apiv1.NodeExternalIP,
					Address: "12.34.56.78",
				},
			},
		},
	})

	podInformerFactory := informers.NewSharedInformerFactory(kubeClient, 0*time.Second)
	controller := newSparkApplicationController(crdClient, kubeClient, apiExtensionsClient, informerFactory, podInformerFactory, recorder,
		&util.MetricConfig{})

	informer := informerFactory.Sparkoperator().V1alpha1().SparkApplications().Informer()
	if app != nil {
		informer.GetIndexer().Add(app)
	}

	podInformer := podInformerFactory.Core().V1().Pods().Informer()
	for _, pod := range pods {
		if pod != nil {
			podInformer.GetIndexer().Add(pod)
		}
	}
	return controller, recorder
}

func TestOnAdd(t *testing.T) {
	ctrl, recorder := newFakeController(nil)

	app := &v1alpha1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Status: v1alpha1.SparkApplicationStatus{},
	}
	ctrl.onAdd(app)

	item, _ := ctrl.queue.Get()
	defer ctrl.queue.Done(item)
	key, ok := item.(string)
	assert.True(t, ok)
	expectedKey, _ := cache.MetaNamespaceKeyFunc(app)
	assert.Equal(t, expectedKey, key)
	ctrl.queue.Forget(item)

	assert.Equal(t, 1, len(recorder.Events))
	event := <-recorder.Events
	assert.True(t, strings.Contains(event, "SparkApplicationAdded"))
}

func TestOnUpdate(t *testing.T) {
	ctrl, recorder := newFakeController(nil)

	appTemplate := &v1alpha1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       "default",
			ResourceVersion: "1",
		},
		Spec: v1alpha1.SparkApplicationSpec{
			Mode:  v1alpha1.ClusterMode,
			Image: stringptr("foo-image:v1"),
			Executor: v1alpha1.ExecutorSpec{
				Instances: int32ptr(1),
			},
		},
	}

	// Case1: Same Spec.
	copyWithSameSpec := appTemplate.DeepCopy()
	copyWithSameSpec.Status.Attempts = 3
	copyWithSameSpec.ResourceVersion = "2"

	ctrl.onUpdate(appTemplate, copyWithSameSpec)

	// Verify that the App was enqueued but no events fired.
	item, _ := ctrl.queue.Get()
	key, ok := item.(string)
	assert.True(t, ok)
	expectedKey, _ := cache.MetaNamespaceKeyFunc(appTemplate)
	assert.Equal(t, expectedKey, key)
	ctrl.queue.Forget(item)
	ctrl.queue.Done(item)
	assert.Equal(t, 0, len(recorder.Events))

	// Case2: Spec Update.
	copyWithSpecUpdate := appTemplate.DeepCopy()
	copyWithSpecUpdate.Spec.Image = stringptr("foo-image:v2")
	copyWithSpecUpdate.ResourceVersion = "2"

	ctrl.onUpdate(appTemplate, copyWithSpecUpdate)

	// Verify that update warning event fired.
	assert.Equal(t, 1, len(recorder.Events))
	event := <-recorder.Events
	assert.True(t, strings.Contains(event, "SparkApplicationUpdateFailed"))
}

func TestOnDelete(t *testing.T) {
	ctrl, recorder := newFakeController(nil)

	app := &v1alpha1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Status: v1alpha1.SparkApplicationStatus{},
	}
	ctrl.onAdd(app)
	ctrl.queue.Get()
	<-recorder.Events

	ctrl.onDelete(app)
	ctrl.queue.ShutDown()
	item, _ := ctrl.queue.Get()
	defer ctrl.queue.Done(item)
	assert.True(t, item == nil)
	event := <-recorder.Events
	assert.True(t, strings.Contains(event, "SparkApplicationDeleted"))
	ctrl.queue.Forget(item)
}

func TestHelperProcessFailure(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	os.Exit(2)
}

func TestHelperProcessSuccess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	os.Exit(0)
}

func fetchCounterValue(m *prometheus.CounterVec, labels map[string]string) float64 {
	pb := &prometheus_model.Metric{}
	m.With(labels).Write(pb)

	return pb.GetCounter().GetValue()
}

type metrics struct {
	submitMetricCount  float64
	runningMetricCount float64
	successMetricCount float64
	failedMetricCount  float64
}

type executorMetrics struct {
	runningMetricCount float64
	successMetricCount float64
	failedMetricCount  float64
}

func TestSyncSparkApplication_NewAppSubmissionSuccess(t *testing.T) {

	os.Setenv(kubernetesServiceHostEnvVar, "localhost")
	os.Setenv(kubernetesServicePortEnvVar, "443")

	app := &v1alpha1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Status: v1alpha1.SparkApplicationStatus{
			SparkApplicationID: "spark-123",
			AppState: v1alpha1.ApplicationState{
				State:        v1alpha1.NewState,
				ErrorMessage: "",
			},
		},
	}

	ctrl, _ := newFakeController(app)
	_, err := ctrl.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Create(app)
	if err != nil {
		t.Fatal(err)
	}

	execCommand = func(command string, args ...string) *exec.Cmd {
		cs := []string{"-test.run=TestHelperProcessSuccess", "--", command}
		cs = append(cs, args...)
		cmd := exec.Command(os.Args[0], cs...)
		cmd.Env = []string{"GO_WANT_HELPER_PROCESS=1"}
		return cmd
	}

	ctrl.syncSparkApplication("default/foo")
	updatedApp, err := ctrl.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(app.Name, metav1.GetOptions{})
	assert.Equal(t, v1alpha1.SubmittedState, updatedApp.Status.AppState.State)

	assert.Equal(t, float64(1), fetchCounterValue(ctrl.metrics.sparkAppSubmitCount, map[string]string{}))
}

func TestSyncSparkApplication_NewAppSubmissionFailed(t *testing.T) {
	os.Setenv(kubernetesServiceHostEnvVar, "localhost")
	os.Setenv(kubernetesServicePortEnvVar, "443")

	restartPolicyOnFailure := v1alpha1.RestartPolicy{
		Type:                             v1alpha1.OnFailure,
		OnFailureRetries:                 int32ptr(1),
		OnFailureRetryInterval:           int64ptr(100),
		OnSubmissionFailureRetryInterval: int64ptr(100),
		OnSubmissionFailureRetries:       int32ptr(1),
	}
	app := &v1alpha1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Spec: v1alpha1.SparkApplicationSpec{
			RestartPolicy: restartPolicyOnFailure,
		},
		Status: v1alpha1.SparkApplicationStatus{
			AppState: v1alpha1.ApplicationState{
				State:        v1alpha1.NewState,
				ErrorMessage: "",
			},
		},
	}

	ctrl, recorder := newFakeController(app)
	_, err := ctrl.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Create(app)
	if err != nil {
		t.Fatal(err)
	}

	execCommand = func(command string, args ...string) *exec.Cmd {
		cs := []string{"-test.run=TestHelperProcessFailure", "--", command}
		cs = append(cs, args...)
		cmd := exec.Command(os.Args[0], cs...)
		cmd.Env = []string{"GO_WANT_HELPER_PROCESS=1"}
		return cmd
	}

	// Attempt 1
	err = ctrl.syncSparkApplication("default/foo")
	updatedApp, err := ctrl.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(app.Name, metav1.GetOptions{})

	assert.Equal(t, v1alpha1.FailedSubmissionState, updatedApp.Status.AppState.State)
	assert.Equal(t, int32(1), updatedApp.Status.SubmissionAttempts)
	assert.Equal(t, float64(0), fetchCounterValue(ctrl.metrics.sparkAppSubmitCount, map[string]string{}))
	assert.Equal(t, float64(1), fetchCounterValue(ctrl.metrics.sparkAppFailureCount, map[string]string{}))

	event := <-recorder.Events
	assert.True(t, strings.Contains(event, "SparkApplicationSubmissionFailed"))

	// Attempt 2: Retry again
	updatedApp.Status.SubmissionTime = metav1.Time{metav1.Now().Add(-100 * time.Second)}
	ctrl, recorder = newFakeController(updatedApp)
	_, err = ctrl.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Create(updatedApp)
	if err != nil {
		t.Fatal(err)
	}
	err = ctrl.syncSparkApplication("default/foo")

	// Verify App Failed again.
	updatedApp, err = ctrl.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(app.Name, metav1.GetOptions{})
	assert.Nil(t, err)
	assert.Equal(t, v1alpha1.FailedSubmissionState, updatedApp.Status.AppState.State)
	assert.Equal(t, int32(2), updatedApp.Status.SubmissionAttempts)
	assert.Equal(t, float64(0), fetchCounterValue(ctrl.metrics.sparkAppSubmitCount, map[string]string{}))

	event = <-recorder.Events
	assert.True(t, strings.Contains(event, "SparkApplicationSubmissionFailed"))

	// Attempt 3: No more retries
	updatedApp.Status.SubmissionTime = metav1.Time{metav1.Now().Add(-100 * time.Second)}
	ctrl, recorder = newFakeController(updatedApp)
	_, err = ctrl.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Create(updatedApp)
	if err != nil {
		t.Fatal(err)
	}
	err = ctrl.syncSparkApplication("default/foo")

	// Verify App Failed again.
	updatedApp, err = ctrl.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(app.Name, metav1.GetOptions{})
	assert.Nil(t, err)
	assert.Equal(t, v1alpha1.FailedSubmissionState, updatedApp.Status.AppState.State)
	// No more submission attempts made.
	assert.Equal(t, int32(2), updatedApp.Status.SubmissionAttempts)
}

func TestSyncSparkApplication_RunningState(t *testing.T) {

	type testcase struct {
		appName                 string
		oldAppStatus            v1alpha1.ApplicationStateType
		oldExecutorStatus       map[string]v1alpha1.ExecutorState
		driverPod               *apiv1.Pod
		executorPod             *apiv1.Pod
		expectedAppState        v1alpha1.ApplicationStateType
		expectedExecutorState   map[string]v1alpha1.ExecutorState
		expectedAppMetrics      metrics
		expectedExecutorMetrics executorMetrics
	}

	os.Setenv(kubernetesServiceHostEnvVar, "localhost")
	os.Setenv(kubernetesServicePortEnvVar, "443")

	app := &v1alpha1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "test",
		},
		Spec: v1alpha1.SparkApplicationSpec{
			RestartPolicy: v1alpha1.RestartPolicy{
				Type: v1alpha1.Never,
			},
		},
		Status: v1alpha1.SparkApplicationStatus{
			AppState: v1alpha1.ApplicationState{
				State:        v1alpha1.SubmittedState,
				ErrorMessage: "",
			},
			ExecutorState: map[string]v1alpha1.ExecutorState{"exec-1": v1alpha1.ExecutorRunningState},
		},
	}
	testcases := []testcase{
		{
			appName:               "foo-1",
			oldAppStatus:          v1alpha1.SubmittedState,
			oldExecutorStatus:     map[string]v1alpha1.ExecutorState{"exec-1": v1alpha1.ExecutorRunningState},
			expectedAppState:      v1alpha1.SubmittedState,
			expectedExecutorState: map[string]v1alpha1.ExecutorState{"exec-1": v1alpha1.ExecutorFailedState},
			expectedAppMetrics:    metrics{},
			expectedExecutorMetrics: executorMetrics{
				failedMetricCount: 1,
			},
		},
		{
			appName:           "foo-2",
			oldAppStatus:      v1alpha1.SubmittedState,
			oldExecutorStatus: map[string]v1alpha1.ExecutorState{"exec-1": v1alpha1.ExecutorRunningState},
			driverPod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo-driver",
					Namespace: "test",
					Labels: map[string]string{
						config.SparkRoleLabel:    sparkDriverRole,
						config.SparkAppNameLabel: "foo-2",
					},
					ResourceVersion: "1",
				},
				Status: apiv1.PodStatus{
					Phase: apiv1.PodRunning,
				},
			},
			executorPod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "exec-1",
					Namespace: "test",
					Labels: map[string]string{
						config.SparkRoleLabel:    sparkExecutorRole,
						config.SparkAppNameLabel: "foo-2",
					},
					ResourceVersion: "1",
				},
				Status: apiv1.PodStatus{
					Phase: apiv1.PodSucceeded,
				},
			},
			expectedAppState:      v1alpha1.RunningState,
			expectedExecutorState: map[string]v1alpha1.ExecutorState{"exec-1": v1alpha1.ExecutorCompletedState},
			expectedAppMetrics: metrics{
				runningMetricCount: 1,
			},
			expectedExecutorMetrics: executorMetrics{
				successMetricCount: 1,
			},
		},
		{
			appName:           "foo-3",
			oldAppStatus:      v1alpha1.FailedState,
			oldExecutorStatus: map[string]v1alpha1.ExecutorState{"exec-1": v1alpha1.ExecutorCompletedState},
			driverPod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo-driver",
					Namespace: "test",
					Labels: map[string]string{
						config.SparkRoleLabel:    sparkDriverRole,
						config.SparkAppNameLabel: "foo-3",
					},
					ResourceVersion: "1",
				},
				Status: apiv1.PodStatus{
					Phase: apiv1.PodRunning,
				},
			},
			executorPod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "exec-1",
					Namespace: "test",
					Labels: map[string]string{
						config.SparkRoleLabel:    sparkExecutorRole,
						config.SparkAppNameLabel: "foo-3",
					},
					ResourceVersion: "1",
				},
				Status: apiv1.PodStatus{
					Phase: apiv1.PodFailed,
				},
			},
			expectedAppState:        v1alpha1.FailedState,
			expectedExecutorState:   map[string]v1alpha1.ExecutorState{"exec-1": v1alpha1.ExecutorCompletedState},
			expectedAppMetrics:      metrics{},
			expectedExecutorMetrics: executorMetrics{},
		},
	}

	testFn := func(test testcase, t *testing.T) {

		app.Status.AppState.State = test.oldAppStatus
		app.Status.ExecutorState = test.oldExecutorStatus
		app.Name = test.appName
		app.Status.Attempts = 1
		ctrl, _ := newFakeController(app, test.driverPod, test.executorPod)
		_, err := ctrl.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Create(app)
		if err != nil {
			t.Fatal(err)
		}
		if test.driverPod != nil {
			ctrl.kubeClient.CoreV1().Pods(app.GetNamespace()).Create(test.driverPod)
		}
		if test.executorPod != nil {
			ctrl.kubeClient.CoreV1().Pods(app.GetNamespace()).Create(test.executorPod)
		}

		err = ctrl.syncSparkApplication(fmt.Sprintf("%s/%s", app.GetNamespace(), app.GetName()))
		assert.Nil(t, err)
		updatedApp, err := ctrl.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(app.Name, metav1.GetOptions{})
		// Verify App/Executor Statuses
		assert.Equal(t, test.expectedAppState, updatedApp.Status.AppState.State)
		assert.Equal(t, test.expectedExecutorState, updatedApp.Status.ExecutorState)

		// Verify App Metrics
		assert.Equal(t, test.expectedAppMetrics.runningMetricCount, ctrl.metrics.sparkAppRunningCount.Value(map[string]string{}))
		assert.Equal(t, test.expectedAppMetrics.successMetricCount, fetchCounterValue(ctrl.metrics.sparkAppSuccessCount, map[string]string{}))
		assert.Equal(t, test.expectedAppMetrics.submitMetricCount, fetchCounterValue(ctrl.metrics.sparkAppSubmitCount, map[string]string{}))
		assert.Equal(t, test.expectedAppMetrics.failedMetricCount, fetchCounterValue(ctrl.metrics.sparkAppFailureCount, map[string]string{}))

		// Verify Executor Metrics
		assert.Equal(t, test.expectedExecutorMetrics.runningMetricCount, ctrl.metrics.sparkAppExecutorRunningCount.Value(map[string]string{}))
		assert.Equal(t, test.expectedExecutorMetrics.successMetricCount, fetchCounterValue(ctrl.metrics.sparkAppExecutorSuccessCount, map[string]string{}))
		assert.Equal(t, test.expectedExecutorMetrics.failedMetricCount, fetchCounterValue(ctrl.metrics.sparkAppExecutorFailureCount, map[string]string{}))
	}

	for _, test := range testcases {
		testFn(test, t)
	}
}

func TestShouldSubmit(t *testing.T) {
	type testcase struct {
		app          *v1alpha1.SparkApplication
		shouldSubmit bool
	}

	testFn := func(test testcase, t *testing.T) {
		assert.Equal(t, test.shouldSubmit, shouldSubmit(test.app))
	}

	restartPolicyAlways := v1alpha1.RestartPolicy{
		Type: v1alpha1.Always,
		OnSubmissionFailureRetryInterval: int64ptr(100),
		OnFailureRetryInterval:           int64ptr(100),
	}

	restartPolicyNever := v1alpha1.RestartPolicy{
		Type: v1alpha1.Never,
	}

	restartPolicyOnFailure := v1alpha1.RestartPolicy{
		Type:                             v1alpha1.OnFailure,
		OnFailureRetries:                 int32ptr(1),
		OnFailureRetryInterval:           int64ptr(100),
		OnSubmissionFailureRetryInterval: int64ptr(100),
		OnSubmissionFailureRetries:       int32ptr(2),
	}

	testcases := []testcase{
		{
			app:          &v1alpha1.SparkApplication{},
			shouldSubmit: true,
		},
		{
			app: &v1alpha1.SparkApplication{
				Spec: v1alpha1.SparkApplicationSpec{
					RestartPolicy: restartPolicyAlways,
				},
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{
						State: v1alpha1.CompletedState,
					},
				},
			},
			shouldSubmit: true,
		},
		{
			app: &v1alpha1.SparkApplication{
				Spec: v1alpha1.SparkApplicationSpec{
					RestartPolicy: restartPolicyAlways,
				},
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{
						State: v1alpha1.FailedSubmissionState,
					},
					SubmissionTime: metav1.Time{Time: metav1.Now().Add(-2000 * time.Second)},
				},
			},
			shouldSubmit: true,
		},
		{
			app: &v1alpha1.SparkApplication{
				Spec: v1alpha1.SparkApplicationSpec{
					RestartPolicy: restartPolicyAlways,
				},
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{
						State: v1alpha1.FailedState,
					},
					CompletionTime: metav1.Time{Time: metav1.Now().Add(-2000 * time.Second)},
				},
			},
			shouldSubmit: true,
		},
		{
			app: &v1alpha1.SparkApplication{
				Spec: v1alpha1.SparkApplicationSpec{
					RestartPolicy: restartPolicyAlways,
				},
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{
						State: v1alpha1.RunningState,
					},
				},
			},
			shouldSubmit: false,
		},
		{
			app: &v1alpha1.SparkApplication{
				Spec: v1alpha1.SparkApplicationSpec{
					RestartPolicy: restartPolicyAlways,
				},
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{
						State: v1alpha1.SubmittedState,
					},
				},
			},
			shouldSubmit: false,
		},
		{
			app: &v1alpha1.SparkApplication{
				Spec: v1alpha1.SparkApplicationSpec{
					RestartPolicy: restartPolicyNever,
				},
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{
						State: v1alpha1.CompletedState,
					},
				},
			},
			shouldSubmit: false,
		},
		{
			app: &v1alpha1.SparkApplication{
				Spec: v1alpha1.SparkApplicationSpec{
					RestartPolicy: restartPolicyNever,
				},
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{
						State: v1alpha1.NewState,
					},
				},
			},
			shouldSubmit: true,
		},
		{
			app:          &v1alpha1.SparkApplication{},
			shouldSubmit: true,
		},
		{
			app: &v1alpha1.SparkApplication{
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{
						State: v1alpha1.FailedState,
					},
					Attempts: 2,
				},
				Spec: v1alpha1.SparkApplicationSpec{
					RestartPolicy: restartPolicyOnFailure,
				},
			},
			shouldSubmit: false,
		},
		{
			app: &v1alpha1.SparkApplication{
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{
						State: v1alpha1.FailedState,
					},
					Attempts:       1,
					CompletionTime: metav1.Now(),
				},
				Spec: v1alpha1.SparkApplicationSpec{
					RestartPolicy: restartPolicyOnFailure,
				},
			},
			shouldSubmit: false,
		},
		{
			app: &v1alpha1.SparkApplication{
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{
						State: v1alpha1.FailedState,
					},
					Attempts:       1,
					CompletionTime: metav1.Time{Time: metav1.Now().Add(-2000 * time.Second)},
				},
				Spec: v1alpha1.SparkApplicationSpec{
					RestartPolicy: restartPolicyOnFailure,
				},
			},
			shouldSubmit: true,
		},
		{
			app: &v1alpha1.SparkApplication{
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{
						State: v1alpha1.FailedSubmissionState,
					},
					SubmissionAttempts: 3,
				},
				Spec: v1alpha1.SparkApplicationSpec{
					RestartPolicy: restartPolicyOnFailure,
				},
			},
			shouldSubmit: false,
		},
		{
			app: &v1alpha1.SparkApplication{
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{
						State: v1alpha1.FailedSubmissionState,
					},
					SubmissionAttempts: 1,
					SubmissionTime:     metav1.Now(),
				},
				Spec: v1alpha1.SparkApplicationSpec{
					RestartPolicy: restartPolicyOnFailure,
				},
			},
			shouldSubmit: false,
		},
		{
			app: &v1alpha1.SparkApplication{
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{
						State: v1alpha1.FailedSubmissionState,
					},
					SubmissionAttempts: 1,
					SubmissionTime:     metav1.Time{Time: metav1.Now().Add(-2000 * time.Second)},
				},
				Spec: v1alpha1.SparkApplicationSpec{
					RestartPolicy: restartPolicyOnFailure,
				},
			},
			shouldSubmit: true,
		},
	}

	for _, test := range testcases {
		testFn(test, t)
	}
}

func stringptr(s string) *string {
	return &s
}

func int32ptr(n int32) *int32 {
	return &n
}

func int64ptr(n int64) *int64 {
	return &n
}
