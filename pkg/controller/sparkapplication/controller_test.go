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
	kubeclientfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1alpha1"
	crdclientfake "k8s.io/spark-on-k8s-operator/pkg/client/clientset/versioned/fake"
	crdinformers "k8s.io/spark-on-k8s-operator/pkg/client/informers/externalversions"
	"k8s.io/spark-on-k8s-operator/pkg/util"
	"k8s.io/spark-on-k8s-operator/pkg/config"
)

func newFakeController(apps ...*v1alpha1.SparkApplication) (*Controller, *record.FakeRecorder) {
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

	controller := newSparkApplicationController(crdClient, kubeClient, apiExtensionsClient, informerFactory, recorder,
		&util.MetricConfig{}, "test")

	informer := informerFactory.Sparkoperator().V1alpha1().SparkApplications().Informer()
	for _, app := range apps {
		informer.GetIndexer().Add(app)
	}

	return controller, recorder
}

func TestOnAdd(t *testing.T) {
	ctrl, recorder := newFakeController()

	app := &v1alpha1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Status: v1alpha1.SparkApplicationStatus{
			AppID: "foo-123",
		},
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
	ctrl, recorder := newFakeController()

	type testcase struct {
		name          string
		oldApp        *v1alpha1.SparkApplication
		newApp        *v1alpha1.SparkApplication
		expectUpdate  bool
		expectRestart bool
	}

	testFn := func(t *testing.T, test testcase) {
		ctrl.onUpdate(test.oldApp, test.newApp)
		if test.expectUpdate {
			item, _ := ctrl.queue.Get()
			defer ctrl.queue.Done(item)
			key, ok := item.(string)
			assert.True(t, ok)
			expectedKey, _ := cache.MetaNamespaceKeyFunc(test.newApp)
			assert.Equal(t, expectedKey, key)
			ctrl.queue.Forget(item)

			event := <-recorder.Events
			assert.True(t, strings.Contains(event, "SparkApplicationUpdated"))
		} else if test.expectRestart {
			item, _ := ctrl.queue.Get()
			defer ctrl.queue.Done(item)
			key, ok := item.(string)
			assert.True(t, ok)
			expectedKey, _ := cache.MetaNamespaceKeyFunc(test.newApp)
			assert.Equal(t, expectedKey, key)
			ctrl.queue.Forget(item)

			event := <-recorder.Events
			assert.True(t, strings.Contains(event, "SparkApplicationRestart"))
		} else {
			assert.Equal(t, 0, ctrl.queue.Len())
			assert.Equal(t, 0, len(recorder.Events))
		}
	}

	appTemplate := v1alpha1.SparkApplication{
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

	copyWithDifferentImage := appTemplate.DeepCopy()
	copyWithDifferentImage.Spec.Image = stringptr("foo-image:v2")
	copyWithDifferentImage.ResourceVersion = "2"

	copyWithDifferentExecutorInstances := appTemplate.DeepCopy()
	copyWithDifferentExecutorInstances.Spec.Executor.Instances = int32ptr(3)
	copyWithDifferentExecutorInstances.ResourceVersion = "2"

	appWithAlwaysRestartPolicy := appTemplate.DeepCopy()
	appWithAlwaysRestartPolicy.Spec.RestartPolicy = v1alpha1.Always
	appWithAlwaysRestartPolicyCompleted := appWithAlwaysRestartPolicy.DeepCopy()
	appWithAlwaysRestartPolicyCompleted.Status.AppState.State = v1alpha1.CompletedState
	appWithAlwaysRestartPolicyCompleted.ResourceVersion = "2"

	appWithAlwaysRestartPolicyFailed := appWithAlwaysRestartPolicy.DeepCopy()
	appWithAlwaysRestartPolicyFailed.Status.AppState.State = v1alpha1.FailedState
	appWithAlwaysRestartPolicyFailed.ResourceVersion = "2"

	appWithOnFailureRestartPolicy := appTemplate.DeepCopy()
	appWithOnFailureRestartPolicy.Spec.RestartPolicy = v1alpha1.OnFailure
	appWithOnFailureRestartPolicyFailed := appWithOnFailureRestartPolicy.DeepCopy()
	appWithOnFailureRestartPolicyFailed.Status.AppState.State = v1alpha1.FailedState
	appWithOnFailureRestartPolicyFailed.ResourceVersion = "2"

	appWithOnFailureRestartPolicyCompleted := appWithOnFailureRestartPolicy.DeepCopy()
	appWithOnFailureRestartPolicyCompleted.Status.AppState.State = v1alpha1.CompletedState
	appWithOnFailureRestartPolicyCompleted.ResourceVersion = "2"

	testcases := []testcase{
		{
			name:          "update with identical spec",
			oldApp:        appTemplate.DeepCopy(),
			newApp:        appTemplate.DeepCopy(),
			expectUpdate:  false,
			expectRestart: false,
		},
		{
			name:          "update with different image",
			oldApp:        appTemplate.DeepCopy(),
			newApp:        copyWithDifferentImage,
			expectUpdate:  true,
			expectRestart: false,
		},
		{
			name:          "update with different executor instances",
			oldApp:        appTemplate.DeepCopy(),
			newApp:        copyWithDifferentExecutorInstances,
			expectUpdate:  true,
			expectRestart: false,
		},
		{
			name:          "update with Completed state and Always restart policy",
			oldApp:        appWithAlwaysRestartPolicy,
			newApp:        appWithAlwaysRestartPolicyCompleted,
			expectUpdate:  false,
			expectRestart: true,
		},
		{
			name:          "update with Failed state and Always restart policy",
			oldApp:        appWithAlwaysRestartPolicy,
			newApp:        appWithAlwaysRestartPolicyFailed,
			expectUpdate:  false,
			expectRestart: true,
		},
		{
			name:          "update with Failed state and OnFailure restart policy",
			oldApp:        appWithOnFailureRestartPolicy,
			newApp:        appWithOnFailureRestartPolicyFailed,
			expectUpdate:  false,
			expectRestart: true,
		},
		{
			name:          "update with Completed state and OnFailure restart policy",
			oldApp:        appWithOnFailureRestartPolicy,
			newApp:        appWithOnFailureRestartPolicyCompleted,
			expectUpdate:  false,
			expectRestart: false,
		},
	}

	for _, test := range testcases {
		testFn(t, test)
	}
}

func TestOnDelete(t *testing.T) {
	ctrl, recorder := newFakeController()

	app := &v1alpha1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Status: v1alpha1.SparkApplicationStatus{
			AppID: "foo-123",
		},
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
			AppID: "foo-123",
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

	submissionRetries := int32(1)
	app := &v1alpha1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Spec: v1alpha1.SparkApplicationSpec{
			MaxSubmissionRetries: &submissionRetries,
		},
		Status: v1alpha1.SparkApplicationStatus{
			AppID: "foo-123",
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

	ctrl.syncSparkApplication("default/foo")
	updatedApp, err := ctrl.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(app.Name, metav1.GetOptions{})
	assert.Equal(t, v1alpha1.FailedSubmissionState, updatedApp.Status.AppState.State)
	assert.Equal(t, float64(0), fetchCounterValue(ctrl.metrics.sparkAppSubmitCount, map[string]string{}))
	assert.Equal(t, float64(1), fetchCounterValue(ctrl.metrics.sparkAppFailureCount, map[string]string{}))

	event := <-recorder.Events
	assert.True(t, strings.Contains(event, "SparkApplicationSubmissionFailed"))

	// Verify App was retried on SubmissionFailure
	event = <-recorder.Events
	assert.True(t, strings.Contains(event, "SparkApplicationSubmissionRetry"))

	item, _ := ctrl.queue.Get()
	defer ctrl.queue.Done(item)
	key, ok := item.(string)
	assert.True(t, ok)
	expectedKey, _ := cache.MetaNamespaceKeyFunc(app)
	assert.Equal(t, expectedKey, key)

	// Try submitting again
	ctrl.syncSparkApplication("default/foo")

	// Verify App Failed again but not retried as we have exhausted MaxSubmissionRetries
	updatedApp, err = ctrl.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Get(app.Name, metav1.GetOptions{})
	assert.Equal(t, v1alpha1.FailedSubmissionState, updatedApp.Status.AppState.State)
	assert.Equal(t, int32(1), updatedApp.Status.SubmissionRetries)
	assert.Equal(t, float64(0), fetchCounterValue(ctrl.metrics.sparkAppSubmitCount, map[string]string{}))
	// We currently don't increment failure count if the same app is stuck in SubmissionFailed state.
	assert.Equal(t, float64(1), fetchCounterValue(ctrl.metrics.sparkAppFailureCount, map[string]string{}))

	event = <-recorder.Events
	assert.True(t, strings.Contains(event, "SparkApplicationSubmissionFailed"))
}

func TestSyncSparkApplication_RunningState(t *testing.T) {

	type testcase struct {
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

	submissionRetries := int32(1)
	app := &v1alpha1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Spec: v1alpha1.SparkApplicationSpec{
			MaxSubmissionRetries: &submissionRetries,
		},
		Status: v1alpha1.SparkApplicationStatus{
			AppID: "foo-123",
			AppState: v1alpha1.ApplicationState{
				State:        v1alpha1.SubmittedState,
				ErrorMessage: "",
			},
			ExecutorState: map[string]v1alpha1.ExecutorState{"exec-1": v1alpha1.ExecutorRunningState},
		},
	}
	testcases := []testcase{
		{
			oldAppStatus:          v1alpha1.SubmittedState,
			oldExecutorStatus:     map[string]v1alpha1.ExecutorState{"exec-1": v1alpha1.ExecutorRunningState},
			expectedAppState:      v1alpha1.FailedState,
			expectedExecutorState: map[string]v1alpha1.ExecutorState{"exec-1": v1alpha1.ExecutorFailedState},
			expectedAppMetrics: metrics{
				failedMetricCount: 1,
			},
			expectedExecutorMetrics: executorMetrics{
				failedMetricCount: 1,
			},
		},
		{
			oldAppStatus:      v1alpha1.SubmittedState,
			oldExecutorStatus: map[string]v1alpha1.ExecutorState{"exec-1": v1alpha1.ExecutorRunningState},
			driverPod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo-driver",
					Namespace: "default",
					Labels: map[string]string{
						sparkRoleLabel:           sparkDriverRole,
						config.SparkAppIDLabel:   "foo-123",
						config.SparkAppNameLabel: "foo",
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
					Namespace: "default",
					Labels: map[string]string{
						sparkRoleLabel:           sparkExecutorRole,
						config.SparkAppIDLabel:   "foo-123",
						config.SparkAppNameLabel: "foo",
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
			oldAppStatus:      v1alpha1.FailedState,
			oldExecutorStatus: map[string]v1alpha1.ExecutorState{"exec-1": v1alpha1.ExecutorCompletedState},
			driverPod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo-driver",
					Namespace: "default",
					Labels: map[string]string{
						sparkRoleLabel:           sparkDriverRole,
						config.SparkAppIDLabel:   "foo-123",
						config.SparkAppNameLabel: "foo",
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
					Namespace: "default",
					Labels: map[string]string{
						sparkRoleLabel:           sparkExecutorRole,
						config.SparkAppIDLabel:   "foo-123",
						config.SparkAppNameLabel: "foo",
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
		ctrl, _ := newFakeController(app)
		_, err := ctrl.crdClient.SparkoperatorV1alpha1().SparkApplications(app.Namespace).Create(app)
		if err != nil {
			t.Fatal(err)
		}
		if test.driverPod != nil {
			ctrl.kubeClient.CoreV1().Pods(app.Namespace).Create(test.driverPod)
		}
		if test.executorPod != nil {
			ctrl.kubeClient.CoreV1().Pods(app.Namespace).Create(test.executorPod)
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

		// Verify Executor Metrivs
		assert.Equal(t, test.expectedExecutorMetrics.runningMetricCount, ctrl.metrics.sparkAppExecutorRunningCount.Value(map[string]string{}))
		assert.Equal(t, test.expectedExecutorMetrics.successMetricCount, fetchCounterValue(ctrl.metrics.sparkAppExecutorSuccessCount, map[string]string{}))
		assert.Equal(t, test.expectedExecutorMetrics.failedMetricCount, fetchCounterValue(ctrl.metrics.sparkAppExecutorFailureCount, map[string]string{}))
	}

	for _, test := range testcases {
		testFn(test, t)
	}
}

func TestHandleRestart(t *testing.T) {
	type testcase struct {
		name          string
		app           *v1alpha1.SparkApplication
		expectRestart bool
	}

	os.Setenv(kubernetesServiceHostEnvVar, "localhost")
	os.Setenv(kubernetesServicePortEnvVar, "443")

	testFn := func(test testcase, t *testing.T) {
		ctrl, recorder := newFakeController(test.app)
		_, err := ctrl.crdClient.SparkoperatorV1alpha1().SparkApplications(test.app.Namespace).Create(test.app)
		if err != nil {
			t.Fatal(err)
		}

		ctrl.handleRestart(test.app)
		if test.expectRestart {
			go ctrl.processNextItem()
			item, _ := ctrl.queue.Get()
			key, ok := item.(string)
			assert.True(t, ok)
			expectedKey, _ := getApplicationKey(test.app.Namespace, test.app.Name)
			assert.Equal(t, expectedKey, key)
			event := <-recorder.Events
			assert.True(t, strings.Contains(event, "SparkApplicationRestart"))
		}
	}

	testcases := []testcase{
		{
			name: "completed application with restart policy Never",
			app: &v1alpha1.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{Name: "foo1", Namespace: "default"},
				Spec:       v1alpha1.SparkApplicationSpec{RestartPolicy: v1alpha1.Never},
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{State: v1alpha1.CompletedState},
				},
			},
			expectRestart: false,
		},
		{
			name: "completed application with restart policy OnFailure",
			app: &v1alpha1.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{Name: "foo2", Namespace: "default"},
				Spec:       v1alpha1.SparkApplicationSpec{RestartPolicy: v1alpha1.OnFailure},
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{State: v1alpha1.CompletedState},
				},
			},
			expectRestart: false,
		},
		{
			name: "completed application with restart policy Always",
			app: &v1alpha1.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{Name: "foo3", Namespace: "default"},
				Spec:       v1alpha1.SparkApplicationSpec{RestartPolicy: v1alpha1.Always},
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{State: v1alpha1.CompletedState},
				},
			},
			expectRestart: true,
		},
		{
			name: "failed application with restart policy Never",
			app: &v1alpha1.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{Name: "foo4", Namespace: "default"},
				Spec:       v1alpha1.SparkApplicationSpec{RestartPolicy: v1alpha1.Never},
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{State: v1alpha1.FailedState},
				},
			},
			expectRestart: false,
		},
		{
			name: "failed application with restart policy OnFailure",
			app: &v1alpha1.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{Name: "foo5", Namespace: "default"},
				Spec:       v1alpha1.SparkApplicationSpec{RestartPolicy: v1alpha1.OnFailure},
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{State: v1alpha1.FailedState},
				},
			},
			expectRestart: true,
		},
		{
			name: "failed application with restart policy Always",
			app: &v1alpha1.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{Name: "foo6", Namespace: "default"},
				Spec:       v1alpha1.SparkApplicationSpec{RestartPolicy: v1alpha1.Always},
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{State: v1alpha1.FailedState},
				},
			},
			expectRestart: true,
		},
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

	testcases := []testcase{
		{
			app:          &v1alpha1.SparkApplication{},
			shouldSubmit: true,
		},
		{
			app: &v1alpha1.SparkApplication{
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{State: v1alpha1.NewState},
				},
			},
			shouldSubmit: true,
		},
		{
			app: &v1alpha1.SparkApplication{
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{State: v1alpha1.SubmittedState},
				},
			},
			shouldSubmit: false,
		},
		{
			app: &v1alpha1.SparkApplication{
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{State: v1alpha1.RunningState},
				},
			},
			shouldSubmit: false,
		},
		{
			app: &v1alpha1.SparkApplication{
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{State: v1alpha1.FailedState},
				},
			},
			shouldSubmit: false,
		},
		{
			app: &v1alpha1.SparkApplication{
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{State: v1alpha1.CompletedState},
				},
			},
			shouldSubmit: false,
		},
		{
			app: &v1alpha1.SparkApplication{
				Spec: v1alpha1.SparkApplicationSpec{RestartPolicy: v1alpha1.OnFailure},
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{State: v1alpha1.FailedState},
				},
			},
			shouldSubmit: true,
		},
		{
			app: &v1alpha1.SparkApplication{
				Spec: v1alpha1.SparkApplicationSpec{RestartPolicy: v1alpha1.Always},
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{State: v1alpha1.FailedState},
				},
			},
			shouldSubmit: true,
		},
		{
			app: &v1alpha1.SparkApplication{
				Spec: v1alpha1.SparkApplicationSpec{RestartPolicy: v1alpha1.Always},
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{State: v1alpha1.CompletedState},
				},
			},
			shouldSubmit: true,
		},
		{
			app: &v1alpha1.SparkApplication{
				Spec: v1alpha1.SparkApplicationSpec{MaxSubmissionRetries: int32ptr(1)},
				Status: v1alpha1.SparkApplicationStatus{
					SubmissionRetries: 0,
					AppState:          v1alpha1.ApplicationState{State: v1alpha1.FailedSubmissionState},
				},
			},
			shouldSubmit: true,
		},
		{
			app: &v1alpha1.SparkApplication{
				Spec: v1alpha1.SparkApplicationSpec{MaxSubmissionRetries: int32ptr(1)},
				Status: v1alpha1.SparkApplicationStatus{
					SubmissionRetries: 1,
					AppState:          v1alpha1.ApplicationState{State: v1alpha1.FailedSubmissionState},
				},
			},
			shouldSubmit: false,
		},
	}

	for _, test := range testcases {
		testFn(test, t)
	}
}

func TestShouldRestart(t *testing.T) {
	type testcase struct {
		app           *v1alpha1.SparkApplication
		shouldRestart bool
	}

	testFn := func(test testcase, t *testing.T) {
		assert.Equal(t, test.shouldRestart, shouldRestart(test.app))
	}

	testcases := []testcase{
		{
			app: &v1alpha1.SparkApplication{
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{State: v1alpha1.FailedState},
				},
			},
			shouldRestart: false,
		},
		{
			app: &v1alpha1.SparkApplication{
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{State: v1alpha1.CompletedState},
				},
			},
			shouldRestart: false,
		},
		{
			app: &v1alpha1.SparkApplication{
				Spec: v1alpha1.SparkApplicationSpec{RestartPolicy: v1alpha1.OnFailure},
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{State: v1alpha1.FailedState},
				},
			},
			shouldRestart: true,
		},
		{
			app: &v1alpha1.SparkApplication{
				Spec: v1alpha1.SparkApplicationSpec{RestartPolicy: v1alpha1.Always},
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{State: v1alpha1.FailedState},
				},
			},
			shouldRestart: true,
		},
		{
			app: &v1alpha1.SparkApplication{
				Spec: v1alpha1.SparkApplicationSpec{RestartPolicy: v1alpha1.Always},
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{State: v1alpha1.CompletedState},
				},
			},
			shouldRestart: true,
		},
	}

	for _, test := range testcases {
		testFn(test, t)
	}
}

func TestShouldRetrySubmission(t *testing.T) {
	type testcase struct {
		app                   *v1alpha1.SparkApplication
		shouldRetrySubmission bool
	}

	testFn := func(test testcase, t *testing.T) {
		assert.Equal(t, test.shouldRetrySubmission, shouldRetrySubmission(test.app))
	}

	testcases := []testcase{
		{
			app: &v1alpha1.SparkApplication{
				Status: v1alpha1.SparkApplicationStatus{
					AppState: v1alpha1.ApplicationState{State: v1alpha1.FailedSubmissionState},
				},
			},
			shouldRetrySubmission: false,
		},
		{
			app: &v1alpha1.SparkApplication{
				Spec: v1alpha1.SparkApplicationSpec{MaxSubmissionRetries: int32ptr(1)},
				Status: v1alpha1.SparkApplicationStatus{
					SubmissionRetries: 0,
					AppState:          v1alpha1.ApplicationState{State: v1alpha1.FailedSubmissionState},
				},
			},
			shouldRetrySubmission: true,
		},
		{
			app: &v1alpha1.SparkApplication{
				Spec: v1alpha1.SparkApplicationSpec{MaxSubmissionRetries: int32ptr(1)},
				Status: v1alpha1.SparkApplicationStatus{
					SubmissionRetries: 1,
					AppState:          v1alpha1.ApplicationState{State: v1alpha1.FailedSubmissionState},
				},
			},
			shouldRetrySubmission: false,
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
