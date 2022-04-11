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
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	prometheus_model "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	kubeclientfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	crdclientfake "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned/fake"
	crdinformers "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/informers/externalversions"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/util"
)

func newFakeController(app *v1beta2.SparkApplication, pods ...*apiv1.Pod) (*Controller, *record.FakeRecorder) {
	crdclientfake.AddToScheme(scheme.Scheme)
	crdClient := crdclientfake.NewSimpleClientset()
	kubeClient := kubeclientfake.NewSimpleClientset()
	util.IngressCapabilities = map[string]bool{"networking.k8s.io/v1": true}
	informerFactory := crdinformers.NewSharedInformerFactory(crdClient, 0*time.Second)
	recorder := record.NewFakeRecorder(3)

	kubeClient.CoreV1().Nodes().Create(context.TODO(), &apiv1.Node{
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
	}, metav1.CreateOptions{})

	podInformerFactory := informers.NewSharedInformerFactory(kubeClient, 0*time.Second)
	controller := newSparkApplicationController(crdClient, kubeClient, informerFactory, podInformerFactory, recorder,
		&util.MetricConfig{}, "", "", nil, true)

	informer := informerFactory.Sparkoperator().V1beta2().SparkApplications().Informer()
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
	ctrl, _ := newFakeController(nil)

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Status: v1beta2.SparkApplicationStatus{},
	}
	ctrl.onAdd(app)

	item, _ := ctrl.queue.Get()
	defer ctrl.queue.Done(item)
	key, ok := item.(string)
	assert.True(t, ok)
	expectedKey, _ := cache.MetaNamespaceKeyFunc(app)
	assert.Equal(t, expectedKey, key)
	ctrl.queue.Forget(item)
}

func TestOnUpdate(t *testing.T) {
	ctrl, recorder := newFakeController(nil)

	appTemplate := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       "default",
			ResourceVersion: "1",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Mode:  v1beta2.ClusterMode,
			Image: stringptr("foo-image:v1"),
			Executor: v1beta2.ExecutorSpec{
				Instances: int32ptr(1),
			},
		},
	}

	// Case1: Same Spec.
	copyWithSameSpec := appTemplate.DeepCopy()
	copyWithSameSpec.Status.ExecutionAttempts = 3
	copyWithSameSpec.ResourceVersion = "2"

	ctrl.onUpdate(appTemplate, copyWithSameSpec)

	// Verify that the SparkApplication was enqueued but no spec update events fired.
	item, _ := ctrl.queue.Get()
	key, ok := item.(string)
	assert.True(t, ok)
	expectedKey, _ := cache.MetaNamespaceKeyFunc(appTemplate)
	assert.Equal(t, expectedKey, key)
	ctrl.queue.Forget(item)
	ctrl.queue.Done(item)
	assert.Equal(t, 0, len(recorder.Events))

	// Case2: Spec update failed.
	copyWithSpecUpdate := appTemplate.DeepCopy()
	copyWithSpecUpdate.Spec.Image = stringptr("foo-image:v2")
	copyWithSpecUpdate.ResourceVersion = "2"

	ctrl.onUpdate(appTemplate, copyWithSpecUpdate)

	// Verify that update failed due to non-existence of SparkApplication.
	assert.Equal(t, 1, len(recorder.Events))
	event := <-recorder.Events
	assert.True(t, strings.Contains(event, "SparkApplicationSpecUpdateFailed"))

	// Case3: Spec update successful.
	ctrl.crdClient.SparkoperatorV1beta2().SparkApplications(appTemplate.Namespace).Create(context.TODO(), appTemplate, metav1.CreateOptions{})
	ctrl.onUpdate(appTemplate, copyWithSpecUpdate)

	// Verify App was enqueued.
	item, _ = ctrl.queue.Get()
	key, ok = item.(string)
	assert.True(t, ok)
	expectedKey, _ = cache.MetaNamespaceKeyFunc(appTemplate)
	assert.Equal(t, expectedKey, key)
	ctrl.queue.Forget(item)
	ctrl.queue.Done(item)
	// Verify that update was succeeded.
	assert.Equal(t, 1, len(recorder.Events))
	event = <-recorder.Events
	assert.True(t, strings.Contains(event, "SparkApplicationSpecUpdateProcessed"))

	// Verify the SparkApplication state was updated to InvalidatingState.
	app, err := ctrl.crdClient.SparkoperatorV1beta2().SparkApplications(appTemplate.Namespace).Get(context.TODO(), appTemplate.Name, metav1.GetOptions{})
	assert.Nil(t, err)
	assert.Equal(t, v1beta2.InvalidatingState, app.Status.AppState.State)
}

func TestOnDelete(t *testing.T) {
	ctrl, recorder := newFakeController(nil)

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Status: v1beta2.SparkApplicationStatus{},
	}
	ctrl.onAdd(app)
	ctrl.queue.Get()

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

func TestSyncSparkApplication_SubmissionFailed(t *testing.T) {
	os.Setenv(sparkHomeEnvVar, "/spark")
	os.Setenv(kubernetesServiceHostEnvVar, "localhost")
	os.Setenv(kubernetesServicePortEnvVar, "443")

	restartPolicyOnFailure := v1beta2.RestartPolicy{
		Type:                             v1beta2.OnFailure,
		OnFailureRetries:                 int32ptr(1),
		OnFailureRetryInterval:           int64ptr(100),
		OnSubmissionFailureRetryInterval: int64ptr(100),
		OnSubmissionFailureRetries:       int32ptr(1),
	}
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Spec: v1beta2.SparkApplicationSpec{
			RestartPolicy: restartPolicyOnFailure,
		},
		Status: v1beta2.SparkApplicationStatus{
			AppState: v1beta2.ApplicationState{
				State:        v1beta2.NewState,
				ErrorMessage: "",
			},
		},
	}

	ctrl, recorder := newFakeController(app)
	_, err := ctrl.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Create(context.TODO(), app, metav1.CreateOptions{})
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
	updatedApp, err := ctrl.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(context.TODO(), app.Name, metav1.GetOptions{})

	assert.Equal(t, v1beta2.FailedSubmissionState, updatedApp.Status.AppState.State)
	assert.Equal(t, int32(1), updatedApp.Status.SubmissionAttempts)
	assert.Equal(t, float64(1), fetchCounterValue(ctrl.metrics.sparkAppCount, map[string]string{}))
	assert.Equal(t, float64(0), fetchCounterValue(ctrl.metrics.sparkAppSubmitCount, map[string]string{}))
	assert.Equal(t, float64(1), fetchCounterValue(ctrl.metrics.sparkAppFailedSubmissionCount, map[string]string{}))

	event := <-recorder.Events
	assert.True(t, strings.Contains(event, "SparkApplicationAdded"))
	event = <-recorder.Events
	assert.True(t, strings.Contains(event, "SparkApplicationSubmissionFailed"))

	// Attempt 2: Retry again.
	updatedApp.Status.LastSubmissionAttemptTime = metav1.Time{Time: metav1.Now().Add(-100 * time.Second)}
	ctrl, recorder = newFakeController(updatedApp)
	_, err = ctrl.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Create(context.TODO(), updatedApp, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	err = ctrl.syncSparkApplication("default/foo")

	// Verify that the application failed again.
	updatedApp, err = ctrl.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(context.TODO(), app.Name, metav1.GetOptions{})
	assert.Nil(t, err)
	assert.Equal(t, v1beta2.FailedSubmissionState, updatedApp.Status.AppState.State)
	assert.Equal(t, int32(2), updatedApp.Status.SubmissionAttempts)
	assert.Equal(t, float64(0), fetchCounterValue(ctrl.metrics.sparkAppSubmitCount, map[string]string{}))

	event = <-recorder.Events
	assert.True(t, strings.Contains(event, "SparkApplicationSubmissionFailed"))

	// Attempt 3: No more retries.
	updatedApp.Status.LastSubmissionAttemptTime = metav1.Time{Time: metav1.Now().Add(-100 * time.Second)}
	ctrl, recorder = newFakeController(updatedApp)
	_, err = ctrl.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Create(context.TODO(), updatedApp, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	err = ctrl.syncSparkApplication("default/foo")

	// Verify that the application failed again.
	updatedApp, err = ctrl.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(context.TODO(), app.Name, metav1.GetOptions{})
	assert.Nil(t, err)
	assert.Equal(t, v1beta2.FailedState, updatedApp.Status.AppState.State)
	// No more submission attempts made.
	assert.Equal(t, int32(2), updatedApp.Status.SubmissionAttempts)
}

func TestValidateDetectsNodeSelectorSuccessNoSelector(t *testing.T) {
	ctrl, _ := newFakeController(nil)

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
	}

	err := ctrl.validateSparkApplication(app)
	assert.Nil(t, err)
}

func TestValidateDetectsNodeSelectorSuccessNodeSelectorAtAppLevel(t *testing.T) {
	ctrl, _ := newFakeController(nil)

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Spec: v1beta2.SparkApplicationSpec{
			NodeSelector: map[string]string{"mynode": "mygift"},
		},
	}

	err := ctrl.validateSparkApplication(app)
	assert.Nil(t, err)
}

func TestValidateDetectsNodeSelectorSuccessNodeSelectorAtPodLevel(t *testing.T) {
	ctrl, _ := newFakeController(nil)

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					NodeSelector: map[string]string{"mynode": "mygift"},
				},
			},
		},
	}

	err := ctrl.validateSparkApplication(app)
	assert.Nil(t, err)

	app.Spec.Executor = v1beta2.ExecutorSpec{
		SparkPodSpec: v1beta2.SparkPodSpec{
			NodeSelector: map[string]string{"mynode": "mygift"},
		},
	}

	err = ctrl.validateSparkApplication(app)
	assert.Nil(t, err)
}

func TestValidateDetectsNodeSelectorFailsAppAndPodLevel(t *testing.T) {
	ctrl, _ := newFakeController(nil)

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Spec: v1beta2.SparkApplicationSpec{
			NodeSelector: map[string]string{"mynode": "mygift"},
			Driver: v1beta2.DriverSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					NodeSelector: map[string]string{"mynode": "mygift"},
				},
			},
		},
	}

	err := ctrl.validateSparkApplication(app)
	assert.NotNil(t, err)

	app.Spec.Executor = v1beta2.ExecutorSpec{
		SparkPodSpec: v1beta2.SparkPodSpec{
			NodeSelector: map[string]string{"mynode": "mygift"},
		},
	}

	err = ctrl.validateSparkApplication(app)
	assert.NotNil(t, err)
}

func TestShouldRetry(t *testing.T) {
	type testcase struct {
		app         *v1beta2.SparkApplication
		shouldRetry bool
	}

	testFn := func(test testcase, t *testing.T) {
		shouldRetry := shouldRetry(test.app)
		assert.Equal(t, test.shouldRetry, shouldRetry)
	}

	restartPolicyAlways := v1beta2.RestartPolicy{
		Type:                             v1beta2.Always,
		OnSubmissionFailureRetryInterval: int64ptr(100),
		OnFailureRetryInterval:           int64ptr(100),
	}

	restartPolicyNever := v1beta2.RestartPolicy{
		Type: v1beta2.Never,
	}

	restartPolicyOnFailure := v1beta2.RestartPolicy{
		Type:                             v1beta2.OnFailure,
		OnFailureRetries:                 int32ptr(1),
		OnFailureRetryInterval:           int64ptr(100),
		OnSubmissionFailureRetryInterval: int64ptr(100),
		OnSubmissionFailureRetries:       int32ptr(2),
	}

	testcases := []testcase{
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				}},
			shouldRetry: false,
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					RestartPolicy: restartPolicyAlways,
				},
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.SucceedingState,
					},
				},
			},
			shouldRetry: true,
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					RestartPolicy: restartPolicyOnFailure,
				},
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.SucceedingState,
					},
				},
			},
			shouldRetry: false,
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					RestartPolicy: restartPolicyOnFailure,
				},
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.FailingState,
					},
				},
			},
			shouldRetry: true,
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					RestartPolicy: restartPolicyNever,
				},
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.FailingState,
					},
				},
			},
			shouldRetry: false,
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					RestartPolicy: restartPolicyNever,
				},
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.FailedSubmissionState,
					},
				},
			},
			shouldRetry: false,
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					RestartPolicy: restartPolicyOnFailure,
				},
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.FailedSubmissionState,
					},
				},
			},
			shouldRetry: true,
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					RestartPolicy: restartPolicyAlways,
				},
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.PendingRerunState,
					},
				},
			},
			shouldRetry: false,
		},
	}

	for _, test := range testcases {
		testFn(test, t)
	}
}

func TestSyncSparkApplication_SubmissionSuccess(t *testing.T) {
	type testcase struct {
		app           *v1beta2.SparkApplication
		expectedState v1beta2.ApplicationStateType
	}
	os.Setenv(sparkHomeEnvVar, "/spark")
	os.Setenv(kubernetesServiceHostEnvVar, "localhost")
	os.Setenv(kubernetesServicePortEnvVar, "443")

	testFn := func(test testcase, t *testing.T) {
		ctrl, _ := newFakeController(test.app)
		_, err := ctrl.crdClient.SparkoperatorV1beta2().SparkApplications(test.app.Namespace).Create(context.TODO(), test.app, metav1.CreateOptions{})
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

		err = ctrl.syncSparkApplication(fmt.Sprintf("%s/%s", test.app.Namespace, test.app.Name))
		assert.Nil(t, err)
		updatedApp, err := ctrl.crdClient.SparkoperatorV1beta2().SparkApplications(test.app.Namespace).Get(context.TODO(), test.app.Name, metav1.GetOptions{})
		assert.Nil(t, err)
		assert.Equal(t, test.expectedState, updatedApp.Status.AppState.State)
		if test.app.Status.AppState.State == v1beta2.NewState {
			assert.Equal(t, float64(1), fetchCounterValue(ctrl.metrics.sparkAppCount, map[string]string{}))
		}
		if test.expectedState == v1beta2.SubmittedState {
			assert.Equal(t, float64(1), fetchCounterValue(ctrl.metrics.sparkAppSubmitCount, map[string]string{}))
		}
	}

	restartPolicyAlways := v1beta2.RestartPolicy{
		Type:                             v1beta2.Always,
		OnSubmissionFailureRetryInterval: int64ptr(100),
		OnFailureRetryInterval:           int64ptr(100),
	}

	restartPolicyNever := v1beta2.RestartPolicy{
		Type: v1beta2.Never,
	}

	restartPolicyOnFailure := v1beta2.RestartPolicy{
		Type:                             v1beta2.OnFailure,
		OnFailureRetries:                 int32ptr(1),
		OnFailureRetryInterval:           int64ptr(100),
		OnSubmissionFailureRetryInterval: int64ptr(100),
		OnSubmissionFailureRetries:       int32ptr(2),
	}

	testcases := []testcase{
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				}},
			expectedState: v1beta2.SubmittedState,
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					RestartPolicy: restartPolicyAlways,
				},
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.SucceedingState,
					},
				},
			},
			expectedState: v1beta2.PendingRerunState,
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					RestartPolicy: restartPolicyAlways,
				},
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.PendingRerunState,
					},
				},
			},
			expectedState: v1beta2.SubmittedState,
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					RestartPolicy: restartPolicyAlways,
				},
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.FailedSubmissionState,
					},
					LastSubmissionAttemptTime: metav1.Time{Time: metav1.Now().Add(-2000 * time.Second)},
				},
			},
			expectedState: v1beta2.FailedSubmissionState,
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					RestartPolicy: restartPolicyAlways,
				},
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.FailedSubmissionState,
					},
					SubmissionAttempts:        1,
					LastSubmissionAttemptTime: metav1.Time{Time: metav1.Now().Add(-2000 * time.Second)},
				},
			},
			expectedState: v1beta2.SubmittedState,
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					RestartPolicy: restartPolicyAlways,
				},
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.FailingState,
					},
					ExecutionAttempts: 1,
					TerminationTime:   metav1.Time{Time: metav1.Now().Add(-2000 * time.Second)},
				},
			},
			expectedState: v1beta2.PendingRerunState,
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					RestartPolicy: restartPolicyAlways,
				},
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.FailingState,
					},
					TerminationTime: metav1.Time{Time: metav1.Now().Add(-2000 * time.Second)},
				},
			},
			expectedState: v1beta2.FailingState,
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					RestartPolicy: restartPolicyNever,
				},
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.InvalidatingState,
					},
					TerminationTime: metav1.Time{Time: metav1.Now().Add(-2000 * time.Second)},
				},
			},
			expectedState: v1beta2.PendingRerunState,
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					RestartPolicy: restartPolicyNever,
				},
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.SucceedingState,
					},
				},
			},
			expectedState: v1beta2.CompletedState,
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Spec: v1beta2.SparkApplicationSpec{
					RestartPolicy: restartPolicyNever,
				},
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.NewState,
					},
				},
			},
			expectedState: v1beta2.SubmittedState,
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.FailingState,
					},
					ExecutionAttempts: 2,
				},
				Spec: v1beta2.SparkApplicationSpec{
					RestartPolicy: restartPolicyOnFailure,
				},
			},
			expectedState: v1beta2.FailedState,
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.FailingState,
					},
					ExecutionAttempts: 1,
					TerminationTime:   metav1.Now(),
				},
				Spec: v1beta2.SparkApplicationSpec{
					RestartPolicy: restartPolicyOnFailure,
				},
			},
			expectedState: v1beta2.FailingState,
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.FailingState,
					},
					ExecutionAttempts: 1,
					TerminationTime:   metav1.Time{Time: metav1.Now().Add(-2000 * time.Second)},
				},
				Spec: v1beta2.SparkApplicationSpec{
					RestartPolicy: restartPolicyOnFailure,
				},
			},
			expectedState: v1beta2.PendingRerunState,
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.FailedSubmissionState,
					},
					SubmissionAttempts: 3,
				},
				Spec: v1beta2.SparkApplicationSpec{
					RestartPolicy: restartPolicyOnFailure,
				},
			},
			expectedState: v1beta2.FailedState,
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.FailedSubmissionState,
					},
					SubmissionAttempts:        1,
					LastSubmissionAttemptTime: metav1.Now(),
				},
				Spec: v1beta2.SparkApplicationSpec{
					RestartPolicy: restartPolicyOnFailure,
				},
			},
			expectedState: v1beta2.FailedSubmissionState,
		},
		{
			app: &v1beta2.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Status: v1beta2.SparkApplicationStatus{
					AppState: v1beta2.ApplicationState{
						State: v1beta2.FailedSubmissionState,
					},
					SubmissionAttempts:        1,
					LastSubmissionAttemptTime: metav1.Time{Time: metav1.Now().Add(-2000 * time.Second)},
				},
				Spec: v1beta2.SparkApplicationSpec{
					RestartPolicy: restartPolicyOnFailure,
				},
			},
			expectedState: v1beta2.SubmittedState,
		},
	}

	for _, test := range testcases {
		testFn(test, t)
	}
}

func TestSyncSparkApplication_ExecutingState(t *testing.T) {
	type testcase struct {
		appName                 string
		oldAppStatus            v1beta2.ApplicationStateType
		oldExecutorStatus       map[string]v1beta2.ExecutorState
		driverPod               *apiv1.Pod
		executorPod             *apiv1.Pod
		expectedAppState        v1beta2.ApplicationStateType
		expectedExecutorState   map[string]v1beta2.ExecutorState
		expectedAppMetrics      metrics
		expectedExecutorMetrics executorMetrics
	}

	os.Setenv(kubernetesServiceHostEnvVar, "localhost")
	os.Setenv(kubernetesServicePortEnvVar, "443")

	appName := "foo"
	driverPodName := appName + "-driver"

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      appName,
			Namespace: "test",
		},
		Spec: v1beta2.SparkApplicationSpec{
			RestartPolicy: v1beta2.RestartPolicy{
				Type: v1beta2.Never,
			},
		},
		Status: v1beta2.SparkApplicationStatus{
			AppState: v1beta2.ApplicationState{
				State:        v1beta2.SubmittedState,
				ErrorMessage: "",
			},
			DriverInfo: v1beta2.DriverInfo{
				PodName: driverPodName,
			},
			ExecutorState: map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorRunningState},
		},
	}

	testcases := []testcase{
		{
			appName:               appName,
			oldAppStatus:          v1beta2.SubmittedState,
			oldExecutorStatus:     map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorRunningState},
			expectedAppState:      v1beta2.FailingState,
			expectedExecutorState: map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorFailedState},
			expectedAppMetrics: metrics{
				failedMetricCount: 1,
			},
			expectedExecutorMetrics: executorMetrics{
				failedMetricCount: 1,
			},
		},
		{
			appName:           appName,
			oldAppStatus:      v1beta2.SubmittedState,
			oldExecutorStatus: map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorRunningState},
			driverPod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      driverPodName,
					Namespace: "test",
					Labels: map[string]string{
						config.SparkRoleLabel:    config.SparkDriverRole,
						config.SparkAppNameLabel: appName,
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
						config.SparkRoleLabel:    config.SparkExecutorRole,
						config.SparkAppNameLabel: appName,
					},
					ResourceVersion: "1",
				},
				Status: apiv1.PodStatus{
					Phase: apiv1.PodSucceeded,
				},
			},
			expectedAppState:      v1beta2.RunningState,
			expectedExecutorState: map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorCompletedState},
			expectedAppMetrics: metrics{
				runningMetricCount: 1,
			},
			expectedExecutorMetrics: executorMetrics{
				successMetricCount: 1,
			},
		},
		{
			appName:           appName,
			oldAppStatus:      v1beta2.RunningState,
			oldExecutorStatus: map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorRunningState},
			driverPod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      driverPodName,
					Namespace: "test",
					Labels: map[string]string{
						config.SparkRoleLabel:    config.SparkDriverRole,
						config.SparkAppNameLabel: appName,
					},
					ResourceVersion: "1",
				},
				Status: apiv1.PodStatus{
					Phase: apiv1.PodRunning,
					ContainerStatuses: []apiv1.ContainerStatus{
						{
							Name: config.SparkDriverContainerName,
							State: apiv1.ContainerState{
								Running: &apiv1.ContainerStateRunning{},
							},
						},
						{
							Name: "sidecar",
							State: apiv1.ContainerState{
								Terminated: &apiv1.ContainerStateTerminated{
									ExitCode: 0,
								},
							},
						},
					},
				},
			},
			executorPod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "exec-1",
					Namespace: "test",
					Labels: map[string]string{
						config.SparkRoleLabel:    config.SparkExecutorRole,
						config.SparkAppNameLabel: appName,
					},
					ResourceVersion: "1",
				},
				Status: apiv1.PodStatus{
					Phase: apiv1.PodSucceeded,
				},
			},
			expectedAppState:      v1beta2.RunningState,
			expectedExecutorState: map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorCompletedState},
			expectedAppMetrics:    metrics{},
			expectedExecutorMetrics: executorMetrics{
				successMetricCount: 1,
			},
		},
		{
			appName:           appName,
			oldAppStatus:      v1beta2.RunningState,
			oldExecutorStatus: map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorRunningState},
			driverPod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      driverPodName,
					Namespace: "test",
					Labels: map[string]string{
						config.SparkRoleLabel:    config.SparkDriverRole,
						config.SparkAppNameLabel: appName,
					},
					ResourceVersion: "1",
				},
				Status: apiv1.PodStatus{
					Phase: apiv1.PodRunning,
					ContainerStatuses: []apiv1.ContainerStatus{
						{
							Name: config.SparkDriverContainerName,
							State: apiv1.ContainerState{
								Terminated: &apiv1.ContainerStateTerminated{
									ExitCode: 0,
								},
							},
						},
						{
							Name: "sidecar",
							State: apiv1.ContainerState{
								Running: &apiv1.ContainerStateRunning{},
							},
						},
					},
				},
			},
			executorPod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "exec-1",
					Namespace: "test",
					Labels: map[string]string{
						config.SparkRoleLabel:    config.SparkExecutorRole,
						config.SparkAppNameLabel: appName,
					},
					ResourceVersion: "1",
				},
				Status: apiv1.PodStatus{
					Phase: apiv1.PodSucceeded,
				},
			},
			expectedAppState:      v1beta2.SucceedingState,
			expectedExecutorState: map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorCompletedState},
			expectedAppMetrics: metrics{
				successMetricCount: 1,
			},
			expectedExecutorMetrics: executorMetrics{
				successMetricCount: 1,
			},
		},
		{
			appName:           appName,
			oldAppStatus:      v1beta2.RunningState,
			oldExecutorStatus: map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorRunningState},
			driverPod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      driverPodName,
					Namespace: "test",
					Labels: map[string]string{
						config.SparkRoleLabel:    config.SparkDriverRole,
						config.SparkAppNameLabel: appName,
					},
					ResourceVersion: "1",
				},
				Status: apiv1.PodStatus{
					Phase: apiv1.PodRunning,
					ContainerStatuses: []apiv1.ContainerStatus{
						{
							Name: config.SparkDriverContainerName,
							State: apiv1.ContainerState{
								Terminated: &apiv1.ContainerStateTerminated{
									ExitCode: 137,
									Reason:   "OOMKilled",
								},
							},
						},
						{
							Name: "sidecar",
							State: apiv1.ContainerState{
								Running: &apiv1.ContainerStateRunning{},
							},
						},
					},
				},
			},
			executorPod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "exec-1",
					Namespace: "test",
					Labels: map[string]string{
						config.SparkRoleLabel:    config.SparkExecutorRole,
						config.SparkAppNameLabel: appName,
					},
					ResourceVersion: "1",
				},
				Status: apiv1.PodStatus{
					Phase: apiv1.PodSucceeded,
				},
			},
			expectedAppState:      v1beta2.FailingState,
			expectedExecutorState: map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorCompletedState},
			expectedAppMetrics: metrics{
				failedMetricCount: 1,
			},
			expectedExecutorMetrics: executorMetrics{
				successMetricCount: 1,
			},
		},
		{
			appName:           appName,
			oldAppStatus:      v1beta2.RunningState,
			oldExecutorStatus: map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorRunningState},
			driverPod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      driverPodName,
					Namespace: "test",
					Labels: map[string]string{
						config.SparkRoleLabel:    config.SparkDriverRole,
						config.SparkAppNameLabel: appName,
					},
					ResourceVersion: "1",
				},
				Status: apiv1.PodStatus{
					Phase: apiv1.PodFailed,
					ContainerStatuses: []apiv1.ContainerStatus{
						{
							Name: config.SparkDriverContainerName,
							State: apiv1.ContainerState{
								Terminated: &apiv1.ContainerStateTerminated{
									ExitCode: 137,
									Reason:   "OOMKilled",
								},
							},
						},
					},
				},
			},
			executorPod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "exec-1",
					Namespace: "test",
					Labels: map[string]string{
						config.SparkRoleLabel:    config.SparkExecutorRole,
						config.SparkAppNameLabel: appName,
					},
					ResourceVersion: "1",
				},
				Status: apiv1.PodStatus{
					Phase: apiv1.PodFailed,
					ContainerStatuses: []apiv1.ContainerStatus{
						{
							Name: config.SparkExecutorContainerName,
							State: apiv1.ContainerState{
								Terminated: &apiv1.ContainerStateTerminated{
									ExitCode: 137,
									Reason:   "OOMKilled",
								},
							},
						},
					},
				},
			},
			expectedAppState:      v1beta2.FailingState,
			expectedExecutorState: map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorFailedState},
			expectedAppMetrics: metrics{
				failedMetricCount: 1,
			},
			expectedExecutorMetrics: executorMetrics{
				failedMetricCount: 1,
			},
		},
		{
			appName:           appName,
			oldAppStatus:      v1beta2.RunningState,
			oldExecutorStatus: map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorRunningState},
			driverPod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      driverPodName,
					Namespace: "test",
					Labels: map[string]string{
						config.SparkRoleLabel:    config.SparkDriverRole,
						config.SparkAppNameLabel: appName,
					},
					ResourceVersion: "1",
				},
				Status: apiv1.PodStatus{
					Phase: apiv1.PodFailed,
					ContainerStatuses: []apiv1.ContainerStatus{
						{
							Name: config.SparkDriverContainerName,
							State: apiv1.ContainerState{
								Terminated: &apiv1.ContainerStateTerminated{
									ExitCode: 0,
								},
							},
						},
						{
							Name: "sidecar",
							State: apiv1.ContainerState{
								Terminated: &apiv1.ContainerStateTerminated{
									ExitCode: 137,
									Reason:   "OOMKilled",
								},
							},
						},
					},
				},
			},
			executorPod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "exec-1",
					Namespace: "test",
					Labels: map[string]string{
						config.SparkRoleLabel:    config.SparkExecutorRole,
						config.SparkAppNameLabel: appName,
					},
					ResourceVersion: "1",
				},
				Status: apiv1.PodStatus{
					Phase: apiv1.PodSucceeded,
				},
			},
			expectedAppState:      v1beta2.SucceedingState,
			expectedExecutorState: map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorCompletedState},
			expectedAppMetrics: metrics{
				successMetricCount: 1,
			},
			expectedExecutorMetrics: executorMetrics{
				successMetricCount: 1,
			},
		},
		{
			appName:                 appName,
			oldAppStatus:            v1beta2.FailingState,
			oldExecutorStatus:       map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorFailedState},
			expectedAppState:        v1beta2.FailedState,
			expectedExecutorState:   map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorFailedState},
			expectedAppMetrics:      metrics{},
			expectedExecutorMetrics: executorMetrics{},
		},
		{
			appName:           appName,
			oldAppStatus:      v1beta2.RunningState,
			oldExecutorStatus: map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorRunningState},
			driverPod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      driverPodName,
					Namespace: "test",
					Labels: map[string]string{
						config.SparkRoleLabel:    config.SparkDriverRole,
						config.SparkAppNameLabel: appName,
					},
					ResourceVersion: "1",
				},
				Status: apiv1.PodStatus{
					Phase: apiv1.PodSucceeded,
				},
			},
			executorPod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "exec-1",
					Namespace: "test",
					Labels: map[string]string{
						config.SparkRoleLabel:    config.SparkExecutorRole,
						config.SparkAppNameLabel: appName,
					},
					ResourceVersion: "1",
				},
				Status: apiv1.PodStatus{
					Phase: apiv1.PodSucceeded,
				},
			},
			expectedAppState:      v1beta2.SucceedingState,
			expectedExecutorState: map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorCompletedState},
			expectedAppMetrics: metrics{
				successMetricCount: 1,
			},
			expectedExecutorMetrics: executorMetrics{
				successMetricCount: 1,
			},
		},
		{
			appName:                 appName,
			oldAppStatus:            v1beta2.SucceedingState,
			oldExecutorStatus:       map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorCompletedState},
			expectedAppState:        v1beta2.CompletedState,
			expectedExecutorState:   map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorCompletedState},
			expectedAppMetrics:      metrics{},
			expectedExecutorMetrics: executorMetrics{},
		},
		{
			appName:           appName,
			oldAppStatus:      v1beta2.SubmittedState,
			oldExecutorStatus: map[string]v1beta2.ExecutorState{},
			driverPod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      driverPodName,
					Namespace: "test",
					Labels: map[string]string{
						config.SparkRoleLabel:    config.SparkDriverRole,
						config.SparkAppNameLabel: appName,
					},
				},
				Status: apiv1.PodStatus{
					Phase: apiv1.PodUnknown,
				},
			},
			executorPod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "exec-1",
					Namespace: "test",
					Labels: map[string]string{
						config.SparkRoleLabel:    config.SparkExecutorRole,
						config.SparkAppNameLabel: appName,
					},
				},
				Status: apiv1.PodStatus{
					Phase: apiv1.PodPending,
				},
			},
			expectedAppState:        v1beta2.UnknownState,
			expectedExecutorState:   map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorPendingState},
			expectedAppMetrics:      metrics{},
			expectedExecutorMetrics: executorMetrics{},
		},
		{
			appName:           appName,
			oldAppStatus:      v1beta2.CompletedState,
			oldExecutorStatus: map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorPendingState},
			driverPod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      driverPodName,
					Namespace: "test",
					Labels: map[string]string{
						config.SparkRoleLabel:    config.SparkDriverRole,
						config.SparkAppNameLabel: appName,
					},
					ResourceVersion: "1",
				},
				Status: apiv1.PodStatus{
					Phase: apiv1.PodSucceeded,
				},
			},
			expectedAppState:      v1beta2.CompletedState,
			expectedExecutorState: map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorCompletedState},
			expectedAppMetrics:    metrics{},
			expectedExecutorMetrics: executorMetrics{
				successMetricCount: 1,
			},
		},
		{
			appName:           appName,
			oldAppStatus:      v1beta2.RunningState,
			oldExecutorStatus: map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorRunningState},
			driverPod: &apiv1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      driverPodName,
					Namespace: "test",
					Labels: map[string]string{
						config.SparkRoleLabel:    config.SparkDriverRole,
						config.SparkAppNameLabel: appName,
					},
					ResourceVersion: "1",
				},
				Status: apiv1.PodStatus{
					Phase: apiv1.PodRunning,
				},
			},
			expectedAppState:        v1beta2.RunningState,
			expectedExecutorState:   map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorUnknownState},
			expectedAppMetrics:      metrics{},
			expectedExecutorMetrics: executorMetrics{},
		},
	}

	testFn := func(test testcase, t *testing.T) {
		app.Status.AppState.State = test.oldAppStatus
		app.Status.ExecutorState = test.oldExecutorStatus
		app.Name = test.appName
		app.Status.ExecutionAttempts = 1
		ctrl, _ := newFakeController(app, test.driverPod, test.executorPod)
		_, err := ctrl.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Create(context.TODO(), app, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if test.driverPod != nil {
			ctrl.kubeClient.CoreV1().Pods(app.Namespace).Create(context.TODO(), test.driverPod, metav1.CreateOptions{})
		}
		if test.executorPod != nil {
			ctrl.kubeClient.CoreV1().Pods(app.Namespace).Create(context.TODO(), test.executorPod, metav1.CreateOptions{})
		}

		err = ctrl.syncSparkApplication(fmt.Sprintf("%s/%s", app.Namespace, app.Name))
		assert.Nil(t, err)
		// Verify application and executor states.
		updatedApp, err := ctrl.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(context.TODO(), app.Name, metav1.GetOptions{})
		assert.Equal(t, test.expectedAppState, updatedApp.Status.AppState.State)
		assert.Equal(t, test.expectedExecutorState, updatedApp.Status.ExecutorState)

		// Validate error message if the driver pod failed.
		if test.driverPod != nil && test.driverPod.Status.Phase == apiv1.PodFailed {
			if len(test.driverPod.Status.ContainerStatuses) > 0 && test.driverPod.Status.ContainerStatuses[0].State.Terminated != nil {
				if test.driverPod.Status.ContainerStatuses[0].State.Terminated.ExitCode != 0 {
					assert.Equal(t, updatedApp.Status.AppState.ErrorMessage,
						fmt.Sprintf("driver container failed with ExitCode: %d, Reason: %s", test.driverPod.Status.ContainerStatuses[0].State.Terminated.ExitCode, test.driverPod.Status.ContainerStatuses[0].State.Terminated.Reason))
				}
			} else {
				assert.Equal(t, updatedApp.Status.AppState.ErrorMessage, "driver container status missing")
			}
		}

		// Verify application metrics.
		assert.Equal(t, test.expectedAppMetrics.runningMetricCount, ctrl.metrics.sparkAppRunningCount.Value(map[string]string{}))
		assert.Equal(t, test.expectedAppMetrics.successMetricCount, fetchCounterValue(ctrl.metrics.sparkAppSuccessCount, map[string]string{}))
		assert.Equal(t, test.expectedAppMetrics.submitMetricCount, fetchCounterValue(ctrl.metrics.sparkAppSubmitCount, map[string]string{}))
		assert.Equal(t, test.expectedAppMetrics.failedMetricCount, fetchCounterValue(ctrl.metrics.sparkAppFailureCount, map[string]string{}))

		// Verify executor metrics.
		assert.Equal(t, test.expectedExecutorMetrics.runningMetricCount, ctrl.metrics.sparkAppExecutorRunningCount.Value(map[string]string{}))
		assert.Equal(t, test.expectedExecutorMetrics.successMetricCount, fetchCounterValue(ctrl.metrics.sparkAppExecutorSuccessCount, map[string]string{}))
		assert.Equal(t, test.expectedExecutorMetrics.failedMetricCount, fetchCounterValue(ctrl.metrics.sparkAppExecutorFailureCount, map[string]string{}))
	}

	for _, test := range testcases {
		testFn(test, t)
	}
}

func TestSyncSparkApplication_ApplicationExpired(t *testing.T) {
	os.Setenv(kubernetesServiceHostEnvVar, "localhost")
	os.Setenv(kubernetesServicePortEnvVar, "443")

	appName := "foo"
	driverPodName := appName + "-driver"

	now := time.Now()
	terminationTime := now.Add(-2 * time.Second)
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      appName,
			Namespace: "test",
		},
		Spec: v1beta2.SparkApplicationSpec{
			RestartPolicy: v1beta2.RestartPolicy{
				Type: v1beta2.Never,
			},
			TimeToLiveSeconds: int64ptr(1),
		},
		Status: v1beta2.SparkApplicationStatus{
			AppState: v1beta2.ApplicationState{
				State:        v1beta2.CompletedState,
				ErrorMessage: "",
			},
			DriverInfo: v1beta2.DriverInfo{
				PodName: driverPodName,
			},
			TerminationTime: metav1.Time{
				Time: terminationTime,
			},
			ExecutorState: map[string]v1beta2.ExecutorState{"exec-1": v1beta2.ExecutorCompletedState},
		},
	}

	ctrl, _ := newFakeController(app)
	_, err := ctrl.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Create(context.TODO(), app, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	err = ctrl.syncSparkApplication(fmt.Sprintf("%s/%s", app.Namespace, app.Name))
	assert.Nil(t, err)

	_, err = ctrl.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(context.TODO(), app.Name, metav1.GetOptions{})
	assert.True(t, errors.IsNotFound(err))
}

func TestIsNextRetryDue(t *testing.T) {
	// Failure cases.
	assert.False(t, isNextRetryDue(nil, 3, metav1.Time{Time: metav1.Now().Add(-100 * time.Second)}))
	assert.False(t, isNextRetryDue(int64ptr(5), 0, metav1.Time{Time: metav1.Now().Add(-100 * time.Second)}))
	assert.False(t, isNextRetryDue(int64ptr(5), 3, metav1.Time{}))
	// Not enough time passed.
	assert.False(t, isNextRetryDue(int64ptr(50), 3, metav1.Time{Time: metav1.Now().Add(-100 * time.Second)}))
	assert.True(t, isNextRetryDue(int64ptr(50), 3, metav1.Time{Time: metav1.Now().Add(-151 * time.Second)}))
}

func TestIngressWithSubpathAffectsSparkConfiguration(t *testing.T) {
	os.Setenv(kubernetesServiceHostEnvVar, "localhost")
	os.Setenv(kubernetesServicePortEnvVar, "443")

	appName := "ingressaffectssparkconfig"

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      appName,
			Namespace: "test",
		},
		Spec: v1beta2.SparkApplicationSpec{
			RestartPolicy: v1beta2.RestartPolicy{
				Type: v1beta2.Never,
			},
			TimeToLiveSeconds: int64ptr(1),
		},
		Status: v1beta2.SparkApplicationStatus{},
	}

	ctrl, _ := newFakeController(app)
	ctrl.ingressURLFormat = "example.com/{{$appNamespace}}/{{$appName}}"
	ctrl.enableUIService = true
	_, err := ctrl.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Create(context.TODO(), app, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	err = ctrl.syncSparkApplication(fmt.Sprintf("%s/%s", app.Namespace, app.Name))
	assert.Nil(t, err)
	deployedApp, err := ctrl.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(context.TODO(), app.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ingresses, err := ctrl.kubeClient.NetworkingV1().Ingresses(app.Namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if ingresses == nil || ingresses.Items == nil || len(ingresses.Items) != 1 {
		t.Fatal("The ingress does not exist, has no items, or wrong amount of items")
	}
	if ingresses.Items[0].Spec.Rules[0].IngressRuleValue.HTTP.Paths[0].Path != "/"+app.Namespace+"/"+app.Name+"(/|$)(.*)" {
		t.Fatal("The ingress subpath was not created successfully.")
	}
	// The controller doesn't sync changes to the sparkConf performed by submitSparkApplication back to the kubernetes API server.
	if deployedApp.Spec.SparkConf["spark.ui.proxyBase"] != "/"+app.Namespace+"/"+app.Name {
		t.Log("The spark configuration does not reflect the subpath expected by the ingress")
	}
	if deployedApp.Spec.SparkConf["spark.ui.proxyRedirectUri"] != "/" {
		t.Log("The spark configuration does not reflect the proxyRedirectUri expected by the ingress")
	}
}

func TestIngressWithClassName(t *testing.T) {
	os.Setenv(kubernetesServiceHostEnvVar, "localhost")
	os.Setenv(kubernetesServicePortEnvVar, "443")

	appName := "ingressaffectssparkconfig"

	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      appName,
			Namespace: "test",
		},
		Spec: v1beta2.SparkApplicationSpec{
			RestartPolicy: v1beta2.RestartPolicy{
				Type: v1beta2.Never,
			},
			TimeToLiveSeconds: int64ptr(1),
		},
		Status: v1beta2.SparkApplicationStatus{},
	}

	ctrl, _ := newFakeController(app)
	ctrl.ingressURLFormat = "{{$appNamespace}}.{{$appName}}.example.com"
	ctrl.ingressClassName = "nginx"
	ctrl.enableUIService = true
	_, err := ctrl.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Create(context.TODO(), app, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	err = ctrl.syncSparkApplication(fmt.Sprintf("%s/%s", app.Namespace, app.Name))
	assert.Nil(t, err)
	_, err = ctrl.crdClient.SparkoperatorV1beta2().SparkApplications(app.Namespace).Get(context.TODO(), app.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ingresses, err := ctrl.kubeClient.NetworkingV1().Ingresses(app.Namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if ingresses == nil || ingresses.Items == nil || len(ingresses.Items) != 1 {
		t.Fatal("The ingress does not exist, has no items, or wrong amount of items")
	}
	if ingresses.Items[0].Spec.IngressClassName == nil || *ingresses.Items[0].Spec.IngressClassName != "nginx" {
		t.Fatal("The ingressClassName does not exists, or wrong value is set")
	}
}

func stringptr(s string) *string {
	return &s
}

func int32ptr(n int32) *int32 {
	return &n
}
