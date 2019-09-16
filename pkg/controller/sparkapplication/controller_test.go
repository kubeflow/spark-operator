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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	prometheus_model "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta1"
)

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

func TestShouldRetry(t *testing.T) {
	type testcase struct {
		app         *v1beta1.SparkApplication
		shouldRetry bool
	}

	testFn := func(test testcase, t *testing.T) {
		shouldRetry := shouldRetry(test.app)
		assert.Equal(t, test.shouldRetry, shouldRetry)
	}

	restartPolicyAlways := v1beta1.RestartPolicy{
		Type:                             v1beta1.Always,
		OnSubmissionFailureRetryInterval: int64ptr(100),
		OnFailureRetryInterval:           int64ptr(100),
	}

	restartPolicyNever := v1beta1.RestartPolicy{
		Type: v1beta1.Never,
	}

	restartPolicyOnFailure := v1beta1.RestartPolicy{
		Type:                             v1beta1.OnFailure,
		OnFailureRetries:                 int32ptr(1),
		OnFailureRetryInterval:           int64ptr(100),
		OnSubmissionFailureRetryInterval: int64ptr(100),
		OnSubmissionFailureRetries:       int32ptr(2),
	}

	testcases := []testcase{
		{
			app: &v1beta1.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				}},
			shouldRetry: false,
		},
		{
			app: &v1beta1.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Spec: v1beta1.SparkApplicationSpec{
					RestartPolicy: restartPolicyAlways,
				},
				Status: v1beta1.SparkApplicationStatus{
					AppState: v1beta1.ApplicationState{
						State: v1beta1.SucceedingState,
					},
				},
			},
			shouldRetry: true,
		},
		{
			app: &v1beta1.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Spec: v1beta1.SparkApplicationSpec{
					RestartPolicy: restartPolicyOnFailure,
				},
				Status: v1beta1.SparkApplicationStatus{
					AppState: v1beta1.ApplicationState{
						State: v1beta1.SucceedingState,
					},
				},
			},
			shouldRetry: false,
		},
		{
			app: &v1beta1.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Spec: v1beta1.SparkApplicationSpec{
					RestartPolicy: restartPolicyOnFailure,
				},
				Status: v1beta1.SparkApplicationStatus{
					AppState: v1beta1.ApplicationState{
						State: v1beta1.FailingState,
					},
				},
			},
			shouldRetry: true,
		},
		{
			app: &v1beta1.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Spec: v1beta1.SparkApplicationSpec{
					RestartPolicy: restartPolicyNever,
				},
				Status: v1beta1.SparkApplicationStatus{
					AppState: v1beta1.ApplicationState{
						State: v1beta1.FailingState,
					},
				},
			},
			shouldRetry: false,
		},
		{
			app: &v1beta1.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Spec: v1beta1.SparkApplicationSpec{
					RestartPolicy: restartPolicyNever,
				},
				Status: v1beta1.SparkApplicationStatus{
					AppState: v1beta1.ApplicationState{
						State: v1beta1.FailedSubmissionState,
					},
				},
			},
			shouldRetry: false,
		},
		{
			app: &v1beta1.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Spec: v1beta1.SparkApplicationSpec{
					RestartPolicy: restartPolicyOnFailure,
				},
				Status: v1beta1.SparkApplicationStatus{
					AppState: v1beta1.ApplicationState{
						State: v1beta1.FailedSubmissionState,
					},
				},
			},
			shouldRetry: true,
		},
		{
			app: &v1beta1.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "default",
				},
				Spec: v1beta1.SparkApplicationSpec{
					RestartPolicy: restartPolicyAlways,
				},
				Status: v1beta1.SparkApplicationStatus{
					AppState: v1beta1.ApplicationState{
						State: v1beta1.PendingRerunState,
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

func TestHasRetryIntervalPassed(t *testing.T) {
	// Failure cases.
	assert.False(t, hasRetryIntervalPassed(nil, 3, metav1.Time{Time: metav1.Now().Add(-100 * time.Second)}))
	assert.False(t, hasRetryIntervalPassed(int64ptr(5), 0, metav1.Time{Time: metav1.Now().Add(-100 * time.Second)}))
	assert.False(t, hasRetryIntervalPassed(int64ptr(5), 3, metav1.Time{}))
	// Not enough time passed.
	assert.False(t, hasRetryIntervalPassed(int64ptr(50), 3, metav1.Time{Time: metav1.Now().Add(-100 * time.Second)}))
	assert.True(t, hasRetryIntervalPassed(int64ptr(50), 3, metav1.Time{Time: metav1.Now().Add(-151 * time.Second)}))
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
