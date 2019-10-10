/*
Copyright 2019 Google LLC

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
	"os"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/stretchr/testify/assert"
)

func newFakeJobManager(ctx context.Context, jobs ...*batchv1.Job) (submissionJobManager, error) {

	s := scheme.Scheme
	app := v1beta2.SparkApplication{}
	s.AddKnownTypes(v1beta2.SchemeGroupVersion, &app)
	fakeClient := fake.NewFakeClientWithScheme(s)

	for _, job := range jobs {
		if job != nil {
			err := fakeClient.Create(ctx, job)
			if err != nil {
				return nil, err
			}
		}
	}
	return &realSubmissionJobManager{
		client: fakeClient,
	}, nil
}

func TestGetSubmissionJob(t *testing.T) {
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Status: v1beta2.SparkApplicationStatus{},
	}
	ctx := context.Background()
	// Case 1: Job doesn't exist.
	jobManager, err := newFakeJobManager(ctx, nil)

	if err != nil {
		t.Errorf("Could not create fake job manager")
	}

	jobResult, err := jobManager.getSubmissionJob(ctx, app)

	assert.NotNil(t, err)
	assert.True(t, errors.IsNotFound(err))
	assert.Nil(t, jobResult)

	// Case 2: Job exists.
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo-spark-submit",
			Namespace: "default",
		},
	}
	jobManager, err = newFakeJobManager(ctx, job)

	jobResult, err = jobManager.getSubmissionJob(ctx, app)
	assert.Nil(t, err)
	assert.NotNil(t, jobResult)
	assert.Equal(t, job, jobResult)
}

func TestDeleteSubmissionJob(t *testing.T) {
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Status: v1beta2.SparkApplicationStatus{},
	}
	ctx := context.Background()
	// Case 1: Job doesn't exist.
	jobManager, err := newFakeJobManager(ctx, nil)
	if err != nil {
		t.Errorf("Could not create fake job manager")
	}

	err = jobManager.deleteSubmissionJob(ctx, app)
	assert.NotNil(t, err)
	assert.True(t, errors.IsNotFound(err))

	// Case 2: Job exists
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo-spark-submit",
			Namespace: "default",
		},
	}
	jobManager, err = newFakeJobManager(ctx, job)
	if err != nil {
		t.Errorf("Could not create fake job manager")
	}

	err = jobManager.deleteSubmissionJob(ctx, app)
	assert.Nil(t, err)
}

func TestCreateSubmissionJob(t *testing.T) {
	os.Setenv(kubernetesServiceHostEnvVar, "localhost")
	os.Setenv(kubernetesServicePortEnvVar, "443")
	ctx := context.Background()
	// Case 1: Image doesn't exist.
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Status: v1beta2.SparkApplicationStatus{},
	}

	jobManager, err := newFakeJobManager(ctx, nil)
	if err != nil {
		t.Errorf("Could not create fake job manager")
	}

	submissionID, driverPodName, err := jobManager.createSubmissionJob(ctx, app)
	assert.NotNil(t, err)
	assert.Empty(t, submissionID)
	assert.Empty(t, driverPodName)

	// Case 2:  Job creation successful.
	app = &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Spec: v1beta2.SparkApplicationSpec{
			Image: stringptr("spark-base-image"),
		},
		Status: v1beta2.SparkApplicationStatus{},
	}
	jobManager, err = newFakeJobManager(ctx, nil)
	submissionID, driverPodName, err = jobManager.createSubmissionJob(ctx, app)
	assert.Nil(t, err)
	assert.NotNil(t, submissionID)
	assert.NotNil(t, driverPodName)

}

func TestHasJobSucceeded(t *testing.T) {
	app := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Status: v1beta2.SparkApplicationStatus{},
	}
	ctx := context.Background()
	// Case 1: Job doesn't exist.
	jobManager, err := newFakeJobManager(ctx, nil)
	if err != nil {
		t.Errorf("Could not create fake job manager")
	}
	result, successTime, err := jobManager.hasJobSucceeded(ctx, app)
	assert.NotNil(t, err)
	assert.True(t, errors.IsNotFound(err))
	assert.Nil(t, result)
	assert.Nil(t, successTime)

	// Case 2: Job exists but not completed.
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo-spark-submit",
			Namespace: "default",
		},
	}
	jobManager, err = newFakeJobManager(ctx, job)
	if err != nil {
		t.Errorf("Could not create fake job manager")
	}
	result, successTime, err = jobManager.hasJobSucceeded(ctx, app)
	assert.Nil(t, err)
	assert.Nil(t, result)
	assert.Nil(t, successTime)

	// Case 3: Job failed.
	job = &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo-spark-submit",
			Namespace: "default",
		},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{
				Type:    batchv1.JobFailed,
				Status:  v1.ConditionTrue,
				Reason:  "pod failed",
				Message: "Pod foo-spark-submit-1 failed",
			}},
		},
	}
	jobManager, err = newFakeJobManager(ctx, job)
	if err != nil {
		t.Errorf("Could not create fake job manager")
	}
	result, successTime, err = jobManager.hasJobSucceeded(ctx, app)
	assert.Equal(t, err.Error(), "Submission Job Failed. Error: pod failed. Pod foo-spark-submit-1 failed")
	assert.False(t, *result)
	assert.Nil(t, successTime)

	// Case 4: Job succeeded.
	job = &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo-spark-submit",
			Namespace: "default",
		},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{
				Type:   batchv1.JobComplete,
				Status: v1.ConditionTrue,
			}},
			CompletionTime: &metav1.Time{Time: time.Now()},
		},
	}
	jobManager, err = newFakeJobManager(ctx, job)
	if err != nil {
		t.Errorf("Could not create fake job manager")
	}
	result, successTime, err = jobManager.hasJobSucceeded(ctx, app)
	assert.Nil(t, err)
	assert.True(t, *result)
	assert.NotNil(t, successTime)
}
