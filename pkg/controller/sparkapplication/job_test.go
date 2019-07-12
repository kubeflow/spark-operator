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
	"k8s.io/api/core/v1"
	"os"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"

	batchv1 "k8s.io/api/batch/v1"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	kubeclientfake "k8s.io/client-go/kubernetes/fake"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta1"
)

func newFakeJobManager(jobs ...*batchv1.Job) submissionJobManagerIface {
	kubeClient := kubeclientfake.NewSimpleClientset()

	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0*time.Second)
	lister := informerFactory.Batch().V1().Jobs().Lister()
	informer := informerFactory.Batch().V1().Jobs().Informer()
	for _, job := range jobs {
		if job != nil {
			informer.GetIndexer().Add(job)
			kubeClient.BatchV1().Jobs(job.GetNamespace()).Create(job)
		}
	}
	return &submissionJobManager{
		jobLister:  lister,
		kubeClient: kubeClient,
	}
}

func TestGetSubmissionJob(t *testing.T) {

	app := &v1beta1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Status: v1beta1.SparkApplicationStatus{},
	}

	// Case 1: Job doesn't exist.
	jobManager := newFakeJobManager(nil)
	jobResult, err := jobManager.getSubmissionJob(app)
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
	jobManager = newFakeJobManager(job)

	jobResult, err = jobManager.getSubmissionJob(app)
	assert.Nil(t, err)
	assert.NotNil(t, jobResult)
	assert.Equal(t, job, jobResult)
}

func TestDeleteSubmissionJob(t *testing.T) {
	app := &v1beta1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Status: v1beta1.SparkApplicationStatus{},
	}

	// Case 1: Job doesn't exist.
	jobManager := newFakeJobManager(nil)
	err := jobManager.deleteSubmissionJob(app)
	assert.NotNil(t, err)
	assert.True(t, errors.IsNotFound(err))

	// Case 2: Job exists
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo-spark-submit",
			Namespace: "default",
		},
	}
	jobManager = newFakeJobManager(job)
	err = jobManager.deleteSubmissionJob(app)
	assert.Nil(t, err)
}

func TestCreateSubmissionJob(t *testing.T) {
	os.Setenv(kubernetesServiceHostEnvVar, "localhost")
	os.Setenv(kubernetesServicePortEnvVar, "443")

	// Case 1: Image doesn't exist.
	app := &v1beta1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Status: v1beta1.SparkApplicationStatus{},
	}

	jobManager := newFakeJobManager(nil)
	submissionID, driverPodName, err := jobManager.createSubmissionJob(app)
	assert.NotNil(t, err)
	assert.Nil(t, submissionID)
	assert.Nil(t, driverPodName)

	// Case 2:  Job creation successful.
	app = &v1beta1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Spec: v1beta1.SparkApplicationSpec{
			Image: stringptr("spark-base-image"),
		},
		Status: v1beta1.SparkApplicationStatus{},
	}
	jobManager = newFakeJobManager(nil)
	submissionID, driverPodName, err = jobManager.createSubmissionJob(app)
	assert.Nil(t, err)
	assert.NotNil(t, submissionID)
	assert.NotNil(t, driverPodName)

}

func TestHasJobSucceeded(t *testing.T) {
	app := &v1beta1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
		},
		Status: v1beta1.SparkApplicationStatus{},
	}

	// Case 1: Job doesn't exist.
	jobManager := newFakeJobManager(nil)
	result, successTime, err := jobManager.hasJobSucceeded(app)
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
	jobManager = newFakeJobManager(job)
	result, successTime, err = jobManager.hasJobSucceeded(app)
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
	jobManager = newFakeJobManager(job)
	result, successTime, err = jobManager.hasJobSucceeded(app)
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
	jobManager = newFakeJobManager(job)
	result, successTime, err = jobManager.hasJobSucceeded(app)
	assert.Nil(t, err)
	assert.True(t, *result)
	assert.NotNil(t, successTime)
}
