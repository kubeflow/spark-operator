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
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"

	"k8s.io/apimachinery/pkg/api/resource"

	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	batchv1listers "k8s.io/client-go/listers/batch/v1"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
)

const (
	sparkSubmitPodMemoryRequest = "100Mi"
	sparkSubmitPodCpuRequest    = "100m"
	sparkSubmitPodMemoryLimit   = "512Mi"
	sparkSubmitPodCpuLimit      = "250m"
)

type submissionJobManager interface {
	createSubmissionJob(app *v1beta2.SparkApplication) (string, string, error)
	deleteSubmissionJob(app *v1beta2.SparkApplication) error
	getSubmissionJob(app *v1beta2.SparkApplication) (*batchv1.Job, error)
	hasJobSucceeded(app *v1beta2.SparkApplication) (*bool, *metav1.Time, error)
}

type realSubmissionJobManager struct {
	kubeClient kubernetes.Interface
	jobLister  batchv1listers.JobLister
}

func (sjm *realSubmissionJobManager) createSubmissionJob(app *v1beta2.SparkApplication) (string, string, error) {
	var image string
	if app.Spec.Image != nil {
		image = *app.Spec.Image
	} else if app.Spec.Driver.Image != nil {
		image = *app.Spec.Driver.Image
	}
	if image == "" {
		return "", "", fmt.Errorf("no image specified in .spec.image or .spec.driver.image in SparkApplication %s/%s",
			app.Namespace, app.Name)
	}

	driverPodName := getDriverPodName(app)
	submissionID := uuid.New().String()
	submissionCmdArgs, err := buildSubmissionCommandArgs(app, driverPodName, submissionID)
	if err != nil {
		return "", "", err
	}

	command := []string{"sh", "-c", fmt.Sprintf("$SPARK_HOME/bin/spark-submit %s", strings.Join(submissionCmdArgs, " "))}
	var one int32 = 1

	imagePullSecrets := make([]v1.LocalObjectReference, len(app.Spec.ImagePullSecrets))
	for i, secret := range app.Spec.ImagePullSecrets {
		imagePullSecrets[i] = v1.LocalObjectReference{secret}
	}
	imagePullPolicy := v1.PullIfNotPresent
	if app.Spec.ImagePullPolicy != nil {
		imagePullPolicy = v1.PullPolicy(*app.Spec.ImagePullPolicy)
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getSubmissionJobName(app),
			Namespace: app.Namespace,
			Labels: map[string]string{
				config.SparkAppNameLabel:            app.Name,
				config.LaunchedBySparkOperatorLabel: "true",
			},
			Annotations:     app.Annotations,
			OwnerReferences: []metav1.OwnerReference{*getOwnerReference(app)},
		},
		Spec: batchv1.JobSpec{
			Parallelism:  &one,
			Completions:  &one,
			BackoffLimit: app.Spec.RestartPolicy.OnSubmissionFailureRetries,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					ImagePullSecrets: imagePullSecrets,
					Containers: []corev1.Container{
						{
							Name:            "spark-submit-runner",
							Image:           image,
							Command:         command,
							ImagePullPolicy: imagePullPolicy,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse(sparkSubmitPodCpuRequest),
									corev1.ResourceMemory: resource.MustParse(sparkSubmitPodMemoryRequest),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse(sparkSubmitPodCpuLimit),
									corev1.ResourceMemory: resource.MustParse(sparkSubmitPodMemoryLimit),
								},
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyNever,
				},
			},
		},
	}
	if app.Spec.ServiceAccount != nil {
		job.Spec.Template.Spec.ServiceAccountName = *app.Spec.ServiceAccount
	}
	// Copy the labels on the SparkApplication to the Job.
	for key, val := range app.Labels {
		job.Labels[key] = val
	}
	_, err = sjm.kubeClient.BatchV1().Jobs(app.Namespace).Create(job)
	if err != nil {
		return "", "", err
	}
	return submissionID, driverPodName, nil
}

func (sjm *realSubmissionJobManager) getSubmissionJob(app *v1beta2.SparkApplication) (*batchv1.Job, error) {
	return sjm.jobLister.Jobs(app.Namespace).Get(getSubmissionJobName(app))
}

func (sjm *realSubmissionJobManager) deleteSubmissionJob(app *v1beta2.SparkApplication) error {
	deleteOptions := metav1.NewDeleteOptions(0)
	background := metav1.DeletePropagationBackground
	deleteOptions.PropagationPolicy = &background
	return sjm.kubeClient.BatchV1().Jobs(app.Namespace).Delete(getSubmissionJobName(app), deleteOptions)
}

// hasJobSucceeded returns a boolean that indicates if the job has succeeded or not if the job has terminated.
// Otherwise, it returns a nil to indicate that the job has not terminated yet.
//  An error is returned if the the job failed or if there was an issue querying the job.
func (sjm *realSubmissionJobManager) hasJobSucceeded(app *v1beta2.SparkApplication) (*bool, *metav1.Time, error) {
	job, err := sjm.getSubmissionJob(app)
	if err != nil {
		return nil, nil, err
	}
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobComplete && cond.Status == v1.ConditionTrue {
			return boolptr(true), job.Status.CompletionTime, nil
		}
		if cond.Type == batchv1.JobFailed && cond.Status == v1.ConditionTrue {
			return boolptr(false), nil,
				errors.New(fmt.Sprintf("Submission Job Failed. Error: %s. %s", cond.Reason, cond.Message))
		}
	}
	return nil, nil, nil
}

func boolptr(v bool) *bool {
	return &v
}
