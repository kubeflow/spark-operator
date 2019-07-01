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
	"k8s.io/apimachinery/pkg/api/resource"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	batchv1listers "k8s.io/client-go/listers/batch/v1"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta1"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
)

const (
	sparkSubmitPodMemory = "1000Mi"
	sparkSubmitPodCpu    = "1024m"
)

type submissionJobManager struct {
	kubeClient kubernetes.Interface
	jobLister  batchv1listers.JobLister
}

func (sjm *submissionJobManager) createSubmissionJob(s *submission) (*batchv1.Job, error) {
	var image string
	if s.app.Spec.Image != nil {
		image = *s.app.Spec.Image
	} else if s.app.Spec.Driver.Image != nil {
		image = *s.app.Spec.Driver.Image
	}
	if image == "" {
		return nil, fmt.Errorf("no image specified in .spec.image or .spec.driver.image in SparkApplication %s/%s",
			s.app.Namespace, s.app.Name)
	}

	command := []string{"sh", "-c", fmt.Sprintf("$SPARK_HOME/bin/spark-submit %s", strings.Join(s.args, " "))}
	var one int32 = 1
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getSubmissionJobName(s.app),
			Namespace: s.app.Namespace,
			Labels: map[string]string{
				config.SparkAppNameLabel:            s.app.Name,
				config.LaunchedBySparkOperatorLabel: "true",
			},
			Annotations:     s.app.Annotations,
			OwnerReferences: []metav1.OwnerReference{*getOwnerReference(s.app)},
		},
		Spec: batchv1.JobSpec{
			Parallelism:  &one,
			Completions:  &one,
			BackoffLimit: s.app.Spec.RestartPolicy.OnSubmissionFailureRetries,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    "spark-submit-runner",
							Image:   image,
							Command: command,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse(sparkSubmitPodCpu),
									corev1.ResourceMemory: resource.MustParse(sparkSubmitPodMemory),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse(sparkSubmitPodCpu),
									corev1.ResourceMemory: resource.MustParse(sparkSubmitPodMemory),
								},
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyNever,
				},
			},
		},
	}
	if s.app.Spec.ServiceAccount != nil {
		job.Spec.Template.Spec.ServiceAccountName = *s.app.Spec.ServiceAccount
	}
	// Copy the labels on the SparkApplication to the Job.
	for key, val := range s.app.Labels {
		job.Labels[key] = val
	}
	return sjm.kubeClient.BatchV1().Jobs(s.app.Namespace).Create(job)
}

func (sjm *submissionJobManager) getSubmissionJob(app *v1beta1.SparkApplication) (*batchv1.Job, error) {
	return sjm.jobLister.Jobs(app.Namespace).Get(getSubmissionJobName(app))
}

func (sjm *submissionJobManager) deleteSubmissionJob(app *v1beta1.SparkApplication) error {
	return sjm.kubeClient.BatchV1().Jobs(app.Namespace).Delete(getSubmissionJobName(app), metav1.NewDeleteOptions(0))
}

// hasJobSucceeded returns a boolean that indicates if the job has succeeded or not if the job has terminated.
// Otherwise, it returns a nil to indicate that the job has not terminated yet.
func (sjm *submissionJobManager) hasJobSucceeded(app *v1beta1.SparkApplication) (*bool, *metav1.Time, error) {
	job, err := sjm.getSubmissionJob(app)
	if err != nil {
		return nil, nil, err
	}
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobComplete {
			return boolptr(true), job.Status.CompletionTime, nil
		}
		if cond.Type == batchv1.JobFailed {
			return boolptr(false), nil, nil
		}
	}
	return nil, nil, nil
}

func boolptr(v bool) *bool {
	return &v
}
