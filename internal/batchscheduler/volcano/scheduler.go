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

package volcano

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	volcanoclient "volcano.sh/apis/pkg/client/clientset/versioned"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/internal/batchscheduler"
	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
)

func init() {
	batchscheduler.NewSchedulerManager().RegisterScheduler(common.VolcanoSchedulerName, &Scheduler{})
}

// Scheduler is a batch scheduler that uses Volcano to schedule Spark applications.
type Scheduler struct {
	extensionClient apiextensionsclient.Interface
	volcanoClient   volcanoclient.Interface
}

// VolcanoScheduler implements batchscheduler.Interface.
var _ batchscheduler.Interface = &Scheduler{}

// Name implements batchscheduler.Interface.
func (s *Scheduler) Name() string {
	return common.VolcanoSchedulerName
}

// ShouldSchedule implements batchscheduler.Interface.
func (s *Scheduler) ShouldSchedule(app *v1beta2.SparkApplication) bool {
	// There is no additional requirement for volcano scheduler
	return true
}

// Schedule implements batchscheduler.Interface.
func (s *Scheduler) Schedule(app *v1beta2.SparkApplication) error {
	if app.Spec.Mode == v1beta2.DeployModeClient {
		return s.syncPodGroupInClientMode(app)
	} else if app.Spec.Mode == v1beta2.DeployModeCluster {
		return s.syncPodGroupInClusterMode(app)
	}
	return nil
}

// Cleanup implements batchscheduler.Interface.
func (s *Scheduler) Cleanup(app *v1beta2.SparkApplication) error {
	podGroupName := s.getPodGroupName(app)
	if err := s.volcanoClient.SchedulingV1beta1().PodGroups(app.Namespace).Delete(context.TODO(), podGroupName, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func NewVolcanoScheduler(config *rest.Config) (batchscheduler.Interface, error) {
	vkClient, err := volcanoclient.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize volcano client: %v", err)
	}
	extClient, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize k8s extension client: %v", err)
	}

	if _, err := extClient.ApiextensionsV1().CustomResourceDefinitions().Get(
		context.TODO(),
		common.VolcanoPodGroupName,
		metav1.GetOptions{},
	); err != nil {
		// For backward compatibility check v1beta1 API version of CustomResourceDefinitions
		if _, err := extClient.ApiextensionsV1beta1().CustomResourceDefinitions().Get(
			context.TODO(),
			common.VolcanoPodGroupName,
			metav1.GetOptions{},
		); err != nil {
			return nil, fmt.Errorf("podGroup CRD is required to exists in current cluster error: %s", err)
		}
	}
	return &Scheduler{
		extensionClient: extClient,
		volcanoClient:   vkClient,
	}, nil
}

func (s *Scheduler) syncPodGroupInClientMode(app *v1beta2.SparkApplication) error {
	// We only care about the executor pods in client mode
	if _, ok := app.Spec.Executor.Annotations[v1beta1.KubeGroupNameAnnotationKey]; !ok {
		totalResource := util.GetExecutorRequestResource(app)

		if app.Spec.BatchSchedulerOptions != nil && len(app.Spec.BatchSchedulerOptions.Resources) > 0 {
			totalResource = app.Spec.BatchSchedulerOptions.Resources
		}
		if err := s.syncPodGroup(app, 1, totalResource); err == nil {
			app.Spec.Executor.Annotations[v1beta1.KubeGroupNameAnnotationKey] = s.getPodGroupName(app)
		} else {
			return err
		}
	}
	return nil
}

func (s *Scheduler) syncPodGroupInClusterMode(app *v1beta2.SparkApplication) error {
	//We need both mark Driver and Executor when submitting
	//NOTE: In cluster mode, the initial size of PodGroup is set to 1 in order to schedule driver pod first.
	if _, ok := app.Spec.Driver.Annotations[v1beta1.KubeGroupNameAnnotationKey]; !ok {
		//Both driver and executor resource will be considered.
		totalResource := util.SumResourceList([]corev1.ResourceList{util.GetExecutorRequestResource(app), util.GetDriverRequestResource(app)})

		if app.Spec.BatchSchedulerOptions != nil && len(app.Spec.BatchSchedulerOptions.Resources) > 0 {
			totalResource = app.Spec.BatchSchedulerOptions.Resources
		}
		if err := s.syncPodGroup(app, 1, totalResource); err == nil {
			app.Spec.Executor.Annotations[v1beta1.KubeGroupNameAnnotationKey] = s.getPodGroupName(app)
			app.Spec.Driver.Annotations[v1beta1.KubeGroupNameAnnotationKey] = s.getPodGroupName(app)
		} else {
			return err
		}
	}
	return nil
}

func (s *Scheduler) getPodGroupName(app *v1beta2.SparkApplication) string {
	return fmt.Sprintf("spark-%s-pg", app.Name)
}

func (s *Scheduler) syncPodGroup(app *v1beta2.SparkApplication, size int32, minResource corev1.ResourceList) error {
	var (
		err error
		pg  *v1beta1.PodGroup
	)
	podGroupName := s.getPodGroupName(app)
	if pg, err = s.volcanoClient.SchedulingV1beta1().PodGroups(app.Namespace).Get(context.TODO(), podGroupName, metav1.GetOptions{}); err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		podGroup := v1beta1.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: app.Namespace,
				Name:      podGroupName,
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(app, v1beta2.SchemeGroupVersion.WithKind("SparkApplication")),
				},
			},
			Spec: v1beta1.PodGroupSpec{
				MinMember:    size,
				MinResources: &minResource,
			},
			Status: v1beta1.PodGroupStatus{
				Phase: v1beta1.PodGroupPending,
			},
		}

		if app.Spec.BatchSchedulerOptions != nil {
			//Update pod group queue if it's specified in Spark Application
			if app.Spec.BatchSchedulerOptions.Queue != nil {
				podGroup.Spec.Queue = *app.Spec.BatchSchedulerOptions.Queue
			}
			//Update pod group priorityClassName if it's specified in Spark Application
			if app.Spec.BatchSchedulerOptions.PriorityClassName != nil {
				podGroup.Spec.PriorityClassName = *app.Spec.BatchSchedulerOptions.PriorityClassName
			}
		}
		_, err = s.volcanoClient.SchedulingV1beta1().PodGroups(app.Namespace).Create(context.TODO(), &podGroup, metav1.CreateOptions{})
	} else {
		if pg.Spec.MinMember != size {
			pg.Spec.MinMember = size
			_, err = s.volcanoClient.SchedulingV1beta1().PodGroups(app.Namespace).Update(context.TODO(), pg, metav1.UpdateOptions{})
		}
	}
	if err != nil {
		return fmt.Errorf("failed to sync PodGroup with error: %s. Abandon schedule pods via volcano", err)
	}
	return nil
}
