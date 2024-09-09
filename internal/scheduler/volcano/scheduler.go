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
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	volcanoclientset "volcano.sh/apis/pkg/client/clientset/versioned"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/internal/scheduler"
	"github.com/kubeflow/spark-operator/pkg/common"
	"github.com/kubeflow/spark-operator/pkg/util"
)

var (
	logger = log.Log.WithName("")
)

// Scheduler is a batch scheduler that uses Volcano to schedule Spark applications.
type Scheduler struct {
	extensionClient apiextensionsclientset.Interface
	volcanoClient   volcanoclientset.Interface
}

// Scheduler implements scheduler.Interface.
var _ scheduler.Interface = &Scheduler{}

// Config defines the configurations of Volcano scheduler.
type Config struct {
	RestConfig *rest.Config
}

// Config implements scheduler.Config.
var _ scheduler.Config = &Config{}

// Factory creates a new VolcanoScheduler instance.
func Factory(config scheduler.Config) (scheduler.Interface, error) {
	c, ok := config.(*Config)
	if !ok {
		return nil, fmt.Errorf("failed to get volcano scheduler config")
	}

	extensionClient, err := apiextensionsclientset.NewForConfig(c.RestConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize k8s extension client: %v", err)
	}

	if _, err := extensionClient.ApiextensionsV1().CustomResourceDefinitions().Get(
		context.TODO(),
		common.VolcanoPodGroupName,
		metav1.GetOptions{},
	); err != nil {
		// For backward compatibility check v1beta1 API version of CustomResourceDefinitions
		if _, err := extensionClient.ApiextensionsV1beta1().CustomResourceDefinitions().Get(
			context.TODO(),
			common.VolcanoPodGroupName,
			metav1.GetOptions{},
		); err != nil {
			return nil, fmt.Errorf("CRD PodGroup does not exist: %v", err)
		}
	}

	volcanoClient, err := volcanoclientset.NewForConfig(c.RestConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize volcano client: %v", err)
	}

	scheduler := &Scheduler{
		extensionClient: extensionClient,
		volcanoClient:   volcanoClient,
	}
	return scheduler, nil
}

// Name implements batchscheduler.Interface.
func (s *Scheduler) Name() string {
	return common.VolcanoSchedulerName
}

// ShouldSchedule implements batchscheduler.Interface.
func (s *Scheduler) ShouldSchedule(_ *v1beta2.SparkApplication) bool {
	// There is no additional requirement for volcano scheduler
	return true
}

// Schedule implements batchscheduler.Interface.
func (s *Scheduler) Schedule(app *v1beta2.SparkApplication) error {
	if app.ObjectMeta.Annotations == nil {
		app.ObjectMeta.Annotations = make(map[string]string)
	}
	if app.Spec.Driver.Annotations == nil {
		app.Spec.Driver.Annotations = make(map[string]string)
	}
	if app.Spec.Executor.Annotations == nil {
		app.Spec.Executor.Annotations = make(map[string]string)
	}

	switch app.Spec.Mode {
	case v1beta2.DeployModeClient:
		return s.syncPodGroupInClientMode(app)
	case v1beta2.DeployModeCluster:
		return s.syncPodGroupInClusterMode(app)
	}
	return nil
}

// Cleanup implements batchscheduler.Interface.
func (s *Scheduler) Cleanup(app *v1beta2.SparkApplication) error {
	name := getPodGroupName(app)
	namespace := app.Namespace
	if err := s.volcanoClient.SchedulingV1beta1().PodGroups(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
		return err
	}
	logger.Info("Deleted PodGroup", "name", name, "namespace", namespace)
	return nil
}

func (s *Scheduler) syncPodGroupInClientMode(app *v1beta2.SparkApplication) error {
	// We only care about the executor pods in client mode
	if _, ok := app.Spec.Executor.Annotations[v1beta1.KubeGroupNameAnnotationKey]; !ok {
		totalResource := util.GetExecutorRequestResource(app)

		if app.Spec.BatchSchedulerOptions != nil && len(app.Spec.BatchSchedulerOptions.Resources) > 0 {
			totalResource = app.Spec.BatchSchedulerOptions.Resources
		}
		if err := s.syncPodGroup(app, 1, totalResource); err == nil {
			app.Spec.Executor.Annotations[v1beta1.KubeGroupNameAnnotationKey] = getPodGroupName(app)
		} else {
			return err
		}
	}
	return nil
}

func (s *Scheduler) syncPodGroupInClusterMode(app *v1beta2.SparkApplication) error {
	// We need mark both driver and executor when submitting.
	// In cluster mode, the initial size of PodGroup is set to 1 in order to schedule driver pod first.
	if _, ok := app.Spec.Driver.Annotations[v1beta1.KubeGroupNameAnnotationKey]; !ok {
		// Both driver and executor resource will be considered.
		totalResource := util.SumResourceList([]corev1.ResourceList{util.GetDriverRequestResource(app), util.GetExecutorRequestResource(app)})
		if app.Spec.BatchSchedulerOptions != nil && len(app.Spec.BatchSchedulerOptions.Resources) > 0 {
			totalResource = app.Spec.BatchSchedulerOptions.Resources
		}

		if err := s.syncPodGroup(app, 1, totalResource); err != nil {
			return err
		}
		app.Spec.Driver.Annotations[v1beta1.KubeGroupNameAnnotationKey] = getPodGroupName(app)
		app.Spec.Executor.Annotations[v1beta1.KubeGroupNameAnnotationKey] = getPodGroupName(app)
	}
	return nil
}

func (s *Scheduler) syncPodGroup(app *v1beta2.SparkApplication, size int32, minResource corev1.ResourceList) error {
	var err error
	var pg *v1beta1.PodGroup
	name := getPodGroupName(app)
	namespace := app.Namespace

	if pg, err = s.volcanoClient.SchedulingV1beta1().PodGroups(namespace).Get(context.TODO(), name, metav1.GetOptions{}); err != nil {
		if !errors.IsNotFound(err) {
			return err
		}

		podGroup := v1beta1.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
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
			// Update pod group queue if it's specified in Spark Application
			if app.Spec.BatchSchedulerOptions.Queue != nil {
				podGroup.Spec.Queue = *app.Spec.BatchSchedulerOptions.Queue
			}
			// Update pod group priorityClassName if it's specified in Spark Application
			if app.Spec.BatchSchedulerOptions.PriorityClassName != nil {
				podGroup.Spec.PriorityClassName = *app.Spec.BatchSchedulerOptions.PriorityClassName
			}
		}
		_, err = s.volcanoClient.SchedulingV1beta1().PodGroups(namespace).Create(context.TODO(), &podGroup, metav1.CreateOptions{})
	} else {
		if pg.Spec.MinMember != size {
			pg.Spec.MinMember = size
			_, err = s.volcanoClient.SchedulingV1beta1().PodGroups(namespace).Update(context.TODO(), pg, metav1.UpdateOptions{})
		}
	}

	if err != nil {
		return fmt.Errorf("failed to sync PodGroup with error: %s. Abandon schedule pods via volcano", err)
	}
	logger.Info("Created PodGroup", "name", name, "namespace", namespace)

	return nil
}
