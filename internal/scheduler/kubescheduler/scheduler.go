/*
Copyright 2024 The Kubeflow authors.

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

package kubescheduler

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"github.com/kubeflow/spark-operator/api/v1beta2"
	"github.com/kubeflow/spark-operator/internal/scheduler"
	"github.com/kubeflow/spark-operator/pkg/util"
)

const (
	Name = "kube-scheduler"
)

var (
	logger = log.Log.WithName("")
)

// Scheduler is a scheduler that uses scheduler plugins to schedule Spark pods.
// Ref: https://github.com/kubernetes-sigs/scheduler-plugins.
type Scheduler struct {
	name   string
	client client.Client
}

// Scheduler implements scheduler.Interface.
var _ scheduler.Interface = &Scheduler{}

// Config defines the configurations of kube-scheduler.
type Config struct {
	SchedulerName string
	Client        client.Client
}

// Config implements scheduler.Config.
var _ scheduler.Config = &Config{}

// Factory creates a new Scheduler instance.
func Factory(config scheduler.Config) (scheduler.Interface, error) {
	c, ok := config.(*Config)
	if !ok {
		return nil, fmt.Errorf("failed to get kube-scheduler config")
	}

	scheduler := &Scheduler{
		name:   c.SchedulerName,
		client: c.Client,
	}
	return scheduler, nil
}

// Name implements scheduler.Interface.
func (s *Scheduler) Name() string {
	return s.name
}

// ShouldSchedule implements scheduler.Interface.
func (s *Scheduler) ShouldSchedule(app *v1beta2.SparkApplication) bool {
	// There is no additional requirements for scheduling.
	return true
}

// Schedule implements scheduler.Interface.
func (s *Scheduler) Schedule(app *v1beta2.SparkApplication) error {
	minResources := util.SumResourceList([]corev1.ResourceList{util.GetDriverRequestResource(app), util.GetExecutorRequestResource(app)})
	podGroup := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getPodGroupName(app),
			Namespace: app.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(app, v1beta2.SchemeGroupVersion.WithKind("SparkApplication")),
			},
		},
		Spec: schedulingv1alpha1.PodGroupSpec{
			MinMember:    1,
			MinResources: minResources,
		},
	}

	if err := s.syncPodGroup(podGroup); err != nil {
		return fmt.Errorf("failed to sync pod group: %v", err)
	}

	// Add a label `scheduling.x-k8s.io/pod-group` to mark the pod belongs to a group
	if app.ObjectMeta.Labels == nil {
		app.ObjectMeta.Labels = make(map[string]string)
	}
	app.ObjectMeta.Labels[schedulingv1alpha1.PodGroupLabel] = podGroup.Name

	return nil
}

// Cleanup implements scheduler.Interface.
func (s *Scheduler) Cleanup(app *v1beta2.SparkApplication) error {
	podGroup := &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getPodGroupName(app),
			Namespace: app.Namespace,
		},
	}
	if err := s.client.Delete(context.TODO(), podGroup); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	logger.Info("Deleted PodGroup", "Name", podGroup.Name, "Namespace", podGroup.Namespace)
	return nil
}

func (s *Scheduler) syncPodGroup(podGroup *schedulingv1alpha1.PodGroup) error {
	key := types.NamespacedName{
		Namespace: podGroup.Namespace,
		Name:      podGroup.Name,
	}

	if err := s.client.Get(context.TODO(), key, &schedulingv1alpha1.PodGroup{}); err != nil {
		if !errors.IsNotFound(err) {
			return err
		}

		if err := s.client.Create(context.TODO(), podGroup); err != nil {
			return err
		}
		logger.Info("Created PodGroup", "Name", podGroup.Name, "Namespace", podGroup.Namespace)
		return nil
	}

	if err := s.client.Update(context.TODO(), podGroup); err != nil {
		return err
	}
	logger.Info("Updated PodGroup", "Name", podGroup.Name, "Namespace", podGroup.Namespace)
	return nil
}
