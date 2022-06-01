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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	"volcano.sh/volcano/pkg/apis/scheduling/v1beta1"
	volcanoclient "volcano.sh/volcano/pkg/client/clientset/versioned"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	schedulerinterface "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/batchscheduler/interface"
)

const (
	PodGroupName = "podgroups.scheduling.volcano.sh"
)

type VolcanoBatchScheduler struct {
	extensionClient apiextensionsclient.Interface
	volcanoClient   volcanoclient.Interface
}

func GetPluginName() string {
	return "volcano"
}

func (v *VolcanoBatchScheduler) Name() string {
	return GetPluginName()
}

func (v *VolcanoBatchScheduler) ShouldSchedule(app *v1beta2.SparkApplication) bool {
	//NOTE: There is no additional requirement for volcano scheduler
	return true
}

func (v *VolcanoBatchScheduler) DoBatchSchedulingOnSubmission(app *v1beta2.SparkApplication) error {
	if app.Spec.Executor.Annotations == nil {
		app.Spec.Executor.Annotations = make(map[string]string)
	}

	if app.Spec.Driver.Annotations == nil {
		app.Spec.Driver.Annotations = make(map[string]string)
	}

	if app.Spec.Mode == v1beta2.ClientMode {
		return v.syncPodGroupInClientMode(app)
	} else if app.Spec.Mode == v1beta2.ClusterMode {
		return v.syncPodGroupInClusterMode(app)
	}
	return nil
}

func (v *VolcanoBatchScheduler) syncPodGroupInClientMode(app *v1beta2.SparkApplication) error {
	// We only care about the executor pods in client mode
	if _, ok := app.Spec.Executor.Annotations[v1beta1.KubeGroupNameAnnotationKey]; !ok {
		totalResource := getExecutorRequestResource(app)

		if app.Spec.BatchSchedulerOptions != nil && len(app.Spec.BatchSchedulerOptions.Resources) > 0 {
			totalResource = app.Spec.BatchSchedulerOptions.Resources
		}
		if err := v.syncPodGroup(app, 1, totalResource); err == nil {
			app.Spec.Executor.Annotations[v1beta1.KubeGroupNameAnnotationKey] = v.getAppPodGroupName(app)
		} else {
			return err
		}
	}
	return nil
}

func (v *VolcanoBatchScheduler) syncPodGroupInClusterMode(app *v1beta2.SparkApplication) error {
	//We need both mark Driver and Executor when submitting
	//NOTE: In cluster mode, the initial size of PodGroup is set to 1 in order to schedule driver pod first.
	if _, ok := app.Spec.Driver.Annotations[v1beta1.KubeGroupNameAnnotationKey]; !ok {
		//Both driver and executor resource will be considered.
		totalResource := sumResourceList([]corev1.ResourceList{getExecutorRequestResource(app), getDriverRequestResource(app)})

		if app.Spec.BatchSchedulerOptions != nil && len(app.Spec.BatchSchedulerOptions.Resources) > 0 {
			totalResource = app.Spec.BatchSchedulerOptions.Resources
		}
		if err := v.syncPodGroup(app, 1, totalResource); err == nil {
			app.Spec.Executor.Annotations[v1beta1.KubeGroupNameAnnotationKey] = v.getAppPodGroupName(app)
			app.Spec.Driver.Annotations[v1beta1.KubeGroupNameAnnotationKey] = v.getAppPodGroupName(app)
		} else {
			return err
		}
	}
	return nil
}

func (v *VolcanoBatchScheduler) getAppPodGroupName(app *v1beta2.SparkApplication) string {
	return fmt.Sprintf("spark-%s-pg", app.Name)
}

func (v *VolcanoBatchScheduler) syncPodGroup(app *v1beta2.SparkApplication, size int32, minResource corev1.ResourceList) error {
	var err error
	podGroupName := v.getAppPodGroupName(app)
	if pg, err := v.volcanoClient.SchedulingV1beta1().PodGroups(app.Namespace).Get(context.TODO(), podGroupName, metav1.GetOptions{}); err != nil {
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
		_, err = v.volcanoClient.SchedulingV1beta1().PodGroups(app.Namespace).Create(context.TODO(), &podGroup, metav1.CreateOptions{})
	} else {
		if pg.Spec.MinMember != size {
			pg.Spec.MinMember = size
			_, err = v.volcanoClient.SchedulingV1beta1().PodGroups(app.Namespace).Update(context.TODO(), pg, metav1.UpdateOptions{})
		}
	}
	if err != nil {
		return fmt.Errorf("failed to sync PodGroup with error: %s. Abandon schedule pods via volcano", err)
	}
	return nil
}

func (v *VolcanoBatchScheduler) CleanupOnCompletion(app *v1beta2.SparkApplication) error {
	podGroupName := v.getAppPodGroupName(app)
	err := v.volcanoClient.SchedulingV1beta1().PodGroups(app.Namespace).Delete(context.TODO(), podGroupName, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func New(config *rest.Config) (schedulerinterface.BatchScheduler, error) {
	vkClient, err := volcanoclient.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize volcano client with error %v", err)
	}
	extClient, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize k8s extension client with error %v", err)
	}

	if _, err := extClient.ApiextensionsV1().CustomResourceDefinitions().Get(
		context.TODO(),
		PodGroupName,
		metav1.GetOptions{},
	); err != nil {
		//For backward compatibility check v1beta1 API version of CustomResourceDefinitions
		if _, err := extClient.ApiextensionsV1beta1().CustomResourceDefinitions().Get(
			context.TODO(),
			PodGroupName,
			metav1.GetOptions{},
		); err != nil {
			return nil, fmt.Errorf("podGroup CRD is required to exists in current cluster error: %s", err)
		}
	}
	return &VolcanoBatchScheduler{
		extensionClient: extClient,
		volcanoClient:   vkClient,
	}, nil
}

func getExecutorRequestResource(app *v1beta2.SparkApplication) corev1.ResourceList {
	minResource := corev1.ResourceList{}

	//CoreRequest correspond to executor's core request
	if app.Spec.Executor.CoreRequest != nil {
		if value, err := resource.ParseQuantity(*app.Spec.Executor.CoreRequest); err == nil {
			minResource[corev1.ResourceCPU] = value
		}
	}

	//Use Core attribute if CoreRequest is empty
	if app.Spec.Executor.Cores != nil {
		if _, ok := minResource[corev1.ResourceCPU]; !ok {
			if value, err := resource.ParseQuantity(fmt.Sprintf("%d", *app.Spec.Executor.Cores)); err == nil {
				minResource[corev1.ResourceCPU] = value
			}
		}
	}

	//CoreLimit correspond to executor's core limit, this attribute will be used only when core request is empty.
	if app.Spec.Executor.CoreLimit != nil {
		if _, ok := minResource[corev1.ResourceCPU]; !ok {
			if value, err := resource.ParseQuantity(*app.Spec.Executor.CoreLimit); err == nil {
				minResource[corev1.ResourceCPU] = value
			}
		}
	}

	//Memory + MemoryOverhead correspond to executor's memory request
	if app.Spec.Executor.Memory != nil {
		if value, err := resource.ParseQuantity(*app.Spec.Executor.Memory); err == nil {
			minResource[corev1.ResourceMemory] = value
		}
	}
	if app.Spec.Executor.MemoryOverhead != nil {
		if value, err := resource.ParseQuantity(*app.Spec.Executor.MemoryOverhead); err == nil {
			if existing, ok := minResource[corev1.ResourceMemory]; ok {
				existing.Add(value)
				minResource[corev1.ResourceMemory] = existing
			}
		}
	}

	resourceList := []corev1.ResourceList{{}}
	for i := int32(0); i < *app.Spec.Executor.Instances; i++ {
		resourceList = append(resourceList, minResource)
	}
	return sumResourceList(resourceList)
}

func getDriverRequestResource(app *v1beta2.SparkApplication) corev1.ResourceList {
	minResource := corev1.ResourceList{}

	//Cores correspond to driver's core request
	if app.Spec.Driver.Cores != nil {
		if value, err := resource.ParseQuantity(fmt.Sprintf("%d", *app.Spec.Driver.Cores)); err == nil {
			minResource[corev1.ResourceCPU] = value
		}
	}

	//CoreLimit correspond to driver's core limit, this attribute will be used only when core request is empty.
	if app.Spec.Driver.CoreLimit != nil {
		if _, ok := minResource[corev1.ResourceCPU]; !ok {
			if value, err := resource.ParseQuantity(*app.Spec.Driver.CoreLimit); err == nil {
				minResource[corev1.ResourceCPU] = value
			}
		}
	}

	//Memory + MemoryOverhead correspond to driver's memory request
	if app.Spec.Driver.Memory != nil {
		if value, err := resource.ParseQuantity(*app.Spec.Driver.Memory); err == nil {
			minResource[corev1.ResourceMemory] = value
		}
	}
	if app.Spec.Driver.MemoryOverhead != nil {
		if value, err := resource.ParseQuantity(*app.Spec.Driver.MemoryOverhead); err == nil {
			if existing, ok := minResource[corev1.ResourceMemory]; ok {
				existing.Add(value)
				minResource[corev1.ResourceMemory] = existing
			}
		}
	}

	return minResource
}

func sumResourceList(list []corev1.ResourceList) corev1.ResourceList {
	totalResource := corev1.ResourceList{}
	for _, l := range list {
		for name, quantity := range l {

			if value, ok := totalResource[name]; !ok {
				totalResource[name] = quantity.DeepCopy()
			} else {
				value.Add(quantity)
				totalResource[name] = value
			}
		}
	}
	return totalResource
}
