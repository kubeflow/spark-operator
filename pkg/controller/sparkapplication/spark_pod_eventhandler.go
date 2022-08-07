/*
Copyright 2018 Google LLC

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
	"strings"

	"github.com/golang/glog"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	crdclientset "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/clientset/versioned"
	crdlisters "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/listers/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
)

// sparkPodEventHandler monitors Spark executor pods and update the SparkApplication objects accordingly.
type sparkPodEventHandler struct {
	applicationLister crdlisters.SparkApplicationLister
	crdClient         crdclientset.Interface
	// call-back function to enqueue SparkApp key for processing.
	enqueueFunc func(appKey interface{})
}

// newSparkPodEventHandler creates a new sparkPodEventHandler instance.
func newSparkPodEventHandler(enqueueFunc func(appKey interface{}), crdClient crdclientset.Interface, lister crdlisters.SparkApplicationLister) *sparkPodEventHandler {
	monitor := &sparkPodEventHandler{
		enqueueFunc:       enqueueFunc,
		crdClient:         crdClient,
		applicationLister: lister,
	}
	return monitor
}

func (s *sparkPodEventHandler) onPodAdded(obj interface{}) {
	pod := obj.(*apiv1.Pod)
	glog.V(2).Infof("Pod %s added in namespace %s.", pod.GetName(), pod.GetNamespace())

	if pod.ObjectMeta.Labels["sparkoperator.k8s.io/launched-by-spark-operator"] == "true" {
		s.enqueueSparkAppForUpdate(pod)
	} else {
		if pod.ObjectMeta.Labels["spark-role"] == "driver" {
			sparkApp, err := createSparkApplication(pod, s.crdClient)
			if err != nil {
				glog.V(2).Infof("failed to create sparkApplication")
				return
			}
			s.enqueue(sparkApp)
		}
	}
}

func (s *sparkPodEventHandler) onPodUpdated(old, updated interface{}) {
	oldPod := old.(*apiv1.Pod)
	updatedPod := updated.(*apiv1.Pod)

	if oldPod.ObjectMeta.Labels["sparkoperator.k8s.io/launched-by-spark-operator"] == "true" {
		if updatedPod.ResourceVersion == oldPod.ResourceVersion {
			return
		}
		glog.V(2).Infof("Pod %s updated in namespace %s.", updatedPod.GetName(), updatedPod.GetNamespace())
		s.enqueueSparkAppForUpdate(updatedPod)
	} else {
		if oldPod.ObjectMeta.Labels["spark-role"] == "driver" && updatedPod.ObjectMeta.Labels["spark-role"] == "driver" {
			if updatedPod.ResourceVersion == oldPod.ResourceVersion {
				return
			}
			glog.V(2).Infof("Pod %s updated in namespace %s.", updatedPod.GetName(), updatedPod.GetNamespace())
			s.enqueue(updatedPod)
		}
	}
}

func (s *sparkPodEventHandler) onPodDeleted(obj interface{}) {
	var deletedPod *apiv1.Pod

	switch obj.(type) {
	case *apiv1.Pod:
		deletedPod = obj.(*apiv1.Pod)
	case cache.DeletedFinalStateUnknown:
		deletedObj := obj.(cache.DeletedFinalStateUnknown).Obj
		deletedPod = deletedObj.(*apiv1.Pod)
	}

	if deletedPod == nil {
		return
	}
	glog.V(2).Infof("Pod %s deleted in namespace %s.", deletedPod.GetName(), deletedPod.GetNamespace())

	if deletedPod.ObjectMeta.Labels["sparkoperator.k8s.io/launched-by-spark-operator"] == "true" {
		s.enqueueSparkAppForUpdate(deletedPod)
	} else {
		s.enqueue(deletedPod)
	}
}

func (s *sparkPodEventHandler) enqueueSparkAppForUpdate(pod *apiv1.Pod) {
	appName, exists := getAppName(pod)
	if !exists {
		return
	}

	if submissionID, exists := pod.Labels[config.SubmissionIDLabel]; exists {
		app, err := s.applicationLister.SparkApplications(pod.GetNamespace()).Get(appName)
		if err != nil || app.Status.SubmissionID != submissionID {
			return
		}
	}

	appKey := createMetaNamespaceKey(pod.GetNamespace(), appName)
	glog.V(2).Infof("Enqueuing SparkApplication %s for app update processing.", appKey)
	s.enqueueFunc(appKey)
}

func (s *sparkPodEventHandler) enqueue(obj interface{}) {
	key, err := keyFunc(obj)
	if err != nil {
		glog.Errorf("failed to get key for %v: %v", obj, err)
		return
	}
	s.enqueueFunc(key)
}

func createSparkApplication(pod *apiv1.Pod, crdClient crdclientset.Interface) (*v1beta2.SparkApplication, error) {
	coreLimit := pod.Spec.Containers[0].Resources.Limits.Cpu().String()
	core, _ := pod.Spec.Containers[0].Resources.Requests.Cpu().AsInt64()
	core32 := int32(core)
	memory := pod.Spec.Containers[0].Resources.Requests.Memory().String()

	var class string
	var mainFile string
	for key, value := range pod.Spec.Containers[0].Args {
		if strings.Contains(value, "class") {
			class = pod.Spec.Containers[0].Args[key+1]
			mainFile = pod.Spec.Containers[0].Args[key+2]
		}
	}

	glog.V(2).Infof("createSparkApplication coreLimit: %s, core: %d, memory: %s, class: %s, mainFile: %s", coreLimit, core32, memory, class, mainFile)

	sparkApp := &v1beta2.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
		},
		Spec: v1beta2.SparkApplicationSpec{
			Type:                v1beta2.ScalaApplicationType,
			Mode:                v1beta2.ClusterMode,
			Image:               &pod.Spec.Containers[0].Image,
			ImagePullPolicy:     (*string)(&pod.Spec.Containers[0].ImagePullPolicy),
			MainClass:           &class,
			MainApplicationFile: &mainFile,
			SparkVersion:        "3.1.3",
			Driver: v1beta2.DriverSpec{
				PodName: &pod.Name,
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores:          &core32,
					CoreLimit:      &coreLimit,
					Memory:         &memory,
					ServiceAccount: &pod.Spec.ServiceAccountName,
				},
			},
			Executor: v1beta2.ExecutorSpec{
				SparkPodSpec: v1beta2.SparkPodSpec{
					Cores:     &core32,
					CoreLimit: &coreLimit,
					Memory:    &memory,
				},
			},
		},
	}

	_, err := crdClient.SparkoperatorV1beta2().SparkApplications(sparkApp.Namespace).Create(context.TODO(), sparkApp, metav1.CreateOptions{})
	if err != nil {
		return sparkApp, err
	}

	return sparkApp, nil
}
