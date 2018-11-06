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
	"github.com/golang/glog"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

// sparkPodEventHandler monitors Spark executor pods and update the SparkApplication objects accordingly.
type sparkPodEventHandler struct {
	// call-back function to enqueue SparkApp key for processing.
	enqueueFunc func(appKey interface{})
}

// newSparkPodEventHandler creates a new sparkPodEventHandler instance.
func newSparkPodEventHandler(enqueueFunc func(appKey interface{})) *sparkPodEventHandler {
	monitor := &sparkPodEventHandler{
		enqueueFunc: enqueueFunc,
	}
	return monitor
}

func (s *sparkPodEventHandler) onPodAdded(obj interface{}) {
	pod := obj.(*apiv1.Pod)
	s.enqueueSparkAppForUpdate(pod)
}

func (s *sparkPodEventHandler) onPodUpdated(old, updated interface{}) {
	oldPod := old.(*apiv1.Pod)
	updatedPod := updated.(*apiv1.Pod)

	if updatedPod.ResourceVersion == oldPod.ResourceVersion {
		return
	}
	s.enqueueSparkAppForUpdate(updatedPod)

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
	s.enqueueSparkAppForUpdate(deletedPod)
}

func (c *sparkPodEventHandler) enqueueSparkAppForUpdate(pod *apiv1.Pod) {
	if appKey, ok := createMetaNamespaceKey(pod); ok {
		glog.V(2).Infof("Enqueuing SparkApplication %s for processing", appKey)
		c.enqueueFunc(appKey)
	}
}
