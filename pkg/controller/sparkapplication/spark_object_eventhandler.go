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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	crdlisters "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/listers/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
)

// sparkObjectEventHandler monitors Spark application resources and update the SparkApplication objects accordingly.
type sparkObjectEventHandler struct {
	applicationLister crdlisters.SparkApplicationLister
	// call-back function to enqueue SparkApp key for processing.
	enqueueFunc func(appKey interface{})
}

// newSparkObjectEventHandler creates a new sparkObjectEventHandler instance.
func newSparkObjectEventHandler(enqueueFunc func(appKey interface{}), lister crdlisters.SparkApplicationLister) *sparkObjectEventHandler {
	monitor := &sparkObjectEventHandler{
		enqueueFunc:       enqueueFunc,
		applicationLister: lister,
	}
	return monitor
}

func (s *sparkObjectEventHandler) onObjectAdded(obj interface{}) {
	object := obj.(metav1.Object)
	glog.V(2).Infof("Object %s added in namespace %s.", object.GetName(), object.GetNamespace())
	s.enqueueSparkAppForUpdate(object)
}

func (s *sparkObjectEventHandler) onObjectUpdated(old, updated interface{}) {
	oldObj := old.(metav1.Object)
	updatedObj := updated.(metav1.Object)

	if updatedObj.GetResourceVersion() == oldObj.GetResourceVersion() {
		return
	}
	glog.V(2).Infof("Object %s updated in namespace %s.", updatedObj.GetName(), updatedObj.GetNamespace())
	s.enqueueSparkAppForUpdate(updatedObj)

}

func (s *sparkObjectEventHandler) onObjectDeleted(obj interface{}) {
	var deletedObject metav1.Object

	switch obj.(type) {
	case metav1.Object:
		deletedObject = obj.(metav1.Object)
	case cache.DeletedFinalStateUnknown:
		deletedObj := obj.(cache.DeletedFinalStateUnknown).Obj
		deletedObject = deletedObj.(metav1.Object)
	}

	if deletedObject == nil {
		return
	}
	glog.V(2).Infof("Object %s deleted in namespace %s.", deletedObject.GetName(), deletedObject.GetNamespace())
	s.enqueueSparkAppForUpdate(deletedObject)
}

func (s *sparkObjectEventHandler) enqueueSparkAppForUpdate(object metav1.Object) {
	appName, exists := getAppName(object)
	if !exists {
		return
	}

	if submissionID, exists := object.GetLabels()[config.SubmissionIDLabel]; exists {
		app, err := s.applicationLister.SparkApplications(object.GetNamespace()).Get(appName)
		if err != nil || app.Status.SubmissionID != submissionID {
			return
		}
	}

	appKey := createMetaNamespaceKey(object.GetNamespace(), appName)
	glog.V(2).Infof("Enqueuing SparkApplication %s for app update processing.", appKey)
	s.enqueueFunc(appKey)
}
