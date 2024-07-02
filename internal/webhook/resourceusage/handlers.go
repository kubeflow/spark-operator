package resourceusage

import (
	"github.com/golang/glog"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"

	crdv1beta2 "github.com/kubeflow/spark-operator/api/v1beta2"
)

func (r *Watcher) onPodAdded(obj interface{}) {
	pod := obj.(*corev1.Pod)
	// A pod launched by the Spark operator will already be accounted for by the CRD informer callback
	if !launchedBySparkOperator(pod.ObjectMeta) {
		r.setResources("Pod", namespaceOrDefault(pod.ObjectMeta), pod.ObjectMeta.Name, podResourceUsage(pod), r.usageByNamespacePod)
	}
}

func (r *Watcher) onPodUpdated(_, newObj interface{}) {
	newPod := newObj.(*corev1.Pod)
	if !launchedBySparkOperator(newPod.ObjectMeta) {
		if newPod.Status.Phase == corev1.PodFailed || newPod.Status.Phase == corev1.PodSucceeded {
			r.deleteResources("Pod", namespaceOrDefault(newPod.ObjectMeta), newPod.ObjectMeta.Name, r.usageByNamespacePod)
		} else {
			r.setResources("Pod", namespaceOrDefault(newPod.ObjectMeta), newPod.ObjectMeta.Name, podResourceUsage(newPod), r.usageByNamespacePod)
		}
	}
}

func (r *Watcher) onPodDeleted(obj interface{}) {
	var pod *corev1.Pod
	switch o := obj.(type) {
	case *corev1.Pod:
		pod = o
	case cache.DeletedFinalStateUnknown:
		pod = o.Obj.(*corev1.Pod)
	default:
		return
	}
	if !launchedBySparkOperator(pod.ObjectMeta) {
		r.deleteResources("Pod", namespaceOrDefault(pod.ObjectMeta), pod.ObjectMeta.Name, r.usageByNamespacePod)
	}
}

func (r *Watcher) onSparkApplicationAdded(obj interface{}) {
	app := obj.(*crdv1beta2.SparkApplication)
	namespace := namespaceOrDefault(app.ObjectMeta)
	resources, err := sparkApplicationResourceUsage(*app)
	if err != nil {
		glog.Errorf("failed to determine resource usage of SparkApplication %s/%s: %v", namespace, app.ObjectMeta.Name, err)
	} else {
		r.setResources(KindSparkApplication, namespace, app.ObjectMeta.Name, resources, r.usageByNamespaceApplication)
	}
}

func (r *Watcher) onSparkApplicationUpdated(oldObj, newObj interface{}) {
	oldApp := oldObj.(*crdv1beta2.SparkApplication)
	newApp := newObj.(*crdv1beta2.SparkApplication)
	if oldApp.ResourceVersion == newApp.ResourceVersion {
		return
	}
	namespace := namespaceOrDefault(newApp.ObjectMeta)
	newResources, err := sparkApplicationResourceUsage(*newApp)
	if err != nil {
		glog.Errorf("failed to determine resource usage of SparkApplication %s/%s: %v", namespace, newApp.ObjectMeta.Name, err)
	} else {
		r.setResources(KindSparkApplication, namespace, newApp.ObjectMeta.Name, newResources, r.usageByNamespaceApplication)
	}
}

func (r *Watcher) onSparkApplicationDeleted(obj interface{}) {
	var app *crdv1beta2.SparkApplication
	switch o := obj.(type) {
	case *crdv1beta2.SparkApplication:
		app = o
	case cache.DeletedFinalStateUnknown:
		app = o.Obj.(*crdv1beta2.SparkApplication)
	default:
		return
	}
	namespace := namespaceOrDefault(app.ObjectMeta)
	r.deleteResources(KindSparkApplication, namespace, app.ObjectMeta.Name, r.usageByNamespaceApplication)
}

func (r *Watcher) onScheduledSparkApplicationAdded(obj interface{}) {
	app := obj.(*crdv1beta2.ScheduledSparkApplication)
	namespace := namespaceOrDefault(app.ObjectMeta)
	resources, err := scheduledSparkApplicationResourceUsage(*app)
	if err != nil {
		glog.Errorf("failed to determine resource usage of ScheduledSparkApplication %s/%s: %v", namespace, app.ObjectMeta.Name, err)
	} else {
		r.setResources(KindScheduledSparkApplication, namespace, app.ObjectMeta.Name, resources, r.usageByNamespaceScheduledApplication)
	}
}

func (r *Watcher) onScheduledSparkApplicationUpdated(oldObj, _ interface{}) {
	newApp := oldObj.(*crdv1beta2.ScheduledSparkApplication)
	namespace := namespaceOrDefault(newApp.ObjectMeta)
	newResources, err := scheduledSparkApplicationResourceUsage(*newApp)
	if err != nil {
		glog.Errorf("failed to determine resource usage of ScheduledSparkApplication %s/%s: %v", namespace, newApp.ObjectMeta.Name, err)
	} else {
		r.setResources(KindSparkApplication, namespace, newApp.ObjectMeta.Name, newResources, r.usageByNamespaceScheduledApplication)
	}
}

func (r *Watcher) onScheduledSparkApplicationDeleted(obj interface{}) {
	var app *crdv1beta2.ScheduledSparkApplication
	switch o := obj.(type) {
	case *crdv1beta2.ScheduledSparkApplication:
		app = o
	case cache.DeletedFinalStateUnknown:
		app = o.Obj.(*crdv1beta2.ScheduledSparkApplication)
	default:
		return
	}
	namespace := namespaceOrDefault(app.ObjectMeta)
	r.deleteResources(KindScheduledSparkApplication, namespace, app.ObjectMeta.Name, r.usageByNamespaceScheduledApplication)
}
