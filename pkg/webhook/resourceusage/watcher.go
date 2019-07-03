package resourceusage

import (
	"fmt"
	so "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta1"
	crdinformers "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/informers/externalversions"
	"github.com/golang/glog"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	corev1informers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/tools/cache"
	"sync"
)

type ResourceUsageWatcher struct {
	currentUsageLock                     *sync.RWMutex
	currentUsageByNamespace              map[string]*ResourceList
	usageByNamespacePod                  map[string]map[string]*ResourceList
	usageByNamespaceScheduledApplication map[string]map[string]*ResourceList
	usageByNamespaceApplication          map[string]map[string]*ResourceList
	crdInformerFactory                   crdinformers.SharedInformerFactory
	coreV1InformerFactory                informers.SharedInformerFactory
	podInformer                          corev1informers.PodInformer
}

// more convenient replacement for corev1.ResourceList
type ResourceList struct {
	cpu    resource.Quantity
	memory resource.Quantity
}

const (
	KindSparkApplication          = "SparkApplication"
	KindScheduledSparkApplication = "ScheduledSparkApplication"
)

func (r ResourceList) String() string {
	return fmt.Sprintf("cpu: %v mcore, memory %v bytes", r.cpu.MilliValue(), r.memory.Value())
}

func NewResourceUsageWatcher(crdInformerFactory crdinformers.SharedInformerFactory, coreV1InformerFactory informers.SharedInformerFactory) ResourceUsageWatcher {
	glog.V(2).Infof("Creating new resource usage watcher")
	r := ResourceUsageWatcher{
		crdInformerFactory:                   crdInformerFactory,
		currentUsageLock:                     &sync.RWMutex{},
		coreV1InformerFactory:                coreV1InformerFactory,
		currentUsageByNamespace:              make(map[string]*ResourceList),
		usageByNamespacePod:                  make(map[string]map[string]*ResourceList),
		usageByNamespaceScheduledApplication: make(map[string]map[string]*ResourceList),
		usageByNamespaceApplication:          make(map[string]map[string]*ResourceList),
	}
	// Note: Events for each handler are processed serially, so no coordination is needed between
	// the different callbacks. Coordination is still needed around updating the shared state.
	sparkApplicationInformer := r.crdInformerFactory.Sparkoperator().V1beta1().SparkApplications()
	sparkApplicationInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    r.onSparkApplicationAdded,
		UpdateFunc: r.onSparkApplicationUpdated,
		DeleteFunc: r.onSparkApplicationDeleted,
	})
	scheduledSparkApplicationInformer := r.crdInformerFactory.Sparkoperator().V1beta1().ScheduledSparkApplications()
	scheduledSparkApplicationInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    r.onScheduledSparkApplicationAdded,
		UpdateFunc: r.onScheduledSparkApplicationUpdated,
		DeleteFunc: r.onScheduledSparkApplicationDeleted,
	})
	r.podInformer = r.coreV1InformerFactory.Core().V1().Pods()
	r.podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    r.onPodAdded,
		UpdateFunc: r.onPodUpdated,
		DeleteFunc: r.onPodDeleted,
	})
	var podList []*corev1.Pod
	podList, err := r.podInformer.Lister().List(labels.Everything())
	if err != nil {
		glog.Error("could not list pods: ", err)
	}
	for _, pod := range podList {
		r.onPodAdded(pod)
	}
	var sparkAppList []*so.SparkApplication
	sparkAppList, err = sparkApplicationInformer.Lister().List(labels.Everything())
	if err != nil {
		glog.Error("could not list SparkApplications: ", err)
	}
	for _, app := range sparkAppList {
		r.onSparkApplicationAdded(app)
	}
	var scheduledSparkAppList []*so.ScheduledSparkApplication
	scheduledSparkAppList, err = scheduledSparkApplicationInformer.Lister().List(labels.Everything())
	if err != nil {
		glog.Error("could not list ScheduledSparkApplications: ", err)
	}
	for _, app := range scheduledSparkAppList {
		r.onScheduledSparkApplicationAdded(app)
	}
	return r
}

func (r *ResourceUsageWatcher) GetCurrentResourceUsage(namespace string) ResourceList {
	r.currentUsageLock.RLock()
	defer r.currentUsageLock.RUnlock()
	if resourceUsageInternal, present := r.currentUsageByNamespace[namespace]; present {
		return ResourceList{
			cpu:    resourceUsageInternal.cpu,
			memory: resourceUsageInternal.memory,
		}
	}
	return ResourceList{}
}

func (r *ResourceUsageWatcher) GetCurrentResourceUsageWithApplication(namespace string, kind string, name string) (namespaceResources, applicationResources ResourceList) {
	r.currentUsageLock.RLock()
	defer r.currentUsageLock.RUnlock()
	if resourceUsageInternal, present := r.currentUsageByNamespace[namespace]; present {
		var applicationResources ResourceList
		var namespaceMap map[string]map[string]*ResourceList
		switch kind {
		case KindSparkApplication:
			namespaceMap = r.usageByNamespaceApplication
		case KindScheduledSparkApplication:
			namespaceMap = r.usageByNamespaceScheduledApplication
		}
		if applicationMap, present := namespaceMap[namespace]; present {
			if ar, present := applicationMap[name]; present {
				applicationResources = *ar
			}
		}
		currentUsage := *resourceUsageInternal // Creates a copy
		currentUsage.cpu.Sub(applicationResources.cpu)
		currentUsage.memory.Sub(applicationResources.memory)
		return currentUsage, applicationResources
	}
	return ResourceList{}, ResourceList{}
}

func (r *ResourceUsageWatcher) unsafeSetResources(namespace string, name string, resources ResourceList, resourceMap map[string]map[string]*ResourceList) {
	if _, present := resourceMap[namespace]; !present {
		resourceMap[namespace] = make(map[string]*ResourceList)
	}
	// Clear any resource usage currently stored for this object
	r.unsafeDeleteResources(namespace, name, resourceMap)
	resourceMap[namespace][name] = &resources
	if current, present := r.currentUsageByNamespace[namespace]; present {
		current.cpu.Add(resources.cpu)
		current.memory.Add(resources.memory)
	} else {
		r.currentUsageByNamespace[namespace] = &ResourceList{
			cpu:    resources.cpu,
			memory: resources.memory,
		}
	}
}

func (r *ResourceUsageWatcher) unsafeDeleteResources(namespace string, name string, resourceMap map[string]map[string]*ResourceList) {
	if namespaceMap, present := resourceMap[namespace]; present {
		if resources, present := namespaceMap[name]; present {
			delete(resourceMap[namespace], name)
			if current, present := r.currentUsageByNamespace[namespace]; present {
				current.cpu.Sub(resources.cpu)
				current.memory.Sub(resources.memory)
			}
		}
	}
}

func (r *ResourceUsageWatcher) setResources(kind, namespace, name string, resources ResourceList, resourceMap map[string]map[string]*ResourceList) {
	glog.V(2).Infof("Updating object %s %s/%s with resources %v", kind, namespace, name, resources)
	r.currentUsageLock.Lock()
	r.unsafeSetResources(namespace, name, resources, resourceMap)
	r.currentUsageLock.Unlock()
	glog.V(2).Infof("Current resources for namespace %s: %v", namespace, r.currentUsageByNamespace[namespace])
}

func (r *ResourceUsageWatcher) deleteResources(kind, namespace, name string, resourceMap map[string]map[string]*ResourceList) {
	glog.V(2).Infof("Deleting resources from object %s/%s", namespace, name)
	r.currentUsageLock.Lock()
	r.unsafeDeleteResources(namespace, name, resourceMap)
	r.currentUsageLock.Unlock()
	glog.V(2).Infof("Current resources for namespace %s: %v", namespace, r.currentUsageByNamespace[namespace])
}
