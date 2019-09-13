package resourceusage

import (
	"fmt"
	so "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	crdinformers "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/informers/externalversions"
	"github.com/golang/glog"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	corev1informers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/tools/cache"
)

type ResourceQuotaEnforcer struct {
	watcher               ResourceUsageWatcher
	resourceQuotaInformer corev1informers.ResourceQuotaInformer
}

func NewResourceQuotaEnforcer(crdInformerFactory crdinformers.SharedInformerFactory, coreV1InformerFactory informers.SharedInformerFactory) ResourceQuotaEnforcer {
	resourceUsageWatcher := newResourceUsageWatcher(crdInformerFactory, coreV1InformerFactory)
	informer := coreV1InformerFactory.Core().V1().ResourceQuotas()
	informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{})
	return ResourceQuotaEnforcer{
		watcher:               resourceUsageWatcher,
		resourceQuotaInformer: informer,
	}
}

func (r ResourceQuotaEnforcer) WaitForCacheSync(stopCh <-chan struct{}) error {
	if !cache.WaitForCacheSync(stopCh, func() bool {
		return r.resourceQuotaInformer.Informer().HasSynced()
	}) {
		return fmt.Errorf("cache sync canceled")
	}
	return nil
}

func (r *ResourceQuotaEnforcer) admitResource(kind, namespace, name string, requestedResources ResourceList) (string, error) {
	glog.V(2).Infof("Processing admission request for %s %s/%s, requesting: %s", kind, namespace, name, requestedResources)
	resourceQuotas, err := r.resourceQuotaInformer.Lister().ResourceQuotas(namespace).List(labels.Everything())
	if err != nil {
		return "", err
	}
	if (requestedResources.cpu.IsZero() && requestedResources.memory.IsZero()) || len(resourceQuotas) == 0 {
		return "", nil
	}

	currentNamespaceUsage, currentApplicationUsage := r.watcher.GetCurrentResourceUsageWithApplication(namespace, kind, name)

	for _, quota := range resourceQuotas {
		// Scope selectors not currently supported, ignore any ResourceQuota that does not match everything.
		if quota.Spec.ScopeSelector != nil || len(quota.Spec.Scopes) > 0 {
			continue
		}

		// If an existing application has increased its usage, check it against the quota again. If its usage hasn't increased, always allow it.
		if requestedResources.cpu.Cmp(currentApplicationUsage.cpu) == 1 {
			if cpuLimit, present := quota.Spec.Hard[corev1.ResourceCPU]; present {
				availableCpu := cpuLimit
				availableCpu.Sub(currentNamespaceUsage.cpu)
				if requestedResources.cpu.Cmp(availableCpu) == 1 {
					return fmt.Sprintf("%s %s/%s requests too many cores (%.3f cores requested, %.3f available).", kind, namespace, name, float64(requestedResources.cpu.MilliValue())/1000.0, float64(availableCpu.MilliValue())/1000.0), nil
				}
			}
		}

		if requestedResources.memory.Cmp(currentApplicationUsage.memory) == 1 {
			if memoryLimit, present := quota.Spec.Hard[corev1.ResourceMemory]; present {
				availableMemory := memoryLimit
				availableMemory.Sub(currentNamespaceUsage.memory)
				if requestedResources.memory.Cmp(availableMemory) == 1 {
					return fmt.Sprintf("%s %s/%s requests too much memory (%dMi requested, %dMi available).", kind, namespace, name, requestedResources.memory.Value()/(1<<20), availableMemory.Value()/(1<<20)), nil
				}
			}
		}
	}
	return "", nil
}

func (r *ResourceQuotaEnforcer) AdmitSparkApplication(app so.SparkApplication) (string, error) {
	resourceUsage, err := sparkApplicationResourceUsage(app)
	if err != nil {
		return "", err
	}
	return r.admitResource(KindSparkApplication, app.ObjectMeta.Namespace, app.ObjectMeta.Name, resourceUsage)
}

func (r *ResourceQuotaEnforcer) AdmitScheduledSparkApplication(app so.ScheduledSparkApplication) (string, error) {
	resourceUsage, err := scheduledSparkApplicationResourceUsage(app)
	if err != nil {
		return "", err
	}
	return r.admitResource(KindScheduledSparkApplication, app.ObjectMeta.Namespace, app.ObjectMeta.Name, resourceUsage)
}
