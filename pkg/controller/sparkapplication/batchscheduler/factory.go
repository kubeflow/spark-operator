package batchscheduler

import (
	"sync"

	"k8s.io/client-go/rest"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/controller/sparkapplication/batchscheduler/interface"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/controller/sparkapplication/batchscheduler/volcano"
)

type schedulerInitializeFunc func(config *rest.Config) schedulerinterface.BatchScheduler

var manageMutex sync.Mutex

var schedulerContainers map[string]schedulerInitializeFunc

func init() {
	schedulerContainers = make(map[string]schedulerInitializeFunc)
	registerBatchScheduler(volcano.GetPluginName(), volcano.New)
}

func registerBatchScheduler(name string, iniFunc schedulerInitializeFunc) {
	manageMutex.Lock()
	defer manageMutex.Unlock()
	schedulerContainers[name] = iniFunc
}

func InitializeBatchScheduler(name string, config *rest.Config) schedulerinterface.BatchScheduler {
	manageMutex.Lock()
	defer manageMutex.Unlock()
	for n, fc := range schedulerContainers {
		if n == name {
			return fc(config)
		}
	}
	return nil
}
