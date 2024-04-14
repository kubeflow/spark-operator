package scheduledsparkapplication

import (
	"github.com/kubeflow/spark-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
)

type sparkApps []*v1beta2.SparkApplication

func (s sparkApps) Len() int {
	return len(s)
}

func (s sparkApps) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sparkApps) Less(i, j int) bool {
	// Sort by decreasing order of application names and correspondingly creation time.
	return s[i].Name > s[j].Name
}
