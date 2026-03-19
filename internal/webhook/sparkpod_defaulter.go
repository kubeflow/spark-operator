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

package webhook

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kubeflow/spark-operator/v2/api/v1beta2"
	"github.com/kubeflow/spark-operator/v2/pkg/common"
	pkgwebhook "github.com/kubeflow/spark-operator/v2/pkg/webhook"
)

// +kubebuilder:webhook:admissionReviewVersions=v1,failurePolicy=fail,groups="",matchPolicy=Exact,mutating=true,name=mutate-pod.sparkoperator.k8s.io,path=/mutate--v1-pod,reinvocationPolicy=Never,resources=pods,sideEffects=None,verbs=create;update,versions=v1,webhookVersions=v1

// SparkPodDefaulter defaults Spark pods.
type SparkPodDefaulter struct {
	client             client.Client
	sparkJobNamespaces map[string]bool
}

// SparkPodDefaulter implements admission.CustomDefaulter.
var _ admission.CustomDefaulter = &SparkPodDefaulter{}

// NewSparkPodDefaulter creates a new SparkPodDefaulter instance.
func NewSparkPodDefaulter(client client.Client, namespaces []string) *SparkPodDefaulter {
	nsMap := make(map[string]bool)
	if len(namespaces) == 0 {
		nsMap[metav1.NamespaceAll] = true
	} else {
		for _, ns := range namespaces {
			nsMap[ns] = true
		}
	}

	return &SparkPodDefaulter{
		client:             client,
		sparkJobNamespaces: nsMap,
	}
}

// Default implements admission.CustomDefaulter.
func (d *SparkPodDefaulter) Default(ctx context.Context, obj runtime.Object) error {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}

	logger := log.FromContext(ctx)
	namespace := pod.Namespace
	if !d.isSparkJobNamespace(namespace) {
		return nil
	}

	appName := pod.Labels[common.LabelSparkAppName]
	if appName == "" {
		return nil
	}

	app := &v1beta2.SparkApplication{}
	if err := d.client.Get(ctx, types.NamespacedName{Name: appName, Namespace: namespace}, app); err != nil {
		return fmt.Errorf("failed to get SparkApplication %s/%s: %v", namespace, appName, err)
	}

	logger.Info("Mutating Pod", "phase", pod.Status.Phase)
	if err := mutateSparkPod(pod, app); err != nil {
		return fmt.Errorf("failed to mutate Spark pod: %v", err)
	}

	return nil
}

func (d *SparkPodDefaulter) isSparkJobNamespace(ns string) bool {
	return d.sparkJobNamespaces[metav1.NamespaceAll] || d.sparkJobNamespaces[ns]
}

func mutateSparkPod(pod *corev1.Pod, app *v1beta2.SparkApplication) error {
	options := []pkgwebhook.MutateSparkPodOption{
		pkgwebhook.AddOwnerReference,
		pkgwebhook.AddEnvVars,
		pkgwebhook.AddEnvFrom,
		pkgwebhook.AddHadoopConfigMap,
		pkgwebhook.AddSparkConfigMap,
		pkgwebhook.AddGeneralConfigMaps,
		pkgwebhook.AddVolumes,
		pkgwebhook.AddContainerPorts,
		pkgwebhook.AddHostNetwork,
		pkgwebhook.AddHostAliases,
		pkgwebhook.AddInitContainers,
		pkgwebhook.AddSidecarContainers,
		pkgwebhook.AddDNSConfig,
		pkgwebhook.AddPriorityClassName,
		pkgwebhook.AddSchedulerName,
		pkgwebhook.AddNodeSelectors,
		pkgwebhook.AddAffinity,
		pkgwebhook.AddTolerations,
		pkgwebhook.AddMemoryLimit,
		pkgwebhook.AddGPU,
		pkgwebhook.AddPrometheusConfig,
		pkgwebhook.AddContainerSecurityContext,
		pkgwebhook.AddPodSecurityContext,
		pkgwebhook.AddTerminationGracePeriodSeconds,
		pkgwebhook.AddPodLifeCycleConfig,
		pkgwebhook.AddShareProcessNamespace,
	}

	return pkgwebhook.MutateSparkPod(pod, app, options...)
}
