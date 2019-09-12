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

package webhook

import (
	"context"
	"encoding/json"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta1"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/util"
	"github.com/golang/glog"
	apiv1 "k8s.io/api/core/v1"
	corev1 "k8s.io/api/core/v1"
	"net/http"
	"reflect"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	logf "sigs.k8s.io/controller-runtime/pkg/runtime/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// AddToManagerFuncs is a list of functions to add all Controllers to the Manager
var AddToManagerFuncs []func(manager.Manager) error

var hooklog = logf.Log.WithName("webhook-log")

// +kubebuilder:webhook:path=/mutate-v1-pod,mutating=true,failurePolicy=fail,groups="",resources=pods,verbs=create;update,versions=v1,name=webhook.sparkoperator.k8s.io

// AddToManager adds all Controllers to the Manager
// +kubebuilder:rbac:groups=admissionregistration.k8s.io,resources=mutatingwebhookconfigurations;validatingwebhookconfigurations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
func AddToManager(m manager.Manager) error {
	for _, f := range AddToManagerFuncs {
		if err := f(m); err != nil {
			return err
		}
	}
	return nil
}

// podAnnotator annotates Pods
type SparkPodMutator struct {
	client       client.Client
	decoder      *admission.Decoder
	JobNameSpace string
}

// podAnnotator adds an annotation to every incoming pods.
func (a *SparkPodMutator) Handle(ctx context.Context, req admission.Request) admission.Response {
	pod := &corev1.Pod{}

	err := a.decoder.Decode(req, pod)
	if err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	return a.mutatePods(pod, req)
}

// podAnnotator implements inject.Client.
// A client will be automatically injected.

// InjectClient injects the client.
func (a *SparkPodMutator) InjectClient(c client.Client) error {
	a.client = c
	return nil
}

// podAnnotator implements admission.DecoderInjector.
// A decoder will be automatically injected.

// InjectDecoder injects the decoder.
func (a *SparkPodMutator) InjectDecoder(d *admission.Decoder) error {
	a.decoder = d
	return nil
}

func (a *SparkPodMutator) mutatePods(pod *corev1.Pod, req admission.Request) admission.Response {
	if !isSparkPod(pod) || !inSparkJobNamespace(req.AdmissionRequest.Namespace, a.JobNameSpace) {
		hooklog.Info("Pod is not subject to mutation...")
		glog.V(2).Infof("Pod %s in namespace %s is not subject to mutation", pod.GetObjectMeta().GetName(), req.AdmissionRequest.Namespace)
		return admission.Allowed("Pod not subject to mutation")
	}

	// Try getting the SparkApplication name from the annotation for that.
	appName := pod.Labels[config.SparkAppNameLabel]
	if appName == "" {
		return admission.Allowed("No Spark Operator app label")
	}

	ctx := context.Background()
	var sparkList v1beta1.SparkApplicationList

	if err := a.client.List(ctx, &sparkList, client.InNamespace(req.AdmissionRequest.Namespace), client.MatchingField(".metadata.name", appName)); err != nil {
		glog.Errorf("failed to get SparkApplication %s/%s: %v", req.AdmissionRequest.Namespace, appName, err)
		return admission.Errored(1, err)
	}

	var app = sparkList.Items[0]
	originalPod := pod.DeepCopy()
	patchSparkPod(pod, &app)

	if !reflect.DeepEqual(pod, originalPod) {
		glog.V(2).Infof("Pod %s in namespace %s is subject to mutation", pod.GetObjectMeta().GetName(), req.AdmissionRequest.Namespace)
		patchBytes, err := json.Marshal(pod)
		if err != nil {
			glog.Errorf("failed to marshal patched pod %v: %v", pod, err)
			return admission.Errored(2, err)
		}
		return admission.PatchResponseFromRaw(req.Object.Raw, patchBytes)
	}
	return admission.Allowed("No patching needed.")
}

func inSparkJobNamespace(podNs string, sparkJobNamespace string) bool {
	if sparkJobNamespace == apiv1.NamespaceAll {
		return true
	}
	return podNs == sparkJobNamespace
}

func isSparkPod(pod *corev1.Pod) bool {
	return util.IsLaunchedBySparkOperator(pod) && (util.IsDriverPod(pod) || util.IsExecutorPod(pod))
}
