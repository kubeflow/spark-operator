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
	"flag"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/golang/glog"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"

	crdapi "github.com/kubeflow/spark-operator/pkg/apis/sparkoperator.k8s.io"
	crdv1beta2 "github.com/kubeflow/spark-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	crinformers "github.com/kubeflow/spark-operator/pkg/client/informers/externalversions"
	crdlisters "github.com/kubeflow/spark-operator/pkg/client/listers/sparkoperator.k8s.io/v1beta2"
	"github.com/kubeflow/spark-operator/pkg/config"
	"github.com/kubeflow/spark-operator/pkg/util"
	"github.com/kubeflow/spark-operator/pkg/webhook/resourceusage"
)

var podResource = metav1.GroupVersionResource{
	Group:    corev1.SchemeGroupVersion.Group,
	Version:  corev1.SchemeGroupVersion.Version,
	Resource: "pods",
}

var sparkApplicationResource = metav1.GroupVersionResource{
	Group:    crdapi.GroupName,
	Version:  crdv1beta2.Version,
	Resource: "sparkapplications",
}

var scheduledSparkApplicationResource = metav1.GroupVersionResource{
	Group:    crdapi.GroupName,
	Version:  crdv1beta2.Version,
	Resource: "scheduledsparkapplications",
}

// WebHook encapsulates things needed to run the webhook.
type WebHook struct {
	clientset                      kubernetes.Interface
	informerFactory                crinformers.SharedInformerFactory
	lister                         crdlisters.SparkApplicationLister
	server                         *http.Server
	certProvider                   *certProvider
	selector                       *metav1.LabelSelector
	sparkJobNamespace              string
	deregisterOnExit               bool
	enableResourceQuotaEnforcement bool
	resourceQuotaEnforcer          resourceusage.ResourceQuotaEnforcer
	coreV1InformerFactory          informers.SharedInformerFactory
	timeoutSeconds                 *int32
}

// Configuration parsed from command-line flags
type webhookFlags struct {
	serverCert         string
	serverCertKey      string
	caCert             string
	certReloadInterval time.Duration
	webhookPort        int
}

var userConfig webhookFlags

func init() {
	flag.StringVar(&userConfig.serverCert, "webhook-server-cert", "/etc/webhook-certs/server-cert.pem", "Path to the X.509-formatted webhook certificate.")
	flag.StringVar(&userConfig.serverCertKey, "webhook-server-cert-key", "/etc/webhook-certs/server-key.pem", "Path to the webhook certificate key.")
	flag.StringVar(&userConfig.caCert, "webhook-ca-cert", "/etc/webhook-certs/ca-cert.pem", "Path to the X.509-formatted webhook CA certificate.")
	flag.DurationVar(&userConfig.certReloadInterval, "webhook-cert-reload-interval", 15*time.Minute, "Time between webhook cert reloads.")
	flag.IntVar(&userConfig.webhookPort, "webhook-port", 8080, "Service port of the webhook server.")
}

// New creates a new WebHook instance.
func New(
	clientset kubernetes.Interface,
	informerFactory crinformers.SharedInformerFactory,
	enableResourceQuotaEnforcement bool,
	coreV1InformerFactory informers.SharedInformerFactory,
) (*WebHook, error) {

	cert, err := NewCertProvider(
		userConfig.serverCert,
		userConfig.serverCertKey,
		userConfig.caCert,
		userConfig.certReloadInterval,
	)
	if err != nil {
		return nil, err
	}

	path := "/webhook"
	hook := &WebHook{
		clientset:                      clientset,
		informerFactory:                informerFactory,
		lister:                         informerFactory.Sparkoperator().V1beta2().SparkApplications().Lister(),
		certProvider:                   cert,
		coreV1InformerFactory:          coreV1InformerFactory,
		enableResourceQuotaEnforcement: enableResourceQuotaEnforcement,
	}

	if enableResourceQuotaEnforcement {
		hook.resourceQuotaEnforcer = resourceusage.NewResourceQuotaEnforcer(informerFactory, coreV1InformerFactory)
	}

	mux := http.NewServeMux()
	mux.HandleFunc(path, hook.serve)
	hook.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", userConfig.webhookPort),
		Handler: mux,
	}

	return hook, nil
}

// Start starts the admission webhook server and registers itself to the API server.
func (wh *WebHook) Start(stopCh <-chan struct{}) error {
	wh.certProvider.Start()
	wh.server.TLSConfig = wh.certProvider.tlsConfig()

	if wh.enableResourceQuotaEnforcement {
		err := wh.resourceQuotaEnforcer.WaitForCacheSync(stopCh)
		if err != nil {
			return err
		}
	}

	go func() {
		glog.Info("Starting the Spark admission webhook server")
		if err := wh.server.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
			glog.Errorf("error while serving the Spark admission webhook: %v\n", err)
		}
	}()

	return nil
}

// Stop deregisters itself with the API server and stops the admission webhook server.
func (wh *WebHook) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	glog.Info("Stopping the Spark pod admission webhook server")
	return wh.server.Shutdown(ctx)
}

func (wh *WebHook) serve(w http.ResponseWriter, r *http.Request) {
	glog.V(2).Info("Serving admission request")
	var body []byte
	if r.Body != nil {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			internalError(w, fmt.Errorf("failed to read the request body"))
			return
		}
		body = data
	}

	if len(body) == 0 {
		denyRequest(w, "empty request body", http.StatusBadRequest)
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType != "application/json" {
		denyRequest(w, "invalid Content-Type, expected `application/json`", http.StatusUnsupportedMediaType)
		return
	}

	review := &admissionv1.AdmissionReview{}
	deserializer := codecs.UniversalDeserializer()
	if _, _, err := deserializer.Decode(body, nil, review); err != nil {
		internalError(w, err)
		return
	}
	var whErr error
	var reviewResponse *admissionv1.AdmissionResponse
	switch review.Request.Resource {
	case podResource:
		reviewResponse, whErr = mutatePods(review, wh.lister, wh.sparkJobNamespace)
	case sparkApplicationResource:
		if !wh.enableResourceQuotaEnforcement {
			unexpectedResourceType(w, review.Request.Resource.String())
			return
		}
		reviewResponse, whErr = admitSparkApplications(review, wh.resourceQuotaEnforcer)
	case scheduledSparkApplicationResource:
		if !wh.enableResourceQuotaEnforcement {
			unexpectedResourceType(w, review.Request.Resource.String())
			return
		}
		reviewResponse, whErr = admitScheduledSparkApplications(review, wh.resourceQuotaEnforcer)
	default:
		unexpectedResourceType(w, review.Request.Resource.String())
		return
	}
	if whErr != nil {
		internalError(w, whErr)
		return
	}

	response := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"},
		Response: reviewResponse,
	}

	if reviewResponse != nil {
		if review.Request != nil {
			response.Response.UID = review.Request.UID
		}
	}

	resp, err := json.Marshal(response)
	if err != nil {
		internalError(w, err)
		return
	}
	if _, err := w.Write(resp); err != nil {
		internalError(w, err)
	}
}

func unexpectedResourceType(w http.ResponseWriter, kind string) {
	denyRequest(w, fmt.Sprintf("unexpected resource type: %v", kind), http.StatusUnsupportedMediaType)
}

func internalError(w http.ResponseWriter, err error) {
	glog.Errorf("internal error: %v", err)
	denyRequest(w, err.Error(), 500)
}

func denyRequest(w http.ResponseWriter, reason string, code int) {
	response := &admissionv1.AdmissionReview{
		Response: &admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Code:    int32(code),
				Message: reason,
			},
		},
	}
	resp, err := json.Marshal(response)
	if err != nil {
		glog.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(code)
	_, err = w.Write(resp)
	if err != nil {
		glog.Errorf("failed to write response body: %v", err)
	}
}

func admitSparkApplications(review *admissionv1.AdmissionReview, enforcer resourceusage.ResourceQuotaEnforcer) (*admissionv1.AdmissionResponse, error) {
	if review.Request.Resource != sparkApplicationResource {
		return nil, fmt.Errorf("expected resource to be %s, got %s", sparkApplicationResource, review.Request.Resource)
	}

	raw := review.Request.Object.Raw
	app := &crdv1beta2.SparkApplication{}
	if err := json.Unmarshal(raw, app); err != nil {
		return nil, fmt.Errorf("failed to unmarshal a SparkApplication from the raw data in the admission request: %v", err)
	}

	reason, err := enforcer.AdmitSparkApplication(*app)
	if err != nil {
		return nil, fmt.Errorf("resource quota enforcement failed for SparkApplication: %v", err)
	}
	response := &admissionv1.AdmissionResponse{Allowed: reason == ""}
	if reason != "" {
		response.Result = &metav1.Status{
			Message: reason,
			Code:    400,
		}
	}
	return response, nil
}

func admitScheduledSparkApplications(review *admissionv1.AdmissionReview, enforcer resourceusage.ResourceQuotaEnforcer) (*admissionv1.AdmissionResponse, error) {
	if review.Request.Resource != scheduledSparkApplicationResource {
		return nil, fmt.Errorf("expected resource to be %s, got %s", scheduledSparkApplicationResource, review.Request.Resource)
	}

	raw := review.Request.Object.Raw
	app := &crdv1beta2.ScheduledSparkApplication{}
	if err := json.Unmarshal(raw, app); err != nil {
		return nil, fmt.Errorf("failed to unmarshal a ScheduledSparkApplication from the raw data in the admission request: %v", err)
	}

	response := &admissionv1.AdmissionResponse{Allowed: true}
	reason, err := enforcer.AdmitScheduledSparkApplication(*app)
	if err != nil {
		return nil, fmt.Errorf("resource quota enforcement failed for ScheduledSparkApplication: %v", err)
	} else if reason != "" {
		response.Allowed = false
		response.Result = &metav1.Status{
			Message: reason,
			Code:    400,
		}
	}
	return response, nil
}

func mutatePods(
	review *admissionv1.AdmissionReview,
	lister crdlisters.SparkApplicationLister,
	sparkJobNs string) (*admissionv1.AdmissionResponse, error) {
	raw := review.Request.Object.Raw
	pod := &corev1.Pod{}
	if err := json.Unmarshal(raw, pod); err != nil {
		return nil, fmt.Errorf("failed to unmarshal a Pod from the raw data in the admission request: %v", err)
	}

	response := &admissionv1.AdmissionResponse{Allowed: true}

	if !isSparkPod(pod) {
		glog.V(2).Infof("Pod %s in namespace %s is not subject to mutation", pod.GetObjectMeta().GetName(), review.Request.Namespace)
		return response, nil
	}

	// Try getting the SparkApplication name from the annotation for that.
	appName := pod.Labels[config.SparkAppNameLabel]
	if appName == "" {
		return response, nil
	}
	app, err := lister.SparkApplications(review.Request.Namespace).Get(appName)
	if err != nil {
		return nil, fmt.Errorf("failed to get SparkApplication %s/%s: %v", review.Request.Namespace, appName, err)
	}

	patchOps := patchSparkPod(pod, app)
	if len(patchOps) > 0 {
		glog.V(2).Infof("Pod %s in namespace %s is subject to mutation", pod.GetObjectMeta().GetName(), review.Request.Namespace)
		patchBytes, err := json.Marshal(patchOps)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal patch operations %v: %v", patchOps, err)
		}
		glog.V(3).Infof("Pod %s mutation/patch result %s", pod.GetObjectMeta().GetName(), patchBytes)
		response.Patch = patchBytes
		patchType := admissionv1.PatchTypeJSONPatch
		response.PatchType = &patchType
	}

	return response, nil
}

func isSparkPod(pod *corev1.Pod) bool {
	return util.IsLaunchedBySparkOperator(pod) && (util.IsDriverPod(pod) || util.IsExecutorPod(pod))
}
