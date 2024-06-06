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
	"github.com/kubeflow/spark-operator/pkg/webhook/resourceusage"
)

const (
	Path          = "/webhook"
	CAKeyPem      = "ca-key.pem"
	CACertPem     = "ca-cert.pem"
	ServerKeyPem  = "server-key.pem"
	ServerCertPem = "server-cert.pem"
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
	sparkJobNamespace              string
	enableResourceQuotaEnforcement bool
	resourceQuotaEnforcer          resourceusage.ResourceQuotaEnforcer
	coreV1InformerFactory          informers.SharedInformerFactory
}

// Configuration parsed from command-line flags
type webhookFlags struct {
	webhookName             string
	webhookPort             int
	webhookSecretName       string
	webhookSecretNamespace  string
	webhookServiceName      string
	webhookServiceNamespace string
}

var userConfig webhookFlags

func init() {
	flag.StringVar(&userConfig.webhookName, "webhook-name", "spark-operator-webhook", "The name of the webhook server.")
	flag.IntVar(&userConfig.webhookPort, "webhook-port", 8080, "Service port of the webhook server.")
	flag.StringVar(&userConfig.webhookSecretName, "webhook-secret-name", "spark-operator-tls", "The name of the secret that contains the webhook server's TLS certificate and key.")
	flag.StringVar(&userConfig.webhookSecretNamespace, "webhook-secret-namespace", "spark-operator", "The namespace of the secret that contains the webhook server's TLS certificate and key.")
	flag.StringVar(&userConfig.webhookServiceName, "webhook-svc-name", "spark-webhook", "The name of the Service for the webhook server.")
	flag.StringVar(&userConfig.webhookServiceNamespace, "webhook-svc-namespace", "spark-operator", "The namespace of the Service for the webhook server.")
}

// New creates a new WebHook instance.
func New(
	clientset kubernetes.Interface,
	informerFactory crinformers.SharedInformerFactory,
	jobNamespace string,
	deregisterOnExit bool,
	enableResourceQuotaEnforcement bool,
	coreV1InformerFactory informers.SharedInformerFactory,
	webhookTimeout *int,
) (*WebHook, error) {
	certProvider, err := NewCertProvider(
		userConfig.webhookServiceName,
		userConfig.webhookServiceNamespace,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create certificate provider: %v", err)
	}

	hook := &WebHook{
		clientset:                      clientset,
		informerFactory:                informerFactory,
		lister:                         informerFactory.Sparkoperator().V1beta2().SparkApplications().Lister(),
		certProvider:                   certProvider,
		sparkJobNamespace:              jobNamespace,
		coreV1InformerFactory:          coreV1InformerFactory,
		enableResourceQuotaEnforcement: enableResourceQuotaEnforcement,
	}

	if enableResourceQuotaEnforcement {
		hook.resourceQuotaEnforcer = resourceusage.NewResourceQuotaEnforcer(informerFactory, coreV1InformerFactory)
	}

	mux := http.NewServeMux()
	mux.HandleFunc(Path, hook.serve)
	hook.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", userConfig.webhookPort),
		Handler: mux,
	}

	return hook, nil
}

// Start starts the webhook server.
func (wh *WebHook) Start(stopCh <-chan struct{}) error {
	if err := wh.updateSecret(userConfig.webhookSecretName, userConfig.webhookSecretNamespace); err != nil {
		return fmt.Errorf("failed to update secret: %v", err)
	}

	if err := wh.updateMutatingWebhookConfiguration(userConfig.webhookName); err != nil {
		return fmt.Errorf("failed to update mutating webhook configuration: %v", err)
	}

	if wh.enableResourceQuotaEnforcement {
		if err := wh.updateValidatingWebhookConfiguration(userConfig.webhookName); err != nil {
			return fmt.Errorf("failed to update validating webhook configuration: %v", err)
		}
		if err := wh.resourceQuotaEnforcer.WaitForCacheSync(stopCh); err != nil {
			return err
		}
	}

	tlsCfg, err := wh.certProvider.TLSConfig()
	if err != nil {
		return fmt.Errorf("failed to get TLS config: %v", err)
	}
	wh.server.TLSConfig = tlsCfg
	go func() {
		glog.Info("Starting the webhook server")
		if err := wh.server.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
			glog.Errorf("error while serving the Spark admission webhook: %v\n", err)
		}
	}()

	return nil
}

// Stop stops the webhook server.
func (wh *WebHook) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	glog.Info("Stopping the webhook server")
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

func (wh *WebHook) updateSecret(name, namespace string) error {
	old, err := wh.clientset.CoreV1().Secrets(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get webhook secret: %v", err)
	}

	caKey, err := wh.certProvider.CAKey()
	if err != nil {
		return fmt.Errorf("failed to get CA key: %v", err)
	}

	caCert, err := wh.certProvider.CACert()
	if err != nil {
		return fmt.Errorf("failed to get CA cert: %v", err)
	}

	serverKey, err := wh.certProvider.ServerKey()
	if err != nil {
		return fmt.Errorf("failed to get server key: %v", err)
	}

	serverCert, err := wh.certProvider.ServerCert()
	if err != nil {
		return fmt.Errorf("failed to get server cert: %v", err)
	}

	new := old.DeepCopy()
	new.Data[CAKeyPem] = caKey
	new.Data[CACertPem] = caCert
	new.Data[ServerKeyPem] = serverKey
	new.Data[ServerCertPem] = serverCert

	_, err = wh.clientset.CoreV1().Secrets(namespace).Update(context.TODO(), new, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("failed to update webhook secret: %v", err)
	}

	glog.Infof("Updated webhook secret %s/%s", namespace, name)
	return nil
}

func (wh *WebHook) updateMutatingWebhookConfiguration(name string) error {
	caBundle, err := wh.certProvider.CACert()
	if err != nil {
		return fmt.Errorf("failed to get CA certificate: %v", err)
	}

	old, err := wh.clientset.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get mutating webhook configuration: %v", err)
	}

	new := old.DeepCopy()
	for i := range new.Webhooks {
		new.Webhooks[i].ClientConfig.CABundle = caBundle
	}

	_, err = wh.clientset.AdmissionregistrationV1().MutatingWebhookConfigurations().Update(context.TODO(), new, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	glog.Infof("Updated mutating webhook configuration %s", name)
	return nil
}

func (wh *WebHook) updateValidatingWebhookConfiguration(name string) error {
	caBundle, err := wh.certProvider.CACert()
	if err != nil {
		return fmt.Errorf("failed to get CA certificate: %v", err)
	}

	old, err := wh.clientset.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get validating webhook configuration: %v", err)
	}

	new := old.DeepCopy()
	for i := range new.Webhooks {
		new.Webhooks[i].ClientConfig.CABundle = caBundle
	}

	_, err = wh.clientset.AdmissionregistrationV1().ValidatingWebhookConfigurations().Update(context.TODO(), new, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	glog.Infof("Updated validating webhook configuration %s", name)
	return nil
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
	sparkJobNs string,
) (*admissionv1.AdmissionResponse, error) {
	raw := review.Request.Object.Raw
	pod := &corev1.Pod{}
	if err := json.Unmarshal(raw, pod); err != nil {
		return nil, fmt.Errorf("failed to unmarshal a Pod from the raw data in the admission request: %v", err)
	}

	response := &admissionv1.AdmissionResponse{Allowed: true}

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
