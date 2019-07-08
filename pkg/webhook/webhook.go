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
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/golang/glog"

	admissionv1beta1 "k8s.io/api/admission/v1beta1"
	"k8s.io/api/admissionregistration/v1beta1"
	arv1beta1 "k8s.io/api/admissionregistration/v1beta1"
	apiv1 "k8s.io/api/core/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	crinformers "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/informers/externalversions"
	crdlisters "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/listers/sparkoperator.k8s.io/v1beta1"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/util"
)

const (
	webhookName = "webhook.sparkoperator.k8s.io"
)

var podResource = metav1.GroupVersionResource{
	Group:    corev1.SchemeGroupVersion.Group,
	Version:  corev1.SchemeGroupVersion.Version,
	Resource: "pods",
}

// WebHook encapsulates things needed to run the webhook.
type WebHook struct {
	clientset         kubernetes.Interface
	lister            crdlisters.SparkApplicationLister
	server            *http.Server
	certProvider      *certProvider
	serviceRef        *v1beta1.ServiceReference
	failurePolicy     v1beta1.FailurePolicyType
	selector          *metav1.LabelSelector
	sparkJobNamespace string
}

// Configuration parsed from command-line flags
type webhookFlags struct {
	serverCert               string
	serverCertKey            string
	caCert                   string
	certReloadInterval       time.Duration
	webhookServiceNamespace  string
	webhookServiceName       string
	webhookPort              int
	webhookConfigName        string
	webhookFailOnError       bool
	webhookNamespaceSelector string
}

var userConfig webhookFlags

func init() {
	flag.StringVar(&userConfig.webhookConfigName, "webhook-config-name", "spark-webhook-config", "The name of the MutatingWebhookConfiguration object to create.")
	flag.StringVar(&userConfig.serverCert, "webhook-server-cert", "/etc/webhook-certs/server-cert.pem", "Path to the X.509-formatted webhook certificate.")
	flag.StringVar(&userConfig.serverCertKey, "webhook-server-cert-key", "/etc/webhook-certs/server-key.pem", "Path to the webhook certificate key.")
	flag.StringVar(&userConfig.caCert, "webhook-ca-cert", "/etc/webhook-certs/ca-cert.pem", "Path to the X.509-formatted webhook CA certificate.")
	flag.DurationVar(&userConfig.certReloadInterval, "webhook-cert-reload-interval", 15*time.Minute, "Time between webhook cert reloads.")
	flag.StringVar(&userConfig.webhookServiceNamespace, "webhook-svc-namespace", "spark-operator", "The namespace of the Service for the webhook server.")
	flag.StringVar(&userConfig.webhookServiceName, "webhook-svc-name", "spark-webhook", "The name of the Service for the webhook server.")
	flag.IntVar(&userConfig.webhookPort, "webhook-port", 8080, "Service port of the webhook server.")
	flag.BoolVar(&userConfig.webhookFailOnError, "webhook-fail-on-error", false, "Whether Kubernetes should reject requests when the webhook fails.")
	flag.StringVar(&userConfig.webhookNamespaceSelector, "webhook-namespace-selector", "", "The webhook will only operate on namespaces with this label, specified in the form key=value. Required if webhook-fail-on-error is true.")
}

// New creates a new WebHook instance.
func New(
	clientset kubernetes.Interface,
	informerFactory crinformers.SharedInformerFactory,
	jobNamespace string,
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
	serviceRef := &v1beta1.ServiceReference{
		Namespace: userConfig.webhookServiceNamespace,
		Name:      userConfig.webhookServiceName,
		Path:      &path,
	}
	hook := &WebHook{
		clientset:         clientset,
		lister:            informerFactory.Sparkoperator().V1beta1().SparkApplications().Lister(),
		certProvider:      cert,
		serviceRef:        serviceRef,
		sparkJobNamespace: jobNamespace,
		failurePolicy:     arv1beta1.Ignore,
	}
	if userConfig.webhookFailOnError {
		if userConfig.webhookNamespaceSelector == "" {
			glog.Fatal("webhook-namespace-selector must be set when webhook-fail-on-error is true.")
		} else {
			kv := strings.SplitN(userConfig.webhookNamespaceSelector, "=", 2)
			if len(kv) != 2 {
				return nil, fmt.Errorf("Webhook namespace selector must be in the form key=value")
			}
			hook.selector = &metav1.LabelSelector{
				MatchLabels: map[string]string{
					kv[0]: kv[1],
				},
			}
		}
		hook.failurePolicy = arv1beta1.Fail
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
func (wh *WebHook) Start() error {
	wh.certProvider.Start()
	wh.server.TLSConfig = wh.certProvider.tlsConfig()

	go func() {
		glog.Info("Starting the Spark pod admission webhook server")
		if err := wh.server.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
			glog.Errorf("error while serving the Spark pod admission webhook: %v\n", err)
		}
	}()

	return wh.selfRegistration(userConfig.webhookConfigName)
}

// Stop deregisters itself with the API server and stops the admission webhook server.
func (wh *WebHook) Stop() error {
	// Do not deregister if strict error handling is enabled; pod deletions are common, and we
	// don't want to create windows where pods can be created without being subject to the webhook.
	if wh.failurePolicy != arv1beta1.Fail {
		if err := wh.selfDeregistration(userConfig.webhookConfigName); err != nil {
			return err
		}
		glog.Infof("Webhook %s deregistered", userConfig.webhookConfigName)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wh.certProvider.Stop()
	glog.Info("Stopping the Spark pod admission webhook server")
	return wh.server.Shutdown(ctx)
}

func (wh *WebHook) serve(w http.ResponseWriter, r *http.Request) {
	glog.V(2).Info("Serving admission request")
	var body []byte
	if r.Body != nil {
		data, err := ioutil.ReadAll(r.Body)
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

	var reviewResponse *admissionv1beta1.AdmissionResponse
	review := &admissionv1beta1.AdmissionReview{}
	deserializer := codecs.UniversalDeserializer()
	if _, _, err := deserializer.Decode(body, nil, review); err != nil {
		internalError(w, err)
		return
	} else {
		if review.Request.Resource == podResource {
			reviewResponse, err = mutatePods(review, wh.lister, wh.sparkJobNamespace)
			if err != nil {
				internalError(w, err)
				return
			}
		} else {
			denyRequest(w, fmt.Sprintf("unexpected resource type: %v", review.Request.Resource.String()), http.StatusUnsupportedMediaType)
			return
		}
	}

	response := admissionv1beta1.AdmissionReview{}
	if reviewResponse != nil {
		response.Response = reviewResponse
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

func internalError(w http.ResponseWriter, err error) {
	glog.Errorf("internal error: %v", err)
	denyRequest(w, err.Error(), 500)
}

func denyRequest(w http.ResponseWriter, reason string, code int) {
	response := &admissionv1beta1.AdmissionReview{
		Response: &admissionv1beta1.AdmissionResponse{
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
		glog.Errorf("Failed to write response body: %v", err)
	}
}

func (wh *WebHook) selfRegistration(webhookConfigName string) error {
	client := wh.clientset.AdmissionregistrationV1beta1().MutatingWebhookConfigurations()
	existing, getErr := client.Get(webhookConfigName, metav1.GetOptions{})
	if getErr != nil && !errors.IsNotFound(getErr) {
		return getErr
	}

	caCert, err := readCertFile(wh.certProvider.caCertFile)
	if err != nil {
		return err
	}
	webhook := v1beta1.Webhook{
		Name: webhookName,
		Rules: []v1beta1.RuleWithOperations{
			{
				Operations: []v1beta1.OperationType{v1beta1.Create},
				Rule: v1beta1.Rule{
					APIGroups:   []string{""},
					APIVersions: []string{"v1"},
					Resources:   []string{"pods"},
				},
			},
		},
		ClientConfig: v1beta1.WebhookClientConfig{
			Service:  wh.serviceRef,
			CABundle: caCert,
		},
		FailurePolicy:     &wh.failurePolicy,
		NamespaceSelector: wh.selector,
	}
	webhooks := []v1beta1.Webhook{webhook}

	if getErr == nil && existing != nil {
		// Update case.
		glog.Info("Updating existing MutatingWebhookConfiguration for the Spark pod admission webhook")
		if !equality.Semantic.DeepEqual(webhooks, existing.Webhooks) {
			existing.Webhooks = webhooks
			if _, err := client.Update(existing); err != nil {
				return err
			}
		}
	} else {
		// Create case.
		glog.Info("Creating a MutatingWebhookConfiguration for the Spark pod admission webhook")
		webhookConfig := &v1beta1.MutatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{
				Name: webhookConfigName,
			},
			Webhooks: webhooks,
		}
		if _, err := client.Create(webhookConfig); err != nil {
			return err
		}
	}
	return nil
}

func (wh *WebHook) selfDeregistration(webhookConfigName string) error {
	client := wh.clientset.AdmissionregistrationV1beta1().MutatingWebhookConfigurations()
	return client.Delete(webhookConfigName, metav1.NewDeleteOptions(0))
}

func mutatePods(
	review *admissionv1beta1.AdmissionReview,
	lister crdlisters.SparkApplicationLister,
	sparkJobNs string) (*admissionv1beta1.AdmissionResponse, error) {
	if review.Request.Resource != podResource {
		return nil, fmt.Errorf("expected resource to be %s, got %s", podResource, review.Request.Resource)
	}

	raw := review.Request.Object.Raw
	pod := &corev1.Pod{}
	if err := json.Unmarshal(raw, pod); err != nil {
		return nil, fmt.Errorf("failed to unmarshal a Pod from the raw data in the admission request: %v", err)
	}

	response := &admissionv1beta1.AdmissionResponse{Allowed: true}

	if !isSparkPod(pod) || !inSparkJobNamespace(review.Request.Namespace, sparkJobNs) {
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
		response.Patch = patchBytes
		patchType := admissionv1beta1.PatchTypeJSONPatch
		response.PatchType = &patchType
	}

	return response, nil
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
