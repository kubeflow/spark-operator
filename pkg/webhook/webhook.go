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
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"reflect"
	"time"

	"github.com/golang/glog"

	admissionv1beta1 "k8s.io/api/admission/v1beta1"
	"k8s.io/api/admissionregistration/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
)

const (
	webhookName       = "webhook.sparkoperator.k8s.io"
	sparkRoleLabel    = "spark-role"
	sparkDriverRole   = "driver"
	sparkExecutorRole = "executor"
	serverCertFile    = "server-cert.pem"
	serverKeyFile     = "server-key.pem"
	caCertFile        = "ca-cert.pem"
)

type WebHook struct {
	clientset  kubernetes.Interface
	server     *http.Server
	cert       *certBundle
	serviceRef *v1beta1.ServiceReference
}

func New(
	clientset kubernetes.Interface,
	certDir string,
	webhookServiceNamespace string,
	webhookServiceName string,
	webhookPort int) (*WebHook, error) {
	cert := &certBundle{
		serverCertFile: filepath.Join(certDir, serverCertFile),
		serverKeyFile:  filepath.Join(certDir, serverKeyFile),
		caCertFile:     filepath.Join(certDir, caCertFile),
	}
	path := "/webhook"
	serviceRef := &v1beta1.ServiceReference{
		Namespace: webhookServiceNamespace,
		Name:      webhookServiceName,
		Path:      &path,
	}
	hook := &WebHook{clientset: clientset, cert: cert, serviceRef: serviceRef}

	mux := http.NewServeMux()
	mux.HandleFunc(path, hook.serve)
	tlsConfig, err := configServerTLS(cert)
	if err != nil {
		return nil, err
	}
	hook.server = &http.Server{
		Addr:      fmt.Sprintf(":%d", webhookPort),
		Handler:   mux,
		TLSConfig: tlsConfig,
	}

	return hook, nil
}

// Start starts the admission webhook server and registers itself to the API server.
func (wh *WebHook) Start(webhookConfigName string) error {
	go func() {
		glog.Info("Starting the Spark pod admission webhook server")
		if err := wh.server.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
			glog.Errorf("error while serving the Spark pod admission webhook: %v\n", err)
		}
	}()

	return wh.selfRegistration(webhookConfigName)
}

// Stop deregisters itself with the API server and stops the admission webhook server.
func (wh *WebHook) Stop(webhookConfigName string) error {
	if err := wh.selfDeregistration(webhookConfigName); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	glog.Info("Stopping the Spark pod admission webhook server")
	return wh.server.Shutdown(ctx)
}

func (wh *WebHook) serve(w http.ResponseWriter, r *http.Request) {
	var body []byte
	if r.Body != nil {
		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			glog.Errorf("failed to read the request body")
			http.Error(w, "failed to read the request body", http.StatusInternalServerError)
			return
		}
		body = data
	}

	if len(body) == 0 {
		glog.Errorf("empty request body")
		http.Error(w, "empty request body", http.StatusBadRequest)
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType != "application/json" {
		glog.Errorf("Content-Type=%s, expected application/json", contentType)
		http.Error(w, "invalid Content-Type, expected `application/json`", http.StatusUnsupportedMediaType)
		return
	}

	var reviewResponse *admissionv1beta1.AdmissionResponse
	review := &admissionv1beta1.AdmissionReview{}
	deserializer := codecs.UniversalDeserializer()
	if _, _, err := deserializer.Decode(body, nil, review); err != nil {
		glog.Error(err)
		reviewResponse = toAdmissionResponse(err)
	} else {
		reviewResponse = mutatePods(review)
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
		glog.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := w.Write(resp); err != nil {
		glog.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (wh *WebHook) selfRegistration(webhookConfigName string) error {
	client := wh.clientset.AdmissionregistrationV1beta1().MutatingWebhookConfigurations()
	existing, getErr := client.Get(webhookConfigName, metav1.GetOptions{})
	if getErr != nil && !errors.IsNotFound(getErr) {
		return getErr
	}

	ignorePolicy := v1beta1.Ignore
	caCert, err := readCertFile(wh.cert.caCertFile)
	if err != nil {
		return err
	}
	webhook := v1beta1.Webhook{
		Name: webhookName,
		Rules: []v1beta1.RuleWithOperations{
			{
				Operations: []v1beta1.OperationType{v1beta1.Create, v1beta1.Update},
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
		FailurePolicy: &ignorePolicy,
	}
	webhooks := []v1beta1.Webhook{webhook}

	if getErr == nil && existing != nil {
		// Update case.
		glog.Info("Updating existing MutatingWebhookConfiguration for the Spark pod admission webhook")
		if !reflect.DeepEqual(webhooks, existing.Webhooks) {
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

func mutatePods(review *admissionv1beta1.AdmissionReview) *admissionv1beta1.AdmissionResponse {
	podResource := metav1.GroupVersionResource{
		Group:    corev1.SchemeGroupVersion.Group,
		Version:  corev1.SchemeGroupVersion.Version,
		Resource: "pods",
	}
	if review.Request.Resource != podResource {
		glog.Errorf("expected resource to be %s, got %s", podResource, review.Request.Resource)
		return nil
	}

	raw := review.Request.Object.Raw
	pod := &corev1.Pod{}
	if err := json.Unmarshal(raw, pod); err != nil {
		glog.Error(err)
		return toAdmissionResponse(err)
	}

	response := &admissionv1beta1.AdmissionResponse{Allowed: true}

	if !isSparkPod(pod) {
		return response
	}

	patchOps, err := patchSparkPod(pod)
	if err != nil {
		glog.Error(err)
		return toAdmissionResponse(err)
	}

	if len(patchOps) > 0 {
		patchType := admissionv1beta1.PatchTypeJSONPatch
		response.PatchType = &patchType
		patchBytes, err := json.Marshal(patchOps)
		if err != nil {
			glog.Error(err)
			return toAdmissionResponse(err)
		}
		response.Patch = patchBytes
	}

	return response
}

func toAdmissionResponse(err error) *admissionv1beta1.AdmissionResponse {
	return &admissionv1beta1.AdmissionResponse{
		Result: &metav1.Status{
			Message: err.Error(),
			Code:    http.StatusInternalServerError,
		},
	}
}

func isSparkPod(pod *corev1.Pod) bool {
	launchedBySparkOperator, ok := pod.Labels[config.LaunchedBySparkOperatorLabel]
	if !ok {
		return false
	}

	sparkRole, ok := pod.Labels[sparkRoleLabel]
	if !ok {
		return false
	}

	return launchedBySparkOperator == "true" && (sparkRole == sparkDriverRole || sparkRole == sparkExecutorRole)
}
