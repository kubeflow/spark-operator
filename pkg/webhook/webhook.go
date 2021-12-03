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

	admissionv1 "k8s.io/api/admission/v1"
	arv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"

	crdapi "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io"
	crdv1beta2 "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/apis/sparkoperator.k8s.io/v1beta2"
	crinformers "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/informers/externalversions"
	crdlisters "github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/client/listers/sparkoperator.k8s.io/v1beta2"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/config"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/util"
	"github.com/GoogleCloudPlatform/spark-on-k8s-operator/pkg/webhook/resourceusage"
)

const (
	webhookName      = "webhook.sparkoperator.k8s.io"
	quotaWebhookName = "quotaenforcer.sparkoperator.k8s.io"
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
	serviceRef                     *arv1.ServiceReference
	failurePolicy                  arv1.FailurePolicyType
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
	flag.StringVar(&userConfig.webhookNamespaceSelector, "webhook-namespace-selector", "", "The webhook will only operate on namespaces with this label, specified in the form key1=value1,key2=value2. Required if webhook-fail-on-error is true.")
}

// New creates a new WebHook instance.
func New(
	clientset kubernetes.Interface,
	informerFactory crinformers.SharedInformerFactory,
	jobNamespace string,
	deregisterOnExit bool,
	enableResourceQuotaEnforcement bool,
	coreV1InformerFactory informers.SharedInformerFactory,
	webhookTimeout *int) (*WebHook, error) {

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
	serviceRef := &arv1.ServiceReference{
		Namespace: userConfig.webhookServiceNamespace,
		Name:      userConfig.webhookServiceName,
		Path:      &path,
	}
	hook := &WebHook{
		clientset:                      clientset,
		informerFactory:                informerFactory,
		lister:                         informerFactory.Sparkoperator().V1beta2().SparkApplications().Lister(),
		certProvider:                   cert,
		serviceRef:                     serviceRef,
		sparkJobNamespace:              jobNamespace,
		deregisterOnExit:               deregisterOnExit,
		failurePolicy:                  arv1.Ignore,
		coreV1InformerFactory:          coreV1InformerFactory,
		enableResourceQuotaEnforcement: enableResourceQuotaEnforcement,
		timeoutSeconds:                 func(b int32) *int32 { return &b }(int32(*webhookTimeout)),
	}

	if userConfig.webhookFailOnError {
		hook.failurePolicy = arv1.Fail
	}

	if userConfig.webhookNamespaceSelector == "" {
		if userConfig.webhookFailOnError {
			return nil, fmt.Errorf("webhook-namespace-selector must be set when webhook-fail-on-error is true")
		}
	} else {
		selector, err := parseNamespaceSelector(userConfig.webhookNamespaceSelector)
		if err != nil {
			return nil, err
		}
		hook.selector = selector
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

func parseNamespaceSelector(selectorArg string) (*metav1.LabelSelector, error) {
	selector := &metav1.LabelSelector{
		MatchLabels: make(map[string]string),
	}

	selectorStrs := strings.Split(selectorArg, ",")
	for _, selectorStr := range selectorStrs {
		kv := strings.SplitN(selectorStr, "=", 2)
		if len(kv) != 2 || kv[0] == "" || kv[1] == "" {
			return nil, fmt.Errorf("webhook namespace selector must be in the form key1=value1,key2=value2")
		}
		selector.MatchLabels[kv[0]] = kv[1]
	}

	return selector, nil
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

	return wh.selfRegistration(userConfig.webhookConfigName)
}

// Stop deregisters itself with the API server and stops the admission webhook server.
func (wh *WebHook) Stop() error {
	// Do not deregister if strict error handling is enabled; pod deletions are common, and we
	// don't want to create windows where pods can be created without being subject to the webhook.
	if wh.failurePolicy != arv1.Fail {
		if err := wh.selfDeregistration(userConfig.webhookConfigName); err != nil {
			return err
		}
		glog.Infof("Webhook %s deregistered", userConfig.webhookConfigName)
	}

	wh.certProvider.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
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

func (wh *WebHook) selfRegistration(webhookConfigName string) error {
	mwcClient := wh.clientset.AdmissionregistrationV1().MutatingWebhookConfigurations()
	vwcClient := wh.clientset.AdmissionregistrationV1().ValidatingWebhookConfigurations()

	caCert, err := readCertFile(wh.certProvider.caCertFile)
	if err != nil {
		return err
	}

	mutatingRules := []arv1.RuleWithOperations{
		{
			Operations: []arv1.OperationType{arv1.Create},
			Rule: arv1.Rule{
				APIGroups:   []string{""},
				APIVersions: []string{"v1"},
				Resources:   []string{"pods"},
			},
		},
	}

	validatingRules := []arv1.RuleWithOperations{
		{
			Operations: []arv1.OperationType{arv1.Create, arv1.Update},
			Rule: arv1.Rule{
				APIGroups:   []string{crdapi.GroupName},
				APIVersions: []string{crdv1beta2.Version},
				Resources:   []string{sparkApplicationResource.Resource, scheduledSparkApplicationResource.Resource},
			},
		},
	}

	sideEffect := arv1.SideEffectClassNoneOnDryRun

	mutatingWebhook := arv1.MutatingWebhook{
		Name:  webhookName,
		Rules: mutatingRules,
		ClientConfig: arv1.WebhookClientConfig{
			Service:  wh.serviceRef,
			CABundle: caCert,
		},
		FailurePolicy:           &wh.failurePolicy,
		NamespaceSelector:       wh.selector,
		TimeoutSeconds:          wh.timeoutSeconds,
		SideEffects:             &sideEffect,
		AdmissionReviewVersions: []string{"v1"},
	}

	validatingWebhook := arv1.ValidatingWebhook{
		Name:  quotaWebhookName,
		Rules: validatingRules,
		ClientConfig: arv1.WebhookClientConfig{
			Service:  wh.serviceRef,
			CABundle: caCert,
		},
		FailurePolicy:           &wh.failurePolicy,
		NamespaceSelector:       wh.selector,
		TimeoutSeconds:          wh.timeoutSeconds,
		SideEffects:             &sideEffect,
		AdmissionReviewVersions: []string{"v1"},
	}

	mutatingWebhooks := []arv1.MutatingWebhook{mutatingWebhook}
	validatingWebhooks := []arv1.ValidatingWebhook{validatingWebhook}

	mutatingExisting, mutatingGetErr := mwcClient.Get(context.TODO(), webhookConfigName, metav1.GetOptions{})
	if mutatingGetErr != nil {
		if !errors.IsNotFound(mutatingGetErr) {
			return mutatingGetErr
		}
		// Create case.
		glog.Info("Creating a MutatingWebhookConfiguration for the Spark pod admission webhook")
		webhookConfig := &arv1.MutatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{
				Name: webhookConfigName,
			},
			Webhooks: mutatingWebhooks,
		}
		if _, err := mwcClient.Create(context.TODO(), webhookConfig, metav1.CreateOptions{}); err != nil {
			return err
		}
	} else {
		// Update case.
		glog.Info("Updating existing MutatingWebhookConfiguration for the Spark pod admission webhook")
		if !equality.Semantic.DeepEqual(mutatingWebhooks, mutatingExisting.Webhooks) {
			mutatingExisting.Webhooks = mutatingWebhooks
			if _, err := mwcClient.Update(context.TODO(), mutatingExisting, metav1.UpdateOptions{}); err != nil {
				return err
			}
		}
	}

	if wh.enableResourceQuotaEnforcement {
		validatingExisting, validatingGetErr := vwcClient.Get(context.TODO(), webhookConfigName, metav1.GetOptions{})
		if validatingGetErr != nil {
			if !errors.IsNotFound(validatingGetErr) {
				return validatingGetErr
			}
			// Create case.
			glog.Info("Creating a ValidatingWebhookConfiguration for the SparkApplication resource quota enforcement webhook")
			webhookConfig := &arv1.ValidatingWebhookConfiguration{
				ObjectMeta: metav1.ObjectMeta{
					Name: webhookConfigName,
				},
				Webhooks: validatingWebhooks,
			}
			if _, err := vwcClient.Create(context.TODO(), webhookConfig, metav1.CreateOptions{}); err != nil {
				return err
			}

		} else {
			// Update case.
			glog.Info("Updating existing ValidatingWebhookConfiguration for the SparkApplication resource quota enforcement webhook")
			if !equality.Semantic.DeepEqual(validatingWebhooks, validatingExisting.Webhooks) {
				validatingExisting.Webhooks = validatingWebhooks
				if _, err := vwcClient.Update(context.TODO(), validatingExisting, metav1.UpdateOptions{}); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (wh *WebHook) selfDeregistration(webhookConfigName string) error {
	mutatingConfigs := wh.clientset.AdmissionregistrationV1().MutatingWebhookConfigurations()
	validatingConfigs := wh.clientset.AdmissionregistrationV1().ValidatingWebhookConfigurations()
	if wh.enableResourceQuotaEnforcement {
		err := validatingConfigs.Delete(context.TODO(), webhookConfigName, metav1.DeleteOptions{GracePeriodSeconds: int64ptr(0)})
		if err != nil {
			return err
		}
	}
	return mutatingConfigs.Delete(context.TODO(), webhookConfigName, metav1.DeleteOptions{GracePeriodSeconds: int64ptr(0)})
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
		glog.V(3).Infof("Pod %s mutation/patch result %s", pod.GetObjectMeta().GetName(), patchBytes)
		response.Patch = patchBytes
		patchType := admissionv1.PatchTypeJSONPatch
		response.PatchType = &patchType
	}

	return response, nil
}

func inSparkJobNamespace(podNs string, sparkJobNamespace string) bool {
	if sparkJobNamespace == corev1.NamespaceAll {
		return true
	}
	return podNs == sparkJobNamespace
}

func isSparkPod(pod *corev1.Pod) bool {
	return util.IsLaunchedBySparkOperator(pod) && (util.IsDriverPod(pod) || util.IsExecutorPod(pod))
}

func int64ptr(n int64) *int64 {
	return &n
}
