/*
Copyright 2026 The Kubeflow authors.

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

package validatingwebhookconfiguration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/kubeflow/spark-operator/v2/pkg/certificate"
	"github.com/kubeflow/spark-operator/v2/pkg/scheme"
)

// fakeCABundleSource is a CABundleSource that is not a *certificate.Provider,
// proving the reconciler depends only on the interface.
type fakeCABundleSource struct {
	caBundle []byte
	err      error
}

func (f fakeCABundleSource) CACert() ([]byte, error) {
	return f.caBundle, f.err
}

func TestUpdateValidatingWebhookConfigurationUsesCABundleSource(t *testing.T) {
	webhook := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	webhook.Name = "webhook"
	webhook.Webhooks = []admissionregistrationv1.ValidatingWebhook{
		{Name: "first.example.com", ClientConfig: admissionregistrationv1.WebhookClientConfig{CABundle: []byte("stale")}},
		{Name: "second.example.com", ClientConfig: admissionregistrationv1.WebhookClientConfig{CABundle: []byte("stale")}},
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme.WebhookScheme).WithObjects(webhook).Build()
	patchCalls := 0
	trackingClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			patchCalls++
			return c.Patch(ctx, obj, patch, opts...)
		},
	})
	wantCA := []byte("desired-bundle")
	reconciler := NewReconciler(trackingClient, fakeCABundleSource{caBundle: wantCA}, webhook.Name)

	key := client.ObjectKey{Name: webhook.Name}
	if err := reconciler.updateValidatingWebhookConfiguration(t.Context(), key); err != nil {
		t.Fatalf("repair drift: %v", err)
	}
	if err := reconciler.updateValidatingWebhookConfiguration(t.Context(), key); err != nil {
		t.Fatalf("reconcile converged object: %v", err)
	}

	if patchCalls != 1 {
		t.Fatalf("expected exactly one patch across drift and convergence, got %d", patchCalls)
	}
	got := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := baseClient.Get(t.Context(), key, got); err != nil {
		t.Fatalf("get repaired webhook: %v", err)
	}
	for i := range got.Webhooks {
		if !bytes.Equal(got.Webhooks[i].ClientConfig.CABundle, wantCA) {
			t.Fatalf("webhook %q has CA bundle %q, want %q", got.Webhooks[i].Name, got.Webhooks[i].ClientConfig.CABundle, wantCA)
		}
	}
}

func TestCABundleEventChannelEnqueuesNamedRequest(t *testing.T) {
	// No channel registered yields no source.
	if src := NewReconciler(nil, fakeCABundleSource{}, "webhook").caBundleEventSource(); src != nil {
		t.Fatal("expected no channel source when no CA event channel is registered")
	}

	caEvents := make(chan event.GenericEvent, 1)
	reconciler := NewReconciler(nil, fakeCABundleSource{}, "webhook").WithCABundleEventChannel(caEvents)
	src := reconciler.caBundleEventSource()
	if src == nil {
		t.Fatal("expected a channel source when a CA event channel is registered")
	}

	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[ctrl.Request]())
	defer queue.ShutDown()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := src.Start(ctx, queue); err != nil {
		t.Fatalf("start channel source: %v", err)
	}

	// A CA commit signal carries no object; the source must still enqueue the
	// reconciler's configured webhook name.
	caEvents <- event.GenericEvent{}

	got := make(chan ctrl.Request, 1)
	go func() {
		item, shutdown := queue.Get()
		if !shutdown {
			got <- item
		}
	}()
	select {
	case req := <-got:
		if req.Name != "webhook" {
			t.Fatalf("enqueued request %v, want name %q", req.NamespacedName, "webhook")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for CA event to enqueue a request")
	}
}

func TestUpdateValidatingWebhookConfigurationPropagatesCABundleSourceError(t *testing.T) {
	webhook := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	webhook.Name = "webhook"
	webhook.Webhooks = []admissionregistrationv1.ValidatingWebhook{{Name: "first.example.com"}}
	baseClient := fake.NewClientBuilder().WithScheme(scheme.WebhookScheme).WithObjects(webhook).Build()
	wantErr := errors.New("source unavailable")
	reconciler := NewReconciler(baseClient, fakeCABundleSource{err: wantErr}, webhook.Name)

	err := reconciler.updateValidatingWebhookConfiguration(t.Context(), client.ObjectKey{Name: webhook.Name})

	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped source error, got %v", err)
	}
}

func TestUpdateValidatingWebhookConfigurationPatchesDriftAndConverges(t *testing.T) {
	webhook := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	webhook.Name = "webhook"
	webhook.Labels = map[string]string{"preserved": "true"}
	webhook.Webhooks = []admissionregistrationv1.ValidatingWebhook{
		{Name: "first.example.com", ClientConfig: admissionregistrationv1.WebhookClientConfig{CABundle: []byte("desired")}},
		{Name: "second.example.com", ClientConfig: admissionregistrationv1.WebhookClientConfig{CABundle: []byte("stale")}},
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme.WebhookScheme).WithObjects(webhook).Build()
	patchCalls := 0
	updateCalls := 0
	trackingClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			patchCalls++
			if patch.Type() != types.StrategicMergePatchType {
				t.Fatalf("expected strategic merge patch, got %q", patch.Type())
			}
			data, err := patch.Data(obj)
			if err != nil {
				t.Fatalf("build patch data: %v", err)
			}
			var contents map[string]any
			if err := json.Unmarshal(data, &contents); err != nil {
				t.Fatalf("decode patch data: %v", err)
			}
			metadata, ok := contents["metadata"].(map[string]any)
			if !ok || metadata["resourceVersion"] == "" {
				t.Fatalf("expected optimistic-lock resourceVersion in patch: %s", data)
			}
			if _, ok := metadata["labels"]; ok {
				t.Fatalf("expected labels to be absent from patch: %s", data)
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			updateCalls++
			return c.Update(ctx, obj, opts...)
		},
	})
	provider := certificate.NewProvider(baseClient, "webhook", "default", false)
	if err := provider.Generate(); err != nil {
		t.Fatalf("generate certificate provider: %v", err)
	}
	wantCA, err := provider.CACert()
	if err != nil {
		t.Fatalf("get CA certificate: %v", err)
	}
	webhook.Webhooks[0].ClientConfig.CABundle = wantCA
	if err := baseClient.Update(t.Context(), webhook); err != nil {
		t.Fatalf("set initial CA certificate: %v", err)
	}
	reconciler := NewReconciler(trackingClient, provider, webhook.Name)

	key := client.ObjectKey{Name: webhook.Name}
	if err := reconciler.updateValidatingWebhookConfiguration(t.Context(), key); err != nil {
		t.Fatalf("repair drift: %v", err)
	}
	if err := reconciler.updateValidatingWebhookConfiguration(t.Context(), key); err != nil {
		t.Fatalf("reconcile converged object: %v", err)
	}

	if patchCalls != 1 {
		t.Fatalf("expected one patch across drift and convergence, got %d", patchCalls)
	}
	if updateCalls != 0 {
		t.Fatalf("expected no full updates, got %d", updateCalls)
	}
	got := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := baseClient.Get(t.Context(), key, got); err != nil {
		t.Fatalf("get repaired webhook: %v", err)
	}
	if got.Labels["preserved"] != "true" {
		t.Fatalf("expected unrelated labels to be preserved, got %v", got.Labels)
	}
	for i := range got.Webhooks {
		if !bytes.Equal(got.Webhooks[i].ClientConfig.CABundle, wantCA) {
			t.Fatalf("webhook %q has CA bundle %q, want generated CA", got.Webhooks[i].Name, got.Webhooks[i].ClientConfig.CABundle)
		}
	}
}

func TestUpdateValidatingWebhookConfigurationPreservesPatchError(t *testing.T) {
	wantErr := errors.New("patch validating webhook configuration")
	webhook := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	webhook.Name = "webhook"
	webhook.Webhooks = []admissionregistrationv1.ValidatingWebhook{{Name: "first.example.com"}}
	baseClient := fake.NewClientBuilder().WithScheme(scheme.WebhookScheme).WithObjects(webhook).Build()
	failingClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
			return wantErr
		},
	})
	provider := certificate.NewProvider(baseClient, "webhook", "default", false)
	if err := provider.Generate(); err != nil {
		t.Fatalf("generate certificate provider: %v", err)
	}
	reconciler := NewReconciler(failingClient, provider, webhook.Name)

	err := reconciler.updateValidatingWebhookConfiguration(t.Context(), client.ObjectKey{Name: webhook.Name})

	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped patch error, got %v", err)
	}
}

func TestReconcilePreservesPatchError(t *testing.T) {
	wantErr := errors.New("patch validating webhook configuration")
	webhook := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	webhook.Name = "webhook"
	webhook.Webhooks = []admissionregistrationv1.ValidatingWebhook{{Name: "first.example.com"}}
	baseClient := fake.NewClientBuilder().WithScheme(scheme.WebhookScheme).WithObjects(webhook).Build()
	failingClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
			return wantErr
		},
	})
	provider := certificate.NewProvider(baseClient, "webhook", "default", false)
	if err := provider.Generate(); err != nil {
		t.Fatalf("generate certificate provider: %v", err)
	}
	reconciler := NewReconciler(failingClient, provider, webhook.Name)

	result, err := reconciler.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKey{Name: webhook.Name}})

	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped patch error, got %v", err)
	}
	if result.Requeue {
		t.Fatal("expected controller-runtime to retry the returned error instead of an explicit requeue")
	}
}

func TestUpdateValidatingWebhookConfigurationPreservesGetError(t *testing.T) {
	wantErr := errors.New("get validating webhook configuration")
	baseClient := fake.NewClientBuilder().WithScheme(scheme.WebhookScheme).Build()
	failingClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return wantErr
		},
	})
	reconciler := NewReconciler(failingClient, certificate.NewProvider(baseClient, "webhook", "default", false), "webhook")

	err := reconciler.updateValidatingWebhookConfiguration(t.Context(), client.ObjectKey{Name: "webhook"})

	if !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped get error, got %v", err)
	}
}

func TestUpdateValidatingWebhookConfigurationIgnoresNotFound(t *testing.T) {
	baseClient := fake.NewClientBuilder().WithScheme(scheme.WebhookScheme).Build()
	reconciler := NewReconciler(baseClient, certificate.NewProvider(baseClient, "webhook", "default", false), "webhook")

	if err := reconciler.updateValidatingWebhookConfiguration(t.Context(), client.ObjectKey{Name: "webhook"}); err != nil {
		t.Fatalf("expected missing configuration to be terminal, got %v", err)
	}
}

func TestUpdateValidatingWebhookConfigurationPreservesCACertError(t *testing.T) {
	webhook := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	webhook.Name = "webhook"
	baseClient := fake.NewClientBuilder().WithScheme(scheme.WebhookScheme).WithObjects(webhook).Build()
	reconciler := NewReconciler(baseClient, certificate.NewProvider(baseClient, "webhook", "default", false), webhook.Name)

	err := reconciler.updateValidatingWebhookConfiguration(t.Context(), client.ObjectKey{Name: webhook.Name})

	if err == nil || err.Error() != "failed to get CA certificate: CA certificate is not set" {
		t.Fatalf("expected CA certificate error, got %v", err)
	}
}

func TestUpdateValidatingWebhookConfigurationPreservesConflictError(t *testing.T) {
	webhook := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	webhook.Name = "webhook"
	webhook.ResourceVersion = "1"
	webhook.Webhooks = []admissionregistrationv1.ValidatingWebhook{{Name: "first.example.com"}}
	baseClient := fake.NewClientBuilder().WithScheme(scheme.WebhookScheme).WithObjects(webhook).Build()
	wantErr := apierrors.NewConflict(schema.GroupResource{Group: "admissionregistration.k8s.io", Resource: "validatingwebhookconfigurations"}, webhook.Name, errors.New("concurrent update"))
	failingClient := interceptor.NewClient(baseClient, interceptor.Funcs{
		Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
			return wantErr
		},
	})
	provider := certificate.NewProvider(baseClient, "webhook", "default", false)
	if err := provider.Generate(); err != nil {
		t.Fatalf("generate certificate provider: %v", err)
	}
	reconciler := NewReconciler(failingClient, provider, webhook.Name)

	err := reconciler.updateValidatingWebhookConfiguration(t.Context(), types.NamespacedName{Name: webhook.Name})

	if !apierrors.IsConflict(err) {
		t.Fatalf("expected wrapped conflict error, got %v", err)
	}
}
