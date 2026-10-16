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

package validatingwebhookconfiguration

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/kubeflow/spark-operator/v2/pkg/certificate"
	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

var (
	logger = ctrl.Log.WithName("")
)

// Reconciler reconciles a ValidatingWebhookConfiguration object.
type Reconciler struct {
	client         client.Client
	caBundleSource certificate.CABundleSource
	name           string
	// caEvents optionally signals CA bundle changes that originate outside the
	// Kubernetes API (e.g. a filesystem CA source committing a new bundle). It is
	// nil for providers whose CA changes are already observable through the
	// watched webhook configuration object.
	caEvents <-chan event.GenericEvent
}

// ValidatingWebhookConfigurationReconciler implements reconcile.Reconciler interface.
var _ reconcile.Reconciler = &Reconciler{}

// NewReconciler creates a new ValidatingWebhookConfigurationReconciler instance.
func NewReconciler(client client.Client, source certificate.CABundleSource, name string) *Reconciler {
	return &Reconciler{
		client:         client,
		caBundleSource: source,
		name:           name,
	}
}

// WithCABundleEventChannel registers an optional channel that signals CA bundle
// changes. A filesystem CA commit emits no Kubernetes object event, so this
// channel is how a newly committed bundle reaches the reconciler between
// object-triggered reconciles. It returns the receiver for fluent construction.
func (r *Reconciler) WithCABundleEventChannel(caEvents <-chan event.GenericEvent) *Reconciler {
	r.caEvents = caEvents
	return r
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, options controller.Options) error {
	kind := "ValidatingWebhookConfiguration"
	name := strings.ToLower(kind)

	// Use a custom log constructor.
	options.LogConstructor = util.NewLogConstructor(mgr.GetLogger(), kind)

	controllerBuilder := ctrl.NewControllerManagedBy(mgr).
		Named(name).
		Watches(
			&admissionregistrationv1.ValidatingWebhookConfiguration{},
			NewEventHandler(),
			builder.WithPredicates(
				NewEventFilter(r.name),
			),
		).
		WithOptions(options)

	if src := r.caBundleEventSource(); src != nil {
		controllerBuilder = controllerBuilder.WatchesRawSource(src)
	}

	return controllerBuilder.Complete(r)
}

// caBundleEventSource returns a channel-backed source that maps every CA-change
// signal to this reconciler's named ValidatingWebhookConfiguration, or nil when
// no CA event channel is registered. A CA source event carries no object, so the
// map handler ignores it and always enqueues the configured webhook, keeping the
// source decoupled from the webhook type and name.
func (r *Reconciler) caBundleEventSource() source.Source {
	if r.caEvents == nil {
		return nil
	}
	return source.Channel(
		r.caEvents,
		handler.EnqueueRequestsFromMapFunc(func(context.Context, client.Object) []reconcile.Request {
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: r.name}}}
		}),
	)
}

// Reconcile implements reconcile.Reconciler.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger.Info("Updating CA bundle of ValidatingWebhookConfiguration", "name", req.Name)
	if err := r.updateValidatingWebhookConfiguration(ctx, req.NamespacedName); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *Reconciler) updateValidatingWebhookConfiguration(ctx context.Context, key types.NamespacedName) error {
	webhook := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := r.client.Get(ctx, key, webhook); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get validating webhook configuration %v: %w", key, err)
	}

	caBundle, err := r.caBundleSource.CACert()
	if err != nil {
		return fmt.Errorf("failed to get CA certificate: %w", err)
	}

	inSync := true
	for i := range webhook.Webhooks {
		if !bytes.Equal(webhook.Webhooks[i].ClientConfig.CABundle, caBundle) {
			inSync = false
			break
		}
	}
	if inSync {
		return nil
	}

	base := webhook.DeepCopy()
	for i := range webhook.Webhooks {
		webhook.Webhooks[i].ClientConfig.CABundle = caBundle
	}
	if err := r.client.Patch(ctx, webhook, client.StrategicMergeFrom(base, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("failed to patch validating webhook configuration %v: %w", key, err)
	}

	return nil
}
