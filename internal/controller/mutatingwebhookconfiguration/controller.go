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

package mutatingwebhookconfiguration

import (
	"context"
	"fmt"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/kubeflow/spark-operator/v2/pkg/certificate"
)

var (
	logger = ctrl.Log.WithName("")
)

// Reconciler reconciles a webhook configuration object.
type Reconciler struct {
	client       client.Client
	certProvider *certificate.Provider
	name         string
}

// MutatingWebhookConfigurationReconciler implements reconcile.Reconciler.
var _ reconcile.Reconciler = &Reconciler{}

// NewReconciler creates a new MutatingWebhookConfigurationReconciler instance.
func NewReconciler(client client.Client, certProvider *certificate.Provider, name string) *Reconciler {
	return &Reconciler{
		client:       client,
		certProvider: certProvider,
		name:         name,
	}
}

func (r *Reconciler) SetupWithManager(mgr ctrl.Manager, options controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("mutating-webhook-configuration-controller").
		Watches(
			&admissionregistrationv1.MutatingWebhookConfiguration{},
			NewEventHandler(),
			builder.WithPredicates(
				NewEventFilter(r.name),
			),
		).
		WithOptions(options).
		Complete(r)
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger.Info("Updating CA bundle of MutatingWebhookConfiguration", "name", req.Name)
	if err := r.updateMutatingWebhookConfiguration(ctx, req.NamespacedName); err != nil {
		return ctrl.Result{Requeue: true}, err
	}
	return ctrl.Result{}, nil
}

func (r *Reconciler) updateMutatingWebhookConfiguration(ctx context.Context, key types.NamespacedName) error {
	webhook := &admissionregistrationv1.MutatingWebhookConfiguration{}
	if err := r.client.Get(ctx, key, webhook); err != nil {
		return fmt.Errorf("failed to get mutating webhook configuration %v: %v", key, err)
	}

	caBundle, err := r.certProvider.CACert()
	if err != nil {
		return fmt.Errorf("failed to get CA certificate: %v", err)
	}

	newWebhook := webhook.DeepCopy()
	for i := range newWebhook.Webhooks {
		newWebhook.Webhooks[i].ClientConfig.CABundle = caBundle
	}
	if err := r.client.Update(ctx, newWebhook); err != nil {
		return fmt.Errorf("failed to update mutating webhook configuration %v: %v", key, err)
	}

	return nil
}
