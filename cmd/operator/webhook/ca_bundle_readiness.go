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

package webhook

import (
	"bytes"
	"context"
	"fmt"
	"net/http"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"

	"github.com/kubeflow/spark-operator/v2/pkg/certificate"
)

// caBundleReadinessChecker gates readiness on caBundle convergence: it reports
// not-ready until the operator has published its committed CA bundle onto every
// webhook entry of both named admission configurations. Because the webhook is
// fail-closed, this keeps the pod out of the endpoints until the API server can
// actually trust it. It is composed only in filesystem+operator mode; in every
// other mode readiness stays the started-only check and never reads admission
// objects.
//
// The configurations are read through the manager's cached client, which is
// field-selected to the two named objects, so this adds no extra API traffic
// beyond the informers the reconcilers already maintain. A read error (cache not
// yet synced, object missing) is reported as not-ready, i.e. it fails closed.
func caBundleReadinessChecker(reader client.Reader, source certificate.CABundleSource, mutatingName, validatingName string) healthz.Checker {
	return func(req *http.Request) error {
		desired, err := source.CACert()
		if err != nil {
			return fmt.Errorf("CA bundle not yet available: %w", err)
		}

		ctx := req.Context()
		if err := mutatingWebhookConfigurationConverged(ctx, reader, mutatingName, desired); err != nil {
			return err
		}
		return validatingWebhookConfigurationConverged(ctx, reader, validatingName, desired)
	}
}

func mutatingWebhookConfigurationConverged(ctx context.Context, reader client.Reader, name string, desired []byte) error {
	config := &admissionregistrationv1.MutatingWebhookConfiguration{}
	if err := reader.Get(ctx, types.NamespacedName{Name: name}, config); err != nil {
		return fmt.Errorf("get mutating webhook configuration %q: %w", name, err)
	}
	if len(config.Webhooks) == 0 {
		return fmt.Errorf("mutating webhook configuration %q has no webhooks", name)
	}
	for i := range config.Webhooks {
		if !bytes.Equal(config.Webhooks[i].ClientConfig.CABundle, desired) {
			return fmt.Errorf("mutating webhook configuration %q webhook %q caBundle has not converged", name, config.Webhooks[i].Name)
		}
	}
	return nil
}

func validatingWebhookConfigurationConverged(ctx context.Context, reader client.Reader, name string, desired []byte) error {
	config := &admissionregistrationv1.ValidatingWebhookConfiguration{}
	if err := reader.Get(ctx, types.NamespacedName{Name: name}, config); err != nil {
		return fmt.Errorf("get validating webhook configuration %q: %w", name, err)
	}
	if len(config.Webhooks) == 0 {
		return fmt.Errorf("validating webhook configuration %q has no webhooks", name)
	}
	for i := range config.Webhooks {
		if !bytes.Equal(config.Webhooks[i].ClientConfig.CABundle, desired) {
			return fmt.Errorf("validating webhook configuration %q webhook %q caBundle has not converged", name, config.Webhooks[i].Name)
		}
	}
	return nil
}

// allCheckers composes checkers into one that passes only when every checker
// passes, evaluated in order. It lets start.go AND the started check with the
// caBundle convergence check without importing net/http there.
func allCheckers(checkers ...healthz.Checker) healthz.Checker {
	return func(req *http.Request) error {
		for _, check := range checkers {
			if err := check(req); err != nil {
				return err
			}
		}
		return nil
	}
}
