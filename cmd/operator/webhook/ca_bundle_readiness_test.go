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
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/kubeflow/spark-operator/v2/pkg/scheme"
)

const (
	testMutatingName   = "spark-operator-webhook"
	testValidatingName = "spark-operator-webhook"
)

type fakeCABundleSource struct {
	bundle []byte
	err    error
}

func (f fakeCABundleSource) CACert() ([]byte, error) {
	return f.bundle, f.err
}

func mutatingConfig(name string, caBundles ...[]byte) *admissionregistrationv1.MutatingWebhookConfiguration {
	config := &admissionregistrationv1.MutatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: name}}
	for i, caBundle := range caBundles {
		config.Webhooks = append(config.Webhooks, admissionregistrationv1.MutatingWebhook{
			Name:         "webhook-" + string(rune('a'+i)) + ".spark-operator.io",
			ClientConfig: admissionregistrationv1.WebhookClientConfig{CABundle: caBundle},
		})
	}
	return config
}

func validatingConfig(name string, caBundles ...[]byte) *admissionregistrationv1.ValidatingWebhookConfiguration {
	config := &admissionregistrationv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: name}}
	for i, caBundle := range caBundles {
		config.Webhooks = append(config.Webhooks, admissionregistrationv1.ValidatingWebhook{
			Name:         "webhook-" + string(rune('a'+i)) + ".spark-operator.io",
			ClientConfig: admissionregistrationv1.WebhookClientConfig{CABundle: caBundle},
		})
	}
	return config
}

func TestCABundleReadinessChecker(t *testing.T) {
	desired := []byte("desired-ca-bundle")
	stale := []byte("stale-ca-bundle")

	tests := []struct {
		name    string
		source  fakeCABundleSource
		objects []client.Object
		wantErr string
	}{
		{
			name:    "bootstrap: not ready before a bundle is committed",
			source:  fakeCABundleSource{err: errors.New("CA bundle is not available")},
			objects: []client.Object{mutatingConfig(testMutatingName, desired), validatingConfig(testValidatingName, desired)},
			wantErr: "not yet available",
		},
		{
			name:    "ready once both configurations carry the committed bundle on every entry",
			source:  fakeCABundleSource{bundle: desired},
			objects: []client.Object{mutatingConfig(testMutatingName, desired, desired), validatingConfig(testValidatingName, desired, desired)},
		},
		{
			name:    "not ready when the mutating configuration is missing",
			source:  fakeCABundleSource{bundle: desired},
			objects: []client.Object{validatingConfig(testValidatingName, desired)},
			wantErr: "get mutating webhook configuration",
		},
		{
			name:    "not ready when the validating configuration is missing",
			source:  fakeCABundleSource{bundle: desired},
			objects: []client.Object{mutatingConfig(testMutatingName, desired)},
			wantErr: "get validating webhook configuration",
		},
		{
			name:    "not ready when a mutating entry still carries a stale bundle",
			source:  fakeCABundleSource{bundle: desired},
			objects: []client.Object{mutatingConfig(testMutatingName, desired, stale), validatingConfig(testValidatingName, desired)},
			wantErr: "mutating webhook configuration",
		},
		{
			name:    "not ready when a validating entry still carries a stale bundle",
			source:  fakeCABundleSource{bundle: desired},
			objects: []client.Object{mutatingConfig(testMutatingName, desired), validatingConfig(testValidatingName, stale)},
			wantErr: "validating webhook configuration",
		},
		{
			name:    "not ready when a configuration has no webhooks",
			source:  fakeCABundleSource{bundle: desired},
			objects: []client.Object{mutatingConfig(testMutatingName), validatingConfig(testValidatingName, desired)},
			wantErr: "has no webhooks",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := fake.NewClientBuilder().WithScheme(scheme.WebhookScheme).WithObjects(tt.objects...).Build()
			checker := caBundleReadinessChecker(reader, tt.source, testMutatingName, testValidatingName)

			err := checker(httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestAllCheckersShortCircuits(t *testing.T) {
	sentinel := errors.New("boom")
	var calls []string

	pass := func(name string) func(*http.Request) error {
		return func(*http.Request) error {
			calls = append(calls, name)
			return nil
		}
	}
	fail := func(name string) func(*http.Request) error {
		return func(*http.Request) error {
			calls = append(calls, name)
			return sentinel
		}
	}

	err := allCheckers(pass("started"), fail("caBundle"), pass("never"))(httptest.NewRequest(http.MethodGet, "/readyz", nil))
	require.ErrorIs(t, err, sentinel)
	assert.Equal(t, []string{"started", "caBundle"}, calls)

	calls = nil
	require.NoError(t, allCheckers(pass("started"), pass("caBundle"))(httptest.NewRequest(http.MethodGet, "/readyz", nil)))
	assert.Equal(t, []string{"started", "caBundle"}, calls)
}
