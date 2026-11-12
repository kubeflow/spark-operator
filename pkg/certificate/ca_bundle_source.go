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

package certificate

// CABundleSource supplies the PEM-encoded CA bundle that admission webhook
// configurations must trust. It decouples the MutatingWebhookConfiguration and
// ValidatingWebhookConfiguration reconcilers from any concrete provider: the
// self-signed path is backed by *Provider, while the filesystem path is backed
// by a source that reads and validates a mounted CA file.
type CABundleSource interface {
	// CACert returns the PEM-encoded CA bundle to publish into each admission
	// webhook's clientConfig.caBundle. It returns an error when no bundle is
	// available yet, so callers must treat a returned error as "do not publish"
	// rather than "publish empty".
	CACert() ([]byte, error)
}

// Provider is the self-signed and cert-manager CABundleSource implementation.
var _ CABundleSource = (*Provider)(nil)
