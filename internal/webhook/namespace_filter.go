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
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/kubeflow/spark-operator/v2/pkg/util"
)

// namespaceFilteringDefaulter wraps a CustomDefaulter and skips it when the
// object's namespace does not match the operator's namespace scope. Objects that
// do not implement metav1.Object (e.g. cluster-scoped) pass through unchanged.
type namespaceFilteringDefaulter struct {
	inner   admission.CustomDefaulter
	matcher *util.NamespaceMatcher
	client  client.Client
}

// NewNamespaceFilteringDefaulter wraps inner with a namespace gate driven by matcher.
func NewNamespaceFilteringDefaulter(inner admission.CustomDefaulter, matcher *util.NamespaceMatcher, c client.Client) admission.CustomDefaulter {
	return &namespaceFilteringDefaulter{inner: inner, matcher: matcher, client: c}
}

func (w *namespaceFilteringDefaulter) Default(ctx context.Context, obj runtime.Object) error {
	matched, err := matchesNamespace(ctx, w.client, w.matcher, obj)
	if err != nil {
		return err
	}
	if !matched {
		return nil
	}
	return w.inner.Default(ctx, obj)
}

// namespaceFilteringValidator wraps a CustomValidator with the same semantics
// as namespaceFilteringDefaulter. For updates the new object's namespace is
// used (namespace is immutable in Kubernetes).
type namespaceFilteringValidator struct {
	inner   admission.CustomValidator
	matcher *util.NamespaceMatcher
	client  client.Client
}

// NewNamespaceFilteringValidator wraps inner with a namespace gate driven by matcher.
func NewNamespaceFilteringValidator(inner admission.CustomValidator, matcher *util.NamespaceMatcher, c client.Client) admission.CustomValidator {
	return &namespaceFilteringValidator{inner: inner, matcher: matcher, client: c}
}

func (w *namespaceFilteringValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	matched, err := matchesNamespace(ctx, w.client, w.matcher, obj)
	if err != nil {
		return nil, err
	}
	if !matched {
		return nil, nil
	}
	return w.inner.ValidateCreate(ctx, obj)
}

func (w *namespaceFilteringValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	matched, err := matchesNamespace(ctx, w.client, w.matcher, newObj)
	if err != nil {
		return nil, err
	}
	if !matched {
		return nil, nil
	}
	return w.inner.ValidateUpdate(ctx, oldObj, newObj)
}

func (w *namespaceFilteringValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	matched, err := matchesNamespace(ctx, w.client, w.matcher, obj)
	if err != nil {
		return nil, err
	}
	if !matched {
		return nil, nil
	}
	return w.inner.ValidateDelete(ctx, obj)
}

func matchesNamespace(ctx context.Context, c client.Client, matcher *util.NamespaceMatcher, obj runtime.Object) (bool, error) {
	meta, ok := obj.(metav1.Object)
	if !ok {
		return true, nil
	}
	return matcher.MatchesWithClient(ctx, c, meta.GetNamespace())
}
