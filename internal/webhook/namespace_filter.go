/*
Copyright The Kubeflow Authors.

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

// namespaceFilteringDefaulter skips the inner Defaulter when the object's
// namespace is out of scope. Non-metav1.Object values pass through.
type namespaceFilteringDefaulter[T runtime.Object] struct {
	inner   admission.Defaulter[T]
	matcher *util.NamespaceMatcher
	client  client.Client
}

// NewNamespaceFilteringDefaulter wraps inner with a namespace gate driven by matcher.
func NewNamespaceFilteringDefaulter[T runtime.Object](inner admission.Defaulter[T], matcher *util.NamespaceMatcher, c client.Client) admission.Defaulter[T] {
	return &namespaceFilteringDefaulter[T]{inner: inner, matcher: matcher, client: c}
}

func (w *namespaceFilteringDefaulter[T]) Default(ctx context.Context, obj T) error {
	matched, err := matchesNamespace(ctx, w.client, w.matcher, obj)
	if err != nil {
		return err
	}
	if !matched {
		return nil
	}
	return w.inner.Default(ctx, obj)
}

// namespaceFilteringValidator is namespaceFilteringDefaulter for Validators.
// Updates use the new object's namespace (namespace is immutable).
type namespaceFilteringValidator[T runtime.Object] struct {
	inner   admission.Validator[T]
	matcher *util.NamespaceMatcher
	client  client.Client
}

// NewNamespaceFilteringValidator wraps inner with a namespace gate driven by matcher.
func NewNamespaceFilteringValidator[T runtime.Object](inner admission.Validator[T], matcher *util.NamespaceMatcher, c client.Client) admission.Validator[T] {
	return &namespaceFilteringValidator[T]{inner: inner, matcher: matcher, client: c}
}

func (w *namespaceFilteringValidator[T]) ValidateCreate(ctx context.Context, obj T) (admission.Warnings, error) {
	matched, err := matchesNamespace(ctx, w.client, w.matcher, obj)
	if err != nil {
		return nil, err
	}
	if !matched {
		return nil, nil
	}
	return w.inner.ValidateCreate(ctx, obj)
}

func (w *namespaceFilteringValidator[T]) ValidateUpdate(ctx context.Context, oldObj, newObj T) (admission.Warnings, error) {
	matched, err := matchesNamespace(ctx, w.client, w.matcher, newObj)
	if err != nil {
		return nil, err
	}
	if !matched {
		return nil, nil
	}
	return w.inner.ValidateUpdate(ctx, oldObj, newObj)
}

func (w *namespaceFilteringValidator[T]) ValidateDelete(ctx context.Context, obj T) (admission.Warnings, error) {
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
	if !ok || meta == nil {
		return true, nil
	}
	return matcher.MatchesWithClient(ctx, c, meta.GetNamespace())
}
